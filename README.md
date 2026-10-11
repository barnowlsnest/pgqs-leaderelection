# pgqs-leaderelection

Leader election for Go services, backed by PostgreSQL. No ZooKeeper, no etcd, no Consul — if your service already talks to Postgres, it already has everything it needs to elect a leader.

[![Go Reference](https://pkg.go.dev/badge/github.com/barnowlsnest/pgqs-leaderelection.svg)](https://pkg.go.dev/github.com/barnowlsnest/pgqs-leaderelection)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## What it does

Exactly one process in a fleet runs a given job — a cron tick, a queue compactor, a cache warmer — while the others stand by and take over within one TTL if the leader dies.

```go
svc.Run(ctx, "nightly-report", pgleader.Callbacks{
    OnElected:  func(ctx context.Context) { runReports(ctx) }, // only the leader gets here
    OnResigned: func() { log.Info("stepped down") },
})
```

## How it works

Two Postgres primitives, each doing the job it is good at:

- **A `leases` row** holds the durable state: who the holder is, when the lease expires. It survives restarts, so a new process learns the current leader by reading one row.
- **A session-scoped advisory lock**, keyed by the hash of the resource name, serializes the decision. Every read-decide-write cycle on the lease row happens while that lock is held, so two candidates can never evaluate the same lease at the same time and both conclude they won.

The advisory lock is taken and released **per operation**, never held for the whole leadership term. A leader running for six hours does not pin a pooled connection for six hours.

Leadership is a lease, not a latch: it expires unless renewed. If a leader is partitioned, paused by a long GC, or killed without cleanup, its lease runs out and the next candidate takes over.

## Install

```bash
go get github.com/barnowlsnest/pgqs-leaderelection
```

Requires Go 1.27+ and PostgreSQL.

## Quickstart

### 1. Run the migration

The `leases` table ships as an embedded migration:

```go
import "github.com/barnowlsnest/pgqs-leaderelection/pkg/database"

if err := database.RollOut(ctx, dbURL); err != nil {
    return err
}
```

`database.RollDown` reverses it.

### 2. Campaign for a resource

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/barnowlsnest/pgqs-harness/v2/db"
	"github.com/barnowlsnest/pgqs-harness/v2/postgres"

	"github.com/barnowlsnest/pgqs-leaderelection/pkg/database"
	"github.com/barnowlsnest/pgqs-leaderelection/pkg/pgleader"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if err := database.RollOut(ctx, dbURL); err != nil {
		panic(err)
	}

	pool, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	leases, err := pgleader.NewLeaseRepo(db.NewBaseDAO[pgleader.LeaseRecord]("pgqs", "leases", pool))
	if err != nil {
		panic(err)
	}

	// identity must be unique per process.
	host, _ := os.Hostname()
	identity := fmt.Sprintf("%s-%d", host, os.Getpid())

	svc, err := pgleader.New(pool, leases, identity)
	if err != nil {
		panic(err)
	}

	err = svc.Run(ctx, "nightly-report", pgleader.Callbacks{
		OnElected:  runReports,
		OnResigned: func() { fmt.Println("no longer leader") },
	})
	fmt.Println("campaign ended:", err)
}

// runReports is leader-only work. Its context is canceled the moment leadership
// is lost, so it must return promptly when that happens.
func runReports(ctx context.Context) {
	<-ctx.Done()
}
```

`Run` blocks until `ctx` is done and always returns `ctx.Err()`. Transient acquire and renew failures are logged and retried, not surfaced — losing the database for a few seconds is not a reason to tear down your service.

## Configuration

```go
svc, err := pgleader.New(pool, leases, identity,
	pgleader.WithTTL(15*time.Second),           // how long a lease stays valid unrenewed
	pgleader.WithRenewInterval(5*time.Second),  // how often the leader renews
	pgleader.WithRetryInterval(5*time.Second),  // how often a follower tries to take over
)
```

| Option | Default | Notes |
| --- | --- | --- |
| `WithTTL` | 15s | Upper bound on how long a dead leader blocks failover. |
| `WithRenewInterval` | 5s | Keep it comfortably below the TTL — a couple of missed renewals should not expire the lease. |
| `WithRetryInterval` | 5s | Lower means faster failover and more polling. |
| `WithClock` | `time.Now` | Test seam. Expiry is compared against this clock, not the database clock. |

The `OnElected` context is canceled at the lease expiry if a renewal has not landed by then, so leader-only work stops on its own when the lease does.

Pick `renewInterval` so that `TTL` covers several renewals: at 15s/5s a leader survives two consecutive failed renewals before it gives up leadership.

## Manual control

`Run` is the loop you usually want, but each step is exported if you are driving the schedule yourself:

```go
lease, err := svc.TryAcquire(ctx, "nightly-report") // one attempt
lease, err = svc.Renew(ctx, "nightly-report")       // extend a lease you hold
err = svc.Release(ctx, "nightly-report")            // step down immediately
ok := svc.IsLeader("nightly-report")                // local view, no query
```

Errors are sentinels, compared with `errors.Is`:

| Error | Meaning |
| --- | --- |
| `ErrLeaseHeldByOther` | Someone else holds a valid lease. Not a failure — try again later. |
| `ErrLockAlreadyAcquired` | Another candidate is mid-decision on this resource. Also not a failure. |
| `ErrLeaseLost` | The lease you tried to renew is no longer yours. Stop leader-only work now. |
| `ErrRunInProgress` | This `Service` is already campaigning for that resource. |

## Guarantees and limits

**One lease holder at a time — guaranteed.** The advisory lock serializes the read-decide-write cycle on the lease row, so two candidates can never both conclude they won. This holds under any failure.

**One *worker* at a time — not guaranteed by any lease system, including this one.** `IsLeader` and the `OnElected` context live in your process's memory. A frozen process cannot be told it is no longer leader:

```
t=0s     A acquires. expires_at = 15s. OnElected starts writing.
t=3s     A's host freezes — GC pause, CPU steal, VM suspend, partition.
t=15s    The lease expires in the database.
t=15.1s  B sees an expired lease, takes over, starts writing.
t=17s    A thaws. It has not yet learned anything, so it is still writing.
         ← two writers
t=17.1s  A's renewal fails, its leader context is canceled, A stops.
```

The database was never wrong; the lease had exactly one valid holder throughout. What was stale was A's *belief*. This window is inherent to leases — etcd, ZooKeeper, Consul and Kubernetes `Lease` objects all have it.

This library makes the window as small as it can be:

- The leader context is canceled by a **timer armed at the lease expiry**, independent of the renewal loop. A renewal stuck on a dead connection cannot keep the term alive past the lease.
- Renewals are bounded by the lease's own remaining lifetime, so a hung call cannot stall the loop.

What it cannot do is stop code already running in a frozen process. **If your leader-only work must never overlap, fence it at the resource.** `LeaseRecord.Version` increments on every acquire and renew, so it is monotonic — carry it into the write and let the target reject stale writers:

```sql
UPDATE report_state
   SET data = $1, fence = $2      -- $2 = Version from your lease
 WHERE report_id = $3
   AND fence < $2;                -- a zombie leader's older Version updates 0 rows
```

In the timeline above, A resumes and writes with `fence = 4`; B already wrote `fence = 5`; A's UPDATE matches no rows. Two leaders existed, one write landed.

If your leader-only work is idempotent or self-correcting — a cache warmer, a scraper, a job that re-reads before acting — you do not need this.

### Other limits

- **Expiry uses the process clock**, not the database clock. Badly skewed clocks across the fleet shorten or stretch effective TTLs; run NTP.
- **Graceful shutdown is fast.** When `Run` returns it releases the lease, so a rolling deploy fails over immediately instead of waiting out the TTL. A `kill -9` waits for expiry.
- `Release` on a lease held by someone else is a no-op, not an error.

## Development

Uses [Task](https://taskfile.dev):

```bash
task build
task test    # needs a running Docker daemon
task lint
```

Tests are testify suites backed by a real PostgreSQL container via [testcontainers](https://testcontainers.com/), one container per suite. There is no mocked-database path: election semantics are only meaningful against a real server.

## License

MIT — see [LICENSE](LICENSE).
