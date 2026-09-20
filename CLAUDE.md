# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

The repo uses [Task](https://taskfile.dev); prefer it over raw `go` commands so lint and test flags stay consistent.

```bash
task build
task test     # go clean -testcache && go test -v -race -timeout 5m ./...
task lint     # go vet, go fmt, goimports, golangci-lint run --fix
task update   # go mod tidy with GOPRIVATE set for github.com/barnowlsnest/*
```

`task test` requires a running Docker daemon — every test spins up a `postgres:16-alpine` testcontainer, one per suite (`SetupSuite`). There are no unit-only tests.

To narrow a run (testify suites, so the subtest name is part of the path):

```bash
go test ./pkg/pgleader -run TestServiceSuite
go test ./pkg/pgleader -run 'TestServiceSuite/TestRunElectsSingleLeader' -v
```

## Architecture

Library (no `main`) providing PostgreSQL-backed leader election. Two packages:

- `pkg/database` — embeds `migrations/*.sql` and exposes `RollOut`/`RollDown`, delegating to `pgqs-harness/v2/db`. The single table is `leases`, keyed by `hash_key bigint` (no `id` column).
- `pkg/pgleader` — the election itself, layered as lock → lease → service.

### The three layers

1. **`lock.go`** — `Lock` wraps a *session-scoped* `pg_try_advisory_lock`, pinning one pooled connection for the lock's lifetime. Release is `sync.Once`-guarded; if the unlock statement itself fails, the connection is **hijacked and closed** rather than returned, because a session that still holds the lock would leak it to the next borrower of that connection.
2. **`lease.go`** — `LeaseRepo` over `db.BaseDAO[LeaseRecord]`. `BaseDAO`'s generic helpers default to an `id` column, so every keyed call must pass `colHashKey` explicitly (`GetByCol`, `DeleteByCol`, `UpdateWithColName`).
3. **`service.go`** — `Service` combines them. Mutual exclusion comes from the advisory lock keyed by `HashKey(resource)`: every read-decide-write cycle on the lease row happens under that lock. The lock is taken and released **per operation**, never held for a whole leadership term, so no pooled connection is stranded while a leader runs.

### Invariants worth preserving

- **Context detachment on release paths.** `Run` and every guarded operation return with the caller's context already canceled. `unlock`, `Lock.discard`, and `releaseOnShutdown` all build fresh contexts via `context.WithoutCancel` / `context.Background` plus a timeout. An unlock issued on a canceled context never reaches the server but still returns the connection to the pool.
- **A failed renewal is not a lost leadership.** In `Service.campaign`, only `ErrLeaseLost` (or the locally recorded expiry passing) triggers resignation. Other errors — a challenger momentarily holding the advisory lock, a dropped connection — are transient and the loop keeps leading, otherwise leadership flaps whenever a challenger polls.
- **`HashKey` reinterprets, never truncates.** `int64(murmur3.Sum64(...))` is a bit-for-bit conversion (hence the `gosec` nolint); ~half the keys are negative and that is correct.
- **One `Run` loop per resource per `Service`.** `beginRun`/`endRun` register terms in `Service.terms`; a second concurrent `Run` gets `ErrRunInProgress`.
- **Expiry is compared against `Service.now`**, not the database clock — `WithClock` exists so tests can advance time instead of sleeping.
- **Nothing is swallowed.** Errors on paths that cannot return one (deferred unlocks, shutdown release, the campaign loop) are logged through `go-logslib/v2/pkg/sharedlog`, the same logger the harness uses. Do not reintroduce `_ =` on an error.
- Sentinel errors are exported and compared with `errors.Is`; callers distinguish "not leader right now" (`ErrLeaseHeldByOther`, `ErrLockAlreadyAcquired`) from real failures.

## Conventions

- Comments explain *why* — the non-obvious constraint (connection stranding, context cancellation, hash reinterpretation) — never what the code plainly says. Do not add banner dividers or comments that restate a name.
- `goimports` local prefix is `github.com/barnowlsnest/pgqs-leaderelection`, so this module's imports form their own final group.
- Lint budget: 150-char lines, functions ≤100 lines / 50 statements, cyclomatic complexity ≤18; `gocritic` runs with all tags enabled.
