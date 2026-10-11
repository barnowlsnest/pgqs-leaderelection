package pgleader_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/barnowlsnest/pgqs-harness/v2/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/barnowlsnest/pgqs-leaderelection/pkg/database"
	"github.com/barnowlsnest/pgqs-leaderelection/pkg/pgleader"
)

const (
	// Intervals used by the Run tests. They are short enough to keep the suite
	// quick and long enough that a loop iteration comfortably outlives a local
	// round trip to the container.
	testTTL    = 2 * time.Second
	testRenew  = 100 * time.Millisecond
	testRetry  = 50 * time.Millisecond
	testWait   = 10 * time.Second
	testTick   = 10 * time.Millisecond
	testSchema = "pgqs"
	testTable  = "leases"

	// incumbent is the identity that mustAcquire installs as the current holder.
	incumbent = "node-a"
)

// testClock is a manually advanced time source, so lease expiry can be reached
// without sleeping.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Now()}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// signal is a one-shot notification. Callbacks may run more than once over a
// Run loop's lifetime — a leader that genuinely loses and regains the lease is
// elected again — so firing must be idempotent.
type signal struct {
	once sync.Once
	ch   chan struct{}
}

func newSignal() *signal {
	return &signal{ch: make(chan struct{})}
}

func (s *signal) fire() {
	s.once.Do(func() { close(s.ch) })
}

type ServiceSuite struct {
	suite.Suite
	container *tcpostgres.PostgresContainer
	connStr   string
	pool      *pgxpool.Pool
	repo      *pgleader.LeaseRepo
	clock     *testClock
}

func TestServiceSuite(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}

func (s *ServiceSuite) SetupSuite() {
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("lease_test"),
		tcpostgres.WithUsername("lease"),
		tcpostgres.WithPassword("lease"),
		tcpostgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.container = ctr

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	s.Require().NoError(err)
	s.connStr = connStr

	s.Require().NoError(database.RollOut(ctx, connStr))

	pool, err := pgxpool.New(ctx, connStr)
	s.Require().NoError(err)
	s.pool = pool
	s.Require().NoError(pool.Ping(ctx))

	repo, err := pgleader.NewLeaseRepo(db.NewBaseDAO[pgleader.LeaseRecord](testSchema, testTable, pool))
	s.Require().NoError(err)
	s.repo = repo
}

func (s *ServiceSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.container != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.Require().NoError(s.container.Terminate(shutdownCtx))
	}
}

func (s *ServiceSuite) SetupTest() {
	_, err := s.pool.Exec(s.T().Context(), `TRUNCATE `+testSchema+`.`+testTable)
	s.Require().NoError(err)

	s.clock = newTestClock()
}

// A restarted service rolls out against a database that is already migrated.
func (s *ServiceSuite) TestRollOutOnMigratedDatabase() {
	s.Require().NoError(database.RollOut(s.T().Context(), s.connStr))
}

// newService builds a Service on the shared pool and repo, driven by the
// suite's manual clock unless an option overrides it.
func (s *ServiceSuite) newService(identity string, opts ...pgleader.Option) *pgleader.Service {
	s.T().Helper()

	all := append([]pgleader.Option{
		pgleader.WithTTL(testTTL),
		pgleader.WithClock(s.clock.Now),
	}, opts...)

	svc, err := pgleader.New(s.pool, s.repo, identity, all...)
	s.Require().NoError(err)

	return svc
}

// realClockOpts drive a Run loop off the wall clock at test speed.
func realClockOpts() []pgleader.Option {
	return []pgleader.Option{
		pgleader.WithClock(time.Now),
		pgleader.WithRenewInterval(testRenew),
		pgleader.WithRetryInterval(testRetry),
	}
}

// lease reads the stored row for resource, or nil when there is none.
func (s *ServiceSuite) lease(resource string) *pgleader.LeaseRecord {
	s.T().Helper()

	rec, err := s.repo.Read(s.T().Context(), pgleader.HashKey(resource))
	if err != nil {
		s.Require().ErrorIs(err, db.ErrNotFound)
		return nil
	}

	return rec
}

// assertLockFree checks from a separate session that the resource's advisory
// lock was handed back, i.e. no operation stranded it on a pooled connection.
func (s *ServiceSuite) assertLockFree(resource string) {
	s.T().Helper()

	ctx := s.T().Context()
	conn, err := s.pool.Acquire(ctx)
	s.Require().NoError(err)
	defer conn.Release()

	var grabbed bool
	s.Require().NoError(conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, pgleader.HashKey(resource)).Scan(&grabbed))
	if grabbed {
		_, err = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, pgleader.HashKey(resource))
		s.Require().NoError(err)
	}
	s.Truef(grabbed, "advisory lock for %q was left held", resource)
}

func (s *ServiceSuite) TestHashKey_isDeterministicAndDistinct() {
	s.Equal(pgleader.HashKey("orders"), pgleader.HashKey("orders"))
	s.NotEqual(pgleader.HashKey("orders"), pgleader.HashKey("payments"))
}

// Roughly half of all murmur3 digests exceed MaxInt64 and therefore land on a
// negative bigint. Such a key must still round-trip through the table and the
// advisory lock.
func (s *ServiceSuite) TestHashKey_negativeKeyRoundTrips() {
	const resource = "probe-1" // murmur3 digest exceeds MaxInt64

	key := pgleader.HashKey(resource)
	s.Require().Negativef(key, "expected %q to hash negative; pick another probe string", resource)

	svc := s.newService("node-a")
	rec, err := svc.TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)
	s.Equal(key, rec.HashKey)
	s.Equal(key, s.lease(resource).HashKey)
}

func (s *ServiceSuite) TestNew_validatesArguments() {
	_, err := pgleader.New(nil, s.repo, "node-a")
	s.Require().ErrorIs(err, pgleader.ErrNilPool)

	_, err = pgleader.New(s.pool, nil, "node-a")
	s.Require().ErrorIs(err, pgleader.ErrNilLeaseRepo)

	_, err = pgleader.New(s.pool, s.repo, "")
	s.Require().ErrorIs(err, pgleader.ErrEmptyIdentity)
}

func (s *ServiceSuite) TestNew_validatesOptions() {
	_, err := pgleader.New(s.pool, s.repo, "node-a", pgleader.WithTTL(0))
	s.Require().ErrorIs(err, pgleader.ErrInvalidTTL)

	_, err = pgleader.New(s.pool, s.repo, "node-a", pgleader.WithRenewInterval(-time.Second))
	s.Require().ErrorIs(err, pgleader.ErrInvalidInterval)

	_, err = pgleader.New(s.pool, s.repo, "node-a", pgleader.WithRetryInterval(0))
	s.Require().ErrorIs(err, pgleader.ErrInvalidInterval)

	_, err = pgleader.New(s.pool, s.repo, "node-a", pgleader.WithClock(nil))
	s.Require().Error(err)
}

func (s *ServiceSuite) TestIdentity_returnsHolderName() {
	s.Equal("node-a", s.newService("node-a").Identity())
}

func (s *ServiceSuite) TestOperations_rejectEmptyResource() {
	svc := s.newService("node-a")

	_, err := svc.TryAcquire(s.T().Context(), "")
	s.Require().ErrorIs(err, pgleader.ErrEmptyResource)

	_, err = svc.Renew(s.T().Context(), "")
	s.Require().ErrorIs(err, pgleader.ErrEmptyResource)

	s.Require().ErrorIs(svc.Release(s.T().Context(), ""), pgleader.ErrEmptyResource)
	s.Require().ErrorIs(svc.Run(s.T().Context(), "", pgleader.Callbacks{}), pgleader.ErrEmptyResource)
}

func (s *ServiceSuite) TestTryAcquire_createsLeaseWhenFree() {
	const resource = "orders"

	svc := s.newService("node-a")
	now := s.clock.Now()

	rec, err := svc.TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)

	s.Equal("node-a", rec.Holder)
	s.Equal(pgleader.HashKey(resource), rec.HashKey)
	s.EqualValues(1, rec.Version)
	s.WithinDuration(now, rec.AcquiredAt, time.Millisecond)
	s.WithinDuration(now, rec.RenewedAt, time.Millisecond)
	s.WithinDuration(now.Add(testTTL), rec.ExpiresAt, time.Millisecond)

	s.Equal("node-a", s.lease(resource).Holder)
	s.assertLockFree(resource)
}

func (s *ServiceSuite) TestTryAcquire_isReentrantForSameHolder() {
	const resource = "orders"

	svc := s.newService("node-a")
	first, err := svc.TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)

	s.clock.Advance(time.Second)
	second, err := svc.TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)

	s.Equal("node-a", second.Holder)
	s.Equal(first.Version+1, second.Version)
	// Re-acquiring our own lease extends it without restarting the term.
	s.WithinDuration(first.AcquiredAt, second.AcquiredAt, time.Millisecond)
	s.True(second.ExpiresAt.After(first.ExpiresAt))
}

func (s *ServiceSuite) TestTryAcquire_rejectedWhileHeldByOther() {
	const resource = "orders"

	s.Require().NotNil(s.mustAcquire(resource))

	challenger := s.newService("node-b")
	s.clock.Advance(testTTL - time.Millisecond)

	rec, err := challenger.TryAcquire(s.T().Context(), resource)
	s.Require().ErrorIs(err, pgleader.ErrLeaseHeldByOther)
	s.Nil(rec)

	s.Equal("node-a", s.lease(resource).Holder)
	s.assertLockFree(resource)
}

func (s *ServiceSuite) TestTryAcquire_stealsExpiredLease() {
	const resource = "shipments"

	held := s.mustAcquire(resource)

	challenger := s.newService("node-b")
	s.clock.Advance(testTTL + time.Millisecond)
	stolenAt := s.clock.Now()

	rec, err := challenger.TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)

	s.Equal("node-b", rec.Holder)
	s.Equal(held.Version+1, rec.Version)
	// A takeover starts a new term, so acquired_at moves too.
	s.WithinDuration(stolenAt, rec.AcquiredAt, time.Millisecond)
	s.WithinDuration(stolenAt.Add(testTTL), rec.ExpiresAt, time.Millisecond)

	s.Equal("node-b", s.lease(resource).Holder)
}

// A second campaigner that arrives while the first is mid-decision is turned
// away by the advisory lock rather than by the lease row.
func (s *ServiceSuite) TestTryAcquire_rejectedWhileAnotherProcessDecides() {
	const resource = "orders"

	blocker, err := pgleader.AcquireLock(s.T().Context(), s.pool, pgleader.HashKey(resource))
	s.Require().NoError(err)
	defer func() { _ = blocker.Release(s.T().Context()) }()

	rec, err := s.newService("node-a").TryAcquire(s.T().Context(), resource)
	s.Require().ErrorIs(err, pgleader.ErrLockAlreadyAcquired)
	s.Nil(rec)
	s.Nil(s.lease(resource))
}

func (s *ServiceSuite) TestRenew_extendsOwnLease() {
	const resource = "orders"

	held := s.mustAcquire(resource)
	svc := s.newService("node-a")

	s.clock.Advance(time.Second)
	renewedAt := s.clock.Now()

	rec, err := svc.Renew(s.T().Context(), resource)
	s.Require().NoError(err)

	s.Equal("node-a", rec.Holder)
	s.Equal(held.Version+1, rec.Version)
	s.WithinDuration(renewedAt, rec.RenewedAt, time.Millisecond)
	s.WithinDuration(renewedAt.Add(testTTL), rec.ExpiresAt, time.Millisecond)
	s.assertLockFree(resource)
}

func (s *ServiceSuite) TestRenew_lostWhenRowMissing() {
	rec, err := s.newService("node-a").Renew(s.T().Context(), "orders")
	s.Require().ErrorIs(err, pgleader.ErrLeaseLost)
	s.Nil(rec)
}

func (s *ServiceSuite) TestRenew_lostWhenHolderChanged() {
	const resource = "invoices"

	s.mustAcquire(resource)

	rec, err := s.newService("node-b").Renew(s.T().Context(), resource)
	s.Require().ErrorIs(err, pgleader.ErrLeaseLost)
	s.Nil(rec)

	// The rejected renewal must not have touched node-a's row.
	s.Equal("node-a", s.lease(resource).Holder)
}

func (s *ServiceSuite) TestRenew_lostWhenLeaseAlreadyExpired() {
	const resource = "orders"

	s.mustAcquire(resource)
	svc := s.newService("node-a")
	s.clock.Advance(testTTL)

	rec, err := svc.Renew(s.T().Context(), resource)
	s.Require().ErrorIs(err, pgleader.ErrLeaseLost)
	s.Nil(rec)
}

func (s *ServiceSuite) TestRelease_deletesOwnLease() {
	const resource = "orders"

	s.mustAcquire(resource)

	s.Require().NoError(s.newService("node-a").Release(s.T().Context(), resource))
	s.Nil(s.lease(resource))
	s.assertLockFree(resource)
}

func (s *ServiceSuite) TestRelease_isNoOpForOtherHolder() {
	const resource = "orders"

	s.mustAcquire(resource)

	s.Require().NoError(s.newService("node-b").Release(s.T().Context(), resource))
	s.Equal("node-a", s.lease(resource).Holder)
}

func (s *ServiceSuite) TestRelease_isNoOpWhenNoLease() {
	s.Require().NoError(s.newService("node-a").Release(s.T().Context(), "orders"))
}

// A released lease is immediately available, without waiting out the TTL.
func (s *ServiceSuite) TestRelease_allowsImmediateTakeover() {
	const resource = "refunds"

	s.mustAcquire(resource)
	s.Require().NoError(s.newService("node-a").Release(s.T().Context(), resource))

	rec, err := s.newService("node-b").TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)
	s.Equal("node-b", rec.Holder)
	s.EqualValues(1, rec.Version)
}

func (s *ServiceSuite) TestRun_electsLeaderAndReleasesOnShutdown() {
	const resource = "orders"

	svc := s.newService("node-a", realClockOpts()...)
	ctx, cancel := context.WithCancel(s.T().Context())

	elected, resigned := newSignal(), newSignal()
	done := s.runAsync(ctx, svc, resource, pgleader.Callbacks{
		OnElected: func(leaderCtx context.Context) {
			elected.fire()
			<-leaderCtx.Done()
		},
		OnResigned: func() { resigned.fire() },
	})

	s.awaitFired(elected, "OnElected")
	s.True(svc.IsLeader(resource))
	s.Equal("node-a", s.lease(resource).Holder)

	cancel()
	s.Require().ErrorIs(<-done, context.Canceled)

	s.awaitFired(resigned, "OnResigned")
	s.False(svc.IsLeader(resource))
	s.Nil(s.lease(resource), "Run should release the lease on shutdown")
	s.assertLockFree(resource)
}

// The lease is a live claim, so a leader keeps pushing expires_at out.
func (s *ServiceSuite) TestRun_renewsLeaseWhileLeading() {
	const resource = "orders"

	svc := s.newService("node-a", realClockOpts()...)
	ctx, cancel := context.WithCancel(s.T().Context())
	defer cancel()

	elected := newSignal()
	done := s.runAsync(ctx, svc, resource, pgleader.Callbacks{
		OnElected: func(context.Context) { elected.fire() },
	})

	s.awaitFired(elected, "OnElected")
	first := s.lease(resource)

	s.Eventually(func() bool {
		return s.lease(resource).Version > first.Version
	}, testWait, testTick, "leader never renewed its lease")

	renewed := s.lease(resource)
	s.Equal("node-a", renewed.Holder)
	s.True(renewed.ExpiresAt.After(first.ExpiresAt))

	cancel()
	s.Require().ErrorIs(<-done, context.Canceled)
}

func (s *ServiceSuite) TestRun_rejectsSecondLoopForSameResource() {
	const resource = "orders"

	svc := s.newService("node-a", realClockOpts()...)
	ctx, cancel := context.WithCancel(s.T().Context())
	defer cancel()

	elected := newSignal()
	done := s.runAsync(ctx, svc, resource, pgleader.Callbacks{
		OnElected: func(context.Context) { elected.fire() },
	})
	s.awaitFired(elected, "OnElected")

	err := svc.Run(ctx, resource, pgleader.Callbacks{})
	s.Require().ErrorIs(err, pgleader.ErrRunInProgress)

	cancel()
	s.Require().ErrorIs(<-done, context.Canceled)
}

// Leadership is tracked per resource, so one Service can lead several at once
// and losing one must not disturb the other.
func (s *ServiceSuite) TestRun_tracksResourcesIndependently() {
	svc := s.newService("node-a", realClockOpts()...)
	ctx, cancel := context.WithCancel(s.T().Context())

	ordersCtx, cancelOrders := context.WithCancel(ctx)
	defer cancelOrders()

	electedOrders, electedPayments := newSignal(), newSignal()
	ordersDone := s.runAsync(ordersCtx, svc, "orders", pgleader.Callbacks{
		OnElected: func(context.Context) { electedOrders.fire() },
	})
	paymentsDone := s.runAsync(ctx, svc, "payments", pgleader.Callbacks{
		OnElected: func(context.Context) { electedPayments.fire() },
	})

	s.awaitFired(electedOrders, "OnElected(orders)")
	s.awaitFired(electedPayments, "OnElected(payments)")
	s.True(svc.IsLeader("orders"))
	s.True(svc.IsLeader("payments"))

	// Stopping the orders campaign leaves the payments one leading.
	cancelOrders()
	s.Require().ErrorIs(<-ordersDone, context.Canceled)
	s.False(svc.IsLeader("orders"))
	s.Nil(s.lease("orders"))

	s.True(svc.IsLeader("payments"))
	s.Equal("node-a", s.lease("payments").Holder)
	s.assertLockFree("payments")

	cancel()
	s.Require().ErrorIs(<-paymentsDone, context.Canceled)
}

func (s *ServiceSuite) TestIsLeader_falseWithoutCampaign() {
	s.False(s.newService("node-a").IsLeader("orders"))
}

// Exactly one of two competing processes leads, and the loser takes over once
// the winner stands down.
func (s *ServiceSuite) TestRun_singleLeaderAcrossProcesses() {
	const resource = "orders"

	nodeA := s.newService("node-a", realClockOpts()...)
	nodeB := s.newService("node-b", realClockOpts()...)

	ctxA, cancelA := context.WithCancel(s.T().Context())
	ctxB, cancelB := context.WithCancel(s.T().Context())
	defer cancelB()

	electedA := newSignal()
	doneA := s.runAsync(ctxA, nodeA, resource, pgleader.Callbacks{
		OnElected: func(context.Context) { electedA.fire() },
	})
	s.awaitFired(electedA, "OnElected(node-a)")

	electedB := newSignal()
	doneB := s.runAsync(ctxB, nodeB, resource, pgleader.Callbacks{
		OnElected: func(context.Context) { electedB.fire() },
	})

	// node-b must not win while node-a holds a valid lease.
	s.Never(func() bool { return nodeB.IsLeader(resource) }, 10*testRetry, testTick)
	s.True(nodeA.IsLeader(resource))
	s.Equal("node-a", s.lease(resource).Holder)

	// node-a stands down and releases, so node-b takes over on its next tick.
	cancelA()
	s.Require().ErrorIs(<-doneA, context.Canceled)

	s.awaitFired(electedB, "OnElected(node-b)")
	s.Equal("node-b", s.lease(resource).Holder)

	cancelB()
	s.Require().ErrorIs(<-doneB, context.Canceled)
}

// mustAcquire takes the lease for resource as the incumbent and returns the row.
func (s *ServiceSuite) mustAcquire(resource string) *pgleader.LeaseRecord {
	s.T().Helper()

	rec, err := s.newService(incumbent).TryAcquire(s.T().Context(), resource)
	s.Require().NoError(err)

	return rec
}

// runAsync starts svc.Run in the background and yields its return value.
func (s *ServiceSuite) runAsync(
	ctx context.Context, svc *pgleader.Service, resource string, cb pgleader.Callbacks,
) <-chan error {
	s.T().Helper()

	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx, resource, cb) }()

	return done
}

// awaitFired fails the test if sig has not fired within testWait.
func (s *ServiceSuite) awaitFired(sig *signal, what string) {
	s.T().Helper()

	select {
	case <-sig.ch:
	case <-time.After(testWait):
		s.FailNowf("timed out", "%s was never called", what)
	}
}

// hangingRepo delegates to a real repo but blocks every Update until release is
// closed, deliberately ignoring the context. It stands in for a renewal stuck on
// a dead connection, where cancellation alone does not get control back.
type hangingRepo struct {
	inner   pgleader.LeaseRecordsRepo
	release chan struct{}
	hung    chan struct{}
	once    sync.Once
}

func newHangingRepo(inner pgleader.LeaseRecordsRepo) *hangingRepo {
	return &hangingRepo{
		inner:   inner,
		release: make(chan struct{}),
		hung:    make(chan struct{}),
	}
}

func (r *hangingRepo) Create(ctx context.Context, lease *pgleader.LeaseRecord) (*pgleader.LeaseRecord, error) {
	return r.inner.Create(ctx, lease)
}

func (r *hangingRepo) Read(ctx context.Context, key int64) (*pgleader.LeaseRecord, error) {
	return r.inner.Read(ctx, key)
}

func (r *hangingRepo) Delete(ctx context.Context, key int64) error {
	return r.inner.Delete(ctx, key)
}

func (r *hangingRepo) Update(ctx context.Context, lease *pgleader.LeaseRecord) (*pgleader.LeaseRecord, error) {
	r.once.Do(func() { close(r.hung) })
	<-r.release

	return r.inner.Update(ctx, lease)
}

// TestRun_expiryCancelsLeaderContextWhileRenewalHangs pins the guarantee that
// the leader context dies with the lease. The renewal never returns, so the Run
// loop cannot notice the expiry; only the term's own timer can.
func (s *ServiceSuite) TestRun_expiryCancelsLeaderContextWhileRenewalHangs() {
	const resource = "hung-renew"

	repo := newHangingRepo(s.repo)
	svc, err := pgleader.New(s.pool, repo, "node-hung",
		pgleader.WithClock(time.Now),
		pgleader.WithTTL(time.Second),
		pgleader.WithRenewInterval(100*time.Millisecond),
		pgleader.WithRetryInterval(testRetry),
	)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	elected, resigned := newSignal(), newSignal()
	leaderCtx := make(chan context.Context, 1)

	done := s.runAsync(ctx, svc, resource, pgleader.Callbacks{
		OnElected: func(lctx context.Context) {
			leaderCtx <- lctx
			elected.fire()
		},
		OnResigned: resigned.fire,
	})

	s.awaitFired(elected, "OnElected")
	lctx := <-leaderCtx

	// The loop is now stuck inside the renewal and stays stuck.
	select {
	case <-repo.hung:
	case <-time.After(testWait):
		s.FailNow("timed out", "renewal was never attempted")
	}

	select {
	case <-lctx.Done():
	case <-time.After(testWait):
		s.FailNow("timed out", "leader context outlived the lease while the renewal hung")
	}
	s.awaitFired(resigned, "OnResigned")
	s.False(svc.IsLeader(resource), "still reports leadership after the lease expired")

	close(repo.release)
	cancel()

	select {
	case runErr := <-done:
		s.Require().ErrorIs(runErr, context.Canceled)
	case <-time.After(testWait):
		s.FailNow("timed out", "Run did not return")
	}
}
