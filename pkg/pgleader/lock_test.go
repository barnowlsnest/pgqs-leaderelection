package pgleader_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/barnowlsnest/pgqs-leaderelection/pkg/pgleader"
)

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

type LockSuite struct {
	suite.Suite
	container *tcpostgres.PostgresContainer
	pool      *pgxpool.Pool
}

func TestLockSuite(t *testing.T) {
	suite.Run(t, new(LockSuite))
}

func (s *LockSuite) SetupSuite() {
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("lock_test"),
		tcpostgres.WithUsername("lock"),
		tcpostgres.WithPassword("lock"),
		tcpostgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.container = ctr

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	s.Require().NoError(err)

	pool, err := pgxpool.New(ctx, connStr)
	s.Require().NoError(err)
	s.pool = pool

	s.Require().NoError(pool.Ping(ctx))
}

func (s *LockSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.container != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.Require().NoError(s.container.Terminate(shutdownCtx))
	}
}

// acquire takes the advisory lock for key and fails the test if it cannot.
func (s *LockSuite) acquire(key int64) *pgleader.Lock {
	s.T().Helper()

	lock, err := pgleader.AcquireLock(s.T().Context(), s.pool, key)
	s.Require().NoError(err)
	s.Require().NotNil(lock)
	return lock
}

// advisoryKeyLocked reports whether key is currently held, by checking from a
// separate pooled connection (a different Postgres session) whether the key can
// still be grabbed. If it can, nobody held it and we drop it again immediately.
func (s *LockSuite) advisoryKeyLocked(key int64) bool {
	s.T().Helper()

	ctx := s.T().Context()
	conn, err := s.pool.Acquire(ctx)
	s.Require().NoError(err)
	defer conn.Release()

	var grabbed bool
	s.Require().NoError(conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&grabbed))
	if grabbed {
		_, err = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, key)
		s.Require().NoError(err)
	}
	return !grabbed
}

func (s *LockSuite) assertLockHeld(key int64) {
	s.T().Helper()
	s.Truef(s.advisoryKeyLocked(key), "expected advisory lock %d to be held", key)
}

func (s *LockSuite) assertLockFree(key int64) {
	s.T().Helper()
	s.Falsef(s.advisoryKeyLocked(key), "expected advisory lock %d to be free", key)
}

// TestAcquireLock_roundTrip walks the acquire -> observe -> release -> observe
// cycle for a spread of key values across the full int64 range.
func (s *LockSuite) TestAcquireLock_roundTrip() {
	cases := []struct {
		name string
		key  int64
	}{
		{"positive key", 42},
		{"zero key", 0},
		{"negative key", -99},
		{"max int64", math.MaxInt64},
		{"min int64", math.MinInt64},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.assertLockFree(tc.key)

			lock := s.acquire(tc.key)
			s.Equal(tc.key, lock.Key())
			s.True(lock.IsHeld())
			s.assertLockHeld(tc.key)

			s.Require().NoError(lock.Release(s.T().Context()))
			s.False(lock.IsHeld())
			s.assertLockFree(tc.key)
		})
	}
}

func (s *LockSuite) TestAcquireLock_heldKeyRejected() {
	const key int64 = 700_001

	held := s.acquire(key)
	defer func() { _ = held.Release(s.T().Context()) }()

	// A second acquire runs on a different pooled connection, i.e. a different
	// PostgreSQL session, so pg_try_advisory_lock returns false.
	second, err := pgleader.AcquireLock(s.T().Context(), s.pool, key)
	s.Require().ErrorIs(err, pgleader.ErrLockAlreadyAcquired)
	s.Nil(second)

	// The rejected attempt must not have leaked the lock or its connection.
	s.assertLockHeld(key)
}

func (s *LockSuite) TestAcquireLock_reacquirableAfterRelease() {
	const key int64 = 700_002

	first := s.acquire(key)
	s.Require().NoError(first.Release(s.T().Context()))
	s.assertLockFree(key)

	second := s.acquire(key)
	s.assertLockHeld(key)
	s.Require().NoError(second.Release(s.T().Context()))
}

func (s *LockSuite) TestAcquireLock_distinctKeysIndependent() {
	const keyA int64 = 700_003
	const keyB int64 = 700_004

	lockA := s.acquire(keyA)
	defer func() { _ = lockA.Release(s.T().Context()) }()

	lockB := s.acquire(keyB)
	defer func() { _ = lockB.Release(s.T().Context()) }()

	s.assertLockHeld(keyA)
	s.assertLockHeld(keyB)
}

func (s *LockSuite) TestAcquireLock_contextCanceled() {
	const key int64 = 700_005

	lock, err := pgleader.AcquireLock(canceledCtx(), s.pool, key)
	s.Require().Error(err)
	s.Nil(lock)
	s.assertLockFree(key)
}

func (s *LockSuite) TestAcquireLock_poolClosed() {
	connStr, err := s.container.ConnectionString(s.T().Context(), "sslmode=disable")
	s.Require().NoError(err)

	pool, err := pgxpool.New(s.T().Context(), connStr)
	s.Require().NoError(err)
	pool.Close()

	lock, err := pgleader.AcquireLock(s.T().Context(), pool, 700_006)
	s.Require().ErrorIs(err, pgleader.ErrFailedAcquireConn)
	s.Nil(lock)
}

func (s *LockSuite) TestRelease_contextCanceled() {
	const key int64 = 700_008

	lock := s.acquire(key)

	err := lock.Release(canceledCtx())
	s.Require().ErrorIs(err, pgleader.ErrFailedToReleaseLock)

	// Release still returned the connection to the pool; the server drops the
	// session lock once that connection is reused/closed.
}

func (s *LockSuite) TestKey_returnsAcquiredKey() {
	const key int64 = 700_009

	lock := s.acquire(key)
	defer func() { _ = lock.Release(s.T().Context()) }()

	s.Equal(key, lock.Key())
}

func (s *LockSuite) TestIsHeld_tracksReleaseState() {
	const key int64 = 700_011

	lock := s.acquire(key)
	s.True(lock.IsHeld())

	s.Require().NoError(lock.Release(s.T().Context()))
	s.False(lock.IsHeld())

	// Idempotent Release keeps IsHeld stable.
	s.Require().NoError(lock.Release(s.T().Context()))
	s.False(lock.IsHeld())
}

// Release is guarded by sync.Once: the unlock happens on the first call and
// later calls are no-ops that report the same result without touching the
// already-returned connection.
func (s *LockSuite) TestRelease_isIdempotent() {
	const key int64 = 700_010

	lock := s.acquire(key)

	s.Require().NoError(lock.Release(s.T().Context()))
	s.assertLockFree(key)

	s.Require().NoError(lock.Release(s.T().Context()))
	s.Require().NoError(lock.Release(canceledCtx()))
	s.assertLockFree(key)
}
