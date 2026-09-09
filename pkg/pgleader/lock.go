package pgleader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/barnowlsnest/pgqs-harness/postgres"
)

var (
	ErrFailedToAcquireLock = errors.New("failed to acquire advisory lock")
	ErrLockAlreadyAcquired = errors.New("advisory lock is already acquired by another process")
	ErrFailedToReleaseLock = errors.New("failed to release advisory lock")
	ErrLockNotHeld         = errors.New("advisory lock was not held by this process")
	ErrFailedAcquireConn   = errors.New("failed to acquire connection from pool")
)

// Lock represents a session-scoped advisory lock in PostgreSQL. It holds the lock key and the database connection used to acquire the lock.
type Lock struct {
	key         int64
	conn        *postgres.DBConn
	releaseOnce sync.Once
	releaseErr  error
	released    atomic.Bool
}

// AcquireLock takes a session-scoped advisory lock. The caller must keep conn
// alive for as long as the lock is needed; releasing conn to a pool while the
// lock is held strands it.
func AcquireLock(ctx context.Context, pool *postgres.DBPool, key int64) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context error before acquiring lock: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConn
	}

	var acquired bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		defer conn.Release()
		return nil, fmt.Errorf("%w: lock %d: %v", ErrFailedToAcquireLock, key, err)
	}
	if !acquired {
		defer conn.Release()
		return nil, fmt.Errorf("%w: lock %d", ErrLockAlreadyAcquired, key)
	}

	return &Lock{
		key:  key,
		conn: conn,
	}, nil
}

// Release releases the advisory lock and returns the connection to the pool. It
// is safe to call Release multiple times: the unlock runs once and every call
// returns the same result.
func (l *Lock) Release(ctx context.Context) error {
	l.releaseOnce.Do(func() {
		defer l.conn.Release()
		l.released.Store(true)

		var released bool
		if err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, l.key).Scan(&released); err != nil {
			l.releaseErr = fmt.Errorf("%w: lock %d: %v", ErrFailedToReleaseLock, l.key, err)
			return
		}
		if !released {
			l.releaseErr = fmt.Errorf("%w: lock %d", ErrLockNotHeld, l.key)
		}
	})

	return l.releaseErr
}

// IsHeld reports whether the lock is still held by this process, i.e. Release
// has not yet run. It tracks local state only and does not query the server.
func (l *Lock) IsHeld() bool {
	return !l.released.Load()
}

// Key returns the advisory lock key associated with this Lock instance.
func (l *Lock) Key() int64 {
	return l.key
}
