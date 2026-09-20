package pgleader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/pgqs-harness/v2/db"
	"github.com/barnowlsnest/pgqs-harness/v2/postgres"
	"github.com/twmb/murmur3"
)

// Defaults used by New when the matching option is not supplied.
const (
	DefaultTTL           = 15 * time.Second
	DefaultRenewInterval = 5 * time.Second
	DefaultRetryInterval = 5 * time.Second

	// releaseTimeout bounds the best-effort lease release performed when Run
	// returns, since the caller's context is already done at that point.
	releaseTimeout = 5 * time.Second

	// unlockTimeout bounds the advisory-lock release that ends every guarded
	// operation.
	unlockTimeout = 5 * time.Second
)

var (
	ErrNilLeaseRepo    = errors.New("lease repository cannot be nil")
	ErrNilPool         = errors.New("connection pool cannot be nil")
	ErrEmptyIdentity   = errors.New("identity cannot be empty")
	ErrEmptyResource   = errors.New("resource cannot be empty")
	ErrInvalidTTL      = errors.New("ttl must be positive")
	ErrInvalidInterval = errors.New("interval must be positive")

	// ErrLeaseHeldByOther is returned when another, still-valid holder owns the lease.
	ErrLeaseHeldByOther = errors.New("lease is held by another process")
	// ErrLeaseLost is returned when renewing a lease this process no longer owns.
	ErrLeaseLost = errors.New("lease was lost")
	// ErrRunInProgress is returned when a Run loop is already campaigning for a resource.
	ErrRunInProgress = errors.New("a campaign for this resource is already running")
)

type (
	LeaseRecordsRepo interface {
		Create(ctx context.Context, lease *LeaseRecord) (*LeaseRecord, error)
		Read(ctx context.Context, key int64) (*LeaseRecord, error)
		Update(ctx context.Context, lease *LeaseRecord) (*LeaseRecord, error)
		Delete(ctx context.Context, key int64) error
	}

	// Callbacks are invoked by Run as leadership changes. Both are optional.
	Callbacks struct {
		// OnElected runs in its own goroutine once this process becomes leader.
		// Its context is canceled as soon as leadership is lost or Run returns,
		// so leader-only work must honor it.
		OnElected func(ctx context.Context)
		// OnResigned runs after leadership is lost, once the OnElected context
		// has been canceled.
		OnResigned func()
	}

	// Service elects a single leader per resource. Mutual exclusion comes from a
	// session-scoped advisory lock keyed by the resource hash: every read-decide-write
	// cycle on the leases row happens while that lock is held, so two processes can
	// never evaluate the same lease concurrently. The lock is taken and released per
	// operation, never held for the whole leadership term, so no pooled connection is
	// stranded while a leader is running.
	Service struct {
		leases   LeaseRecordsRepo
		pool     *postgres.DBPool
		identity string

		ttl           time.Duration
		renewInterval time.Duration
		retryInterval time.Duration
		now           func() time.Time

		// mu guards terms, the registry of in-flight Run loops keyed by resource.
		mu    sync.Mutex
		terms map[string]*term
	}

	// Option customizes a Service built by New.
	Option func(*Service) error
)

// WithTTL sets how long an acquired lease stays valid without a renewal.
func WithTTL(ttl time.Duration) Option {
	return func(s *Service) error {
		if ttl <= 0 {
			return fmt.Errorf("%w: ttl=%s", ErrInvalidTTL, ttl)
		}
		s.ttl = ttl
		return nil
	}
}

// WithRenewInterval sets how often Run renews the lease while leading. It should
// be comfortably smaller than the TTL so a slow renewal does not expire the lease.
func WithRenewInterval(d time.Duration) Option {
	return func(s *Service) error {
		if d <= 0 {
			return fmt.Errorf("%w: renew=%s", ErrInvalidInterval, d)
		}
		s.renewInterval = d
		return nil
	}
}

// WithRetryInterval sets how often Run retries acquisition while not leading.
func WithRetryInterval(d time.Duration) Option {
	return func(s *Service) error {
		if d <= 0 {
			return fmt.Errorf("%w: retry=%s", ErrInvalidInterval, d)
		}
		s.retryInterval = d
		return nil
	}
}

// WithClock replaces the time source, primarily for tests. Note that lease
// expiry is compared against this clock, not the database clock.
func WithClock(now func() time.Time) Option {
	return func(s *Service) error {
		if now == nil {
			return errors.New("clock cannot be nil")
		}
		s.now = now
		return nil
	}
}

// New builds a Service. identity must be unique per process (for example
// hostname + pid); it is written to the lease row as the holder.
func New(pool *postgres.DBPool, leases LeaseRecordsRepo, identity string, opts ...Option) (*Service, error) {
	switch {
	case pool == nil:
		return nil, ErrNilPool
	case leases == nil:
		return nil, ErrNilLeaseRepo
	case identity == "":
		return nil, ErrEmptyIdentity
	}

	s := &Service{
		leases:        leases,
		pool:          pool,
		identity:      identity,
		ttl:           DefaultTTL,
		renewInterval: DefaultRenewInterval,
		retryInterval: DefaultRetryInterval,
		now:           time.Now,
		terms:         make(map[string]*term),
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// HashKey maps a resource name onto the bigint lease/advisory-lock key. The
// murmur3 digest is reinterpreted, not truncated: every hash has exactly one
// matching int64, so equality lookups and the leases primary key stay correct.
// Roughly half of all keys are negative, which is expected.
func HashKey(resource string) int64 {
	//nolint:gosec // G115: deliberate bit-for-bit reinterpretation, not a range conversion; see doc comment.
	return int64(murmur3.Sum64([]byte(resource)))
}

// Identity returns the holder name this Service writes to lease rows.
func (s *Service) Identity() string {
	return s.identity
}

// IsLeader reports whether the Run loop for resource currently believes this
// process holds the lease. It reflects local state as of the last acquire/renew
// and does not query the database; it is false when no Run loop is campaigning
// for resource.
func (s *Service) IsLeader(resource string) bool {
	s.mu.Lock()
	t := s.terms[resource]
	s.mu.Unlock()

	return t != nil && t.leading.Load()
}

// TryAcquire makes a single attempt to become leader of resource. It succeeds
// when the lease is free, already owned by this identity, or owned by an expired
// holder. It returns ErrLeaseHeldByOther when a valid lease belongs to someone
// else, and ErrLockAlreadyAcquired when another process is mid-decision on the
// same resource; both mean "not leader right now", not a failure.
func (s *Service) TryAcquire(ctx context.Context, resource string) (*LeaseRecord, error) {
	if resource == "" {
		return nil, ErrEmptyResource
	}

	key := HashKey(resource)

	lock, err := AcquireLock(ctx, s.pool, key)
	if err != nil {
		return nil, err
	}
	defer unlock(ctx, lock)

	current, err := s.read(ctx, key)
	if err != nil {
		return nil, err
	}

	now := s.now()
	switch {
	case current == nil:
		return s.leases.Create(ctx, s.newLease(key, now))
	case current.Holder == s.identity:
		return s.leases.Update(ctx, s.extend(current, now))
	case now.Before(current.ExpiresAt):
		return nil, fmt.Errorf("%w: resource %q held by %q until %s",
			ErrLeaseHeldByOther, resource, current.Holder, current.ExpiresAt.UTC().Format(time.RFC3339))
	default:
		// The previous holder let the lease expire, so it is ours to take over.
		current.Holder = s.identity
		current.AcquiredAt = now
		return s.leases.Update(ctx, s.extend(current, now))
	}
}

// Renew extends a lease this process already holds. It returns ErrLeaseLost if
// the row disappeared, was taken over by another holder, or expired before the
// renewal reached the database.
func (s *Service) Renew(ctx context.Context, resource string) (*LeaseRecord, error) {
	if resource == "" {
		return nil, ErrEmptyResource
	}

	key := HashKey(resource)

	lock, err := AcquireLock(ctx, s.pool, key)
	if err != nil {
		return nil, err
	}
	defer unlock(ctx, lock)

	current, err := s.read(ctx, key)
	if err != nil {
		return nil, err
	}

	now := s.now()
	switch {
	case current == nil:
		return nil, fmt.Errorf("%w: resource %q has no lease row", ErrLeaseLost, resource)
	case current.Holder != s.identity:
		return nil, fmt.Errorf("%w: resource %q now held by %q", ErrLeaseLost, resource, current.Holder)
	case !now.Before(current.ExpiresAt):
		return nil, fmt.Errorf("%w: resource %q lease expired at %s",
			ErrLeaseLost, resource, current.ExpiresAt.UTC().Format(time.RFC3339))
	}

	return s.leases.Update(ctx, s.extend(current, now))
}

// Release gives up the lease so the next candidate can take over immediately
// instead of waiting for expiry. Releasing a lease held by somebody else is a
// no-op, not an error.
func (s *Service) Release(ctx context.Context, resource string) error {
	if resource == "" {
		return ErrEmptyResource
	}

	key := HashKey(resource)

	lock, err := AcquireLock(ctx, s.pool, key)
	if err != nil {
		return err
	}
	defer unlock(ctx, lock)

	current, err := s.read(ctx, key)
	if err != nil {
		return err
	}
	if current == nil || current.Holder != s.identity {
		return nil
	}

	return s.leases.Delete(ctx, key)
}

// Run campaigns for resource until ctx is done, acquiring the lease, renewing it
// every renew interval while leading and retrying every retry interval while
// not. Leadership transitions are reported through cb. Run always returns
// ctx.Err(); transient acquire/renew failures are treated as "not leader" and
// retried rather than surfaced.
func (s *Service) Run(ctx context.Context, resource string, cb Callbacks) error {
	if resource == "" {
		return ErrEmptyResource
	}

	t, err := s.beginRun(resource)
	if err != nil {
		return err
	}
	defer func() {
		s.resign(t, cb)
		s.endRun(resource)
		s.releaseOnShutdown(resource)
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		s.campaign(ctx, resource, t, cb)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.interval(t)):
		}
	}
}

// campaign runs a single iteration of the election loop: renew the lease while
// leading, otherwise try to take it.
//
// A failed renewal is not by itself a loss of leadership. Only ErrLeaseLost
// says another process owns the row; anything else is transient (a challenger
// momentarily holding the advisory lock, a dropped connection) and the lease we
// already hold stays valid until its recorded expiry, so the loop keeps leading
// and retries. Resigning on the first transient error would hand leadership
// back and forth whenever a challenger polls.
func (s *Service) campaign(ctx context.Context, resource string, t *term, cb Callbacks) {
	if t.leading.Load() {
		rec, err := s.Renew(ctx, resource)
		switch {
		case err == nil:
			t.expiry = rec.ExpiresAt
		case errors.Is(err, ErrLeaseLost), !s.now().Before(t.expiry):
			s.resign(t, cb)
		}

		return
	}

	rec, err := s.TryAcquire(ctx, resource)
	if err != nil {
		return
	}

	t.expiry = rec.ExpiresAt
	leaderCtx := t.begin(ctx)
	if cb.OnElected != nil {
		go cb.OnElected(leaderCtx)
	}
}

// beginRun registers the leadership term for resource. Two concurrent Run loops
// on one Service would campaign for the same lease row under the same identity
// and fight over each other's callbacks, so the second caller is rejected.
func (s *Service) beginRun(resource string) (*term, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, busy := s.terms[resource]; busy {
		return nil, fmt.Errorf("%w: resource %q", ErrRunInProgress, resource)
	}

	t := &term{}
	s.terms[resource] = t

	return t, nil
}

// endRun deregisters the term so a later Run can campaign for resource again.
func (s *Service) endRun(resource string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.terms, resource)
}

// term is the leadership state of a single Run loop: whether that loop holds
// the lease, when the lease it holds runs out, and the cancel func of the
// context handed to OnElected. leading is atomic because IsLeader reads it from
// other goroutines; stop and expiry are touched only by the owning Run
// goroutine.
type term struct {
	leading atomic.Bool
	stop    context.CancelFunc
	expiry  time.Time
}

// begin marks the start of a leadership term and derives its leader context.
func (t *term) begin(ctx context.Context) context.Context {
	leaderCtx, stop := context.WithCancel(ctx)
	t.stop = stop
	t.leading.Store(true)

	return leaderCtx
}

// resign cancels the leader context and reports the loss exactly once per term.
func (s *Service) resign(t *term, cb Callbacks) {
	if !t.leading.Swap(false) {
		return
	}

	if t.stop != nil {
		t.stop()
		t.stop = nil
	}
	if cb.OnResigned != nil {
		cb.OnResigned()
	}
}

// unlock drops the advisory lock on a context derived from ctx but detached from
// its cancellation, because ctx is routinely already canceled by the time the
// deferred unlock runs. pg_advisory_unlock issued on a canceled context never
// reaches the server, yet Lock.Release still returns the connection to the pool
// while its session still holds the lock, stranding it for the next borrower.
func unlock(ctx context.Context, lock *Lock) {
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()

	_ = lock.Release(unlockCtx)
}

// releaseOnShutdown drops the lease on a fresh context, because Run only returns
// once the caller's context is already canceled.
func (s *Service) releaseOnShutdown(resource string) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()

	_ = s.Release(ctx, resource)
}

func (s *Service) interval(t *term) time.Duration {
	if t.leading.Load() {
		return s.renewInterval
	}

	return s.retryInterval
}

// read returns the lease row for key, or (nil, nil) when no row exists.
func (s *Service) read(ctx context.Context, key int64) (*LeaseRecord, error) {
	lease, err := s.leases.Read(ctx, key)
	if errors.Is(err, db.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return lease, nil
}

// newLease builds the row for a resource nobody has ever claimed.
func (s *Service) newLease(key int64, now time.Time) *LeaseRecord {
	return &LeaseRecord{
		HashKey:    key,
		Holder:     s.identity,
		Version:    1,
		AcquiredAt: now,
		RenewedAt:  now,
		ExpiresAt:  now.Add(s.ttl),
	}
}

// extend pushes the expiry of an existing row out by one TTL and bumps its
// version, so a stale writer's view is always distinguishable from a fresh one.
func (s *Service) extend(lease *LeaseRecord, now time.Time) *LeaseRecord {
	lease.Version++
	lease.RenewedAt = now
	lease.ExpiresAt = now.Add(s.ttl)

	return lease
}
