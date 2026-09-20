package pgleader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
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
		// Its context is canceled when leadership is lost, when the lease
		// expires without a successful renewal, or when Run returns, so
		// leader-only work must honor it.
		//
		// Cancellation bounds this process's belief that it leads; it cannot
		// bound the work already in flight. A process frozen past its lease
		// expiry keeps running until it thaws, by which time another process may
		// hold the lease. Work that must never overlap has to be fenced at the
		// resource it writes, using LeaseRecord.Version as the fencing token.
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
//
// The expiry itself is enforced by a timer on the term, not by this loop, so a
// renewal that hangs cannot keep the term alive past the lease.
func (s *Service) campaign(ctx context.Context, resource string, t *term, cb Callbacks) {
	if t.leading.Load() {
		s.renewTerm(ctx, resource, t, cb)

		return
	}

	rec, err := s.TryAcquire(ctx, resource)
	if err != nil {
		if !errors.Is(err, ErrLeaseHeldByOther) && !errors.Is(err, ErrLockAlreadyAcquired) {
			log.Warn("acquiring lease failed",
				append(s.fields(resource), logger.StringField("error", err.Error()))...)
		}

		return
	}

	leaderCtx := s.beginTerm(ctx, t, rec.ExpiresAt, cb)
	if cb.OnElected != nil {
		go cb.OnElected(leaderCtx)
	}
}

// renewTerm performs one renewal of a lease this loop holds.
//
// The call is bounded by the lease's own remaining lifetime. A renewal still in
// flight when the lease expires cannot save the term, and letting it block the
// loop indefinitely would leave OnElected running with nobody watching the
// clock.
func (s *Service) renewTerm(ctx context.Context, resource string, t *term, cb Callbacks) {
	remaining := t.remaining(s.now())
	if remaining <= 0 {
		s.resign(t, cb)

		return
	}

	renewCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()

	rec, err := s.Renew(renewCtx, resource)
	switch {
	case err == nil:
		if !t.leading.Load() {
			// The expiry timer ended the term while this renewal was in flight.
			// The row is extended but we no longer lead; the next loop iteration
			// reacquires it, since the holder is still this identity.
			return
		}
		t.extend(rec.ExpiresAt, s.now(), s.expireTerm(t, cb))
	case errors.Is(err, ErrLeaseLost), !s.now().Before(t.deadline()):
		log.Error(fmt.Errorf("renewing lease, resigning: %w", err), s.fields(resource)...)
		s.resign(t, cb)
	default:
		log.Warn("lease renewal failed, still leading until expiry",
			append(s.fields(resource), logger.StringField("error", err.Error()))...)
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
// the lease, when the lease it holds runs out, the cancel func of the context
// handed to OnElected, and the timer that enforces the expiry.
//
// leading is atomic because IsLeader reads it from other goroutines. The
// remaining fields are guarded by mu, because the expiry timer fires on its own
// goroutine and races the owning Run goroutine.
type term struct {
	leading atomic.Bool

	mu     sync.Mutex
	stop   context.CancelFunc
	expiry time.Time
	timer  *time.Timer
}

// beginTerm marks the start of a leadership term, derives its leader context and
// arms the expiry timer.
func (s *Service) beginTerm(ctx context.Context, t *term, expiry time.Time, cb Callbacks) context.Context {
	leaderCtx, stop := context.WithCancel(ctx)

	t.mu.Lock()
	t.stop = stop
	t.expiry = expiry
	t.timer = time.AfterFunc(expiry.Sub(s.now()), s.expireTerm(t, cb))
	t.mu.Unlock()

	t.leading.Store(true)

	return leaderCtx
}

// expireTerm builds the expiry-timer callback. It ends the term the moment the
// lease runs out, without waiting for the Run loop to come back round: a
// renewal that hangs on a dead connection must not leave OnElected running past
// the point where another process may take the lease over.
//
// A renewal that landed while the timer was already queued has pushed the
// expiry out, so the callback re-arms instead of resigning.
func (s *Service) expireTerm(t *term, cb Callbacks) func() {
	return func() {
		if t.rearm(s.now()) {
			return
		}

		log.Warn("lease expired without a successful renewal, resigning",
			logger.StringField("identity", s.identity))
		s.resign(t, cb)
	}
}

// deadline reports when the held lease runs out.
func (t *term) deadline() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.expiry
}

// remaining reports how much of the held lease is left as of now.
func (t *term) remaining(now time.Time) time.Duration {
	return t.deadline().Sub(now)
}

// extend records a renewed expiry and pushes the expiry timer out to match.
func (t *term) extend(expiry, now time.Time, onExpire func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.expiry = expiry
	if t.timer != nil {
		t.timer.Stop()
	}
	t.timer = time.AfterFunc(expiry.Sub(now), onExpire)
}

// rearm reports whether the term outlived a fired expiry timer, re-arming the
// timer for the new expiry when it did.
func (t *term) rearm(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	left := t.expiry.Sub(now)
	if left <= 0 {
		return false
	}
	if t.timer != nil {
		t.timer.Reset(left)
	}

	return true
}

// resign ends a leadership term exactly once: it disarms the expiry timer,
// cancels the leader context and reports the loss. It is called both from the
// Run goroutine and from the expiry timer, so the atomic swap is what makes it
// exactly-once.
func (s *Service) resign(t *term, cb Callbacks) {
	if !t.leading.Swap(false) {
		return
	}

	t.mu.Lock()
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	stop := t.stop
	t.stop = nil
	t.mu.Unlock()

	if stop != nil {
		stop()
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

	if err := lock.Release(unlockCtx); err != nil {
		log.Error(fmt.Errorf("releasing advisory lock: %w", err), logger.Int64Field("lock_key", lock.Key()))
	}
}

// releaseOnShutdown drops the lease on a fresh context, because Run only returns
// once the caller's context is already canceled.
func (s *Service) releaseOnShutdown(resource string) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()

	if err := s.Release(ctx, resource); err != nil {
		log.Error(fmt.Errorf("releasing lease on shutdown: %w", err),
			logger.StringField("resource", resource), logger.StringField("identity", s.identity))
	}
}

// fields are the log fields identifying a campaign.
func (s *Service) fields(resource string) []logger.Field {
	return []logger.Field{
		logger.StringField("resource", resource),
		logger.StringField("identity", s.identity),
	}
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
