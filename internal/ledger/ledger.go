// Package ledger stores points entries and enforces the earning rules on them.
//
// The Store offers two all-or-nothing operations: Award (once per payment,
// capped per member per month) and Reverse (once per payment, removing exactly
// the points the award holds). Each does its check, decision and write as a
// single step, so several replicas sharing one ledger cannot act on a stale
// answer. Balance is the read side.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrNotAwarded is returned by Reverse when the payment has no award entry.
// Retrying cannot help; the caller decides what to do (pointsd acks and logs).
var ErrNotAwarded = errors.New("ledger: payment was never awarded")

// ErrDuplicate is returned by Append when an entry with the same (PaymentID, Kind) already exists. It mirrors the UNIQUE (payment_id, kind) constraint in Postgres.
var ErrDuplicate = errors.New("ledger: duplicate entry")

type Store interface {
	// Award records the points for a settled payment exactly once.
	Award(ctx context.Context, e Entry, monthlyCap int64) (Entry, error)

	// Reverse records the reversal of a payment's award exactly once.
	Reverse(ctx context.Context, paymentID string) (Entry, error)

	// Balance is the sum of the points on the member's entries.
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

// findLocked returns the first entry for paymentID of the given kind. s.mu must be held.
func (s *MemoryStore) findLocked(paymentID string, kind Kind) (Entry, bool) {
	for _, e := range s.entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return e, true
		}
	}
	return Entry{}, false
}

// awardedInMonthLocked sums the award points of memberID in month. Reversals are not subtracted: they do not give cap room back. s.mu must be held.
func (s *MemoryStore) awardedInMonthLocked(memberID, month string) int64 {
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID && e.Month == month && e.Kind == KindAward {
			total += e.Points
		}
	}
	return total
}

// Award implements Store. The round trip (latency, faults) happens before the lock is taken, so waiting never happens while the lock is held.
func (s *MemoryStore) Award(ctx context.Context, e Entry, monthlyCap int64) (Entry, error) {
	if err := s.roundTrip(ctx, "award"); err != nil {
		return Entry{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.findLocked(e.PaymentID, KindAward); ok {
		return existing, nil
	}
	if room := monthlyCap - s.awardedInMonthLocked(e.MemberID, e.Month); e.Points > room {
		e.Points = max(room, 0)
	}
	e.Kind = KindAward
	s.entries = append(s.entries, e)
	return e, nil
}

// Reverse implements Store.
func (s *MemoryStore) Reverse(ctx context.Context, paymentID string) (Entry, error) {
	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return Entry{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	award, ok := s.findLocked(paymentID, KindAward)
	if !ok {
		return Entry{}, ErrNotAwarded
	}
	if existing, ok := s.findLocked(paymentID, KindReversal); ok {
		return existing, nil
	}
	rev := Entry{
		MemberID:  award.MemberID,
		PaymentID: paymentID,
		Kind:      KindReversal,
		Points:    -award.Points,
		Month:     award.Month,
	}
	s.entries = append(s.entries, rev)
	return rev, nil
}

// Append is a raw insert. It rejects a second entry with the same (PaymentID, Kind) with ErrDuplicate, like the Postgres unique constraint.
func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.findLocked(e.PaymentID, e.Kind); ok {
		return ErrDuplicate
	}
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
