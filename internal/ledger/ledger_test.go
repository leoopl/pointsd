package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
)

const monthlyCap int64 = 10_000

var ctx = context.Background()

func award(paymentID, memberID string, points int64) ledger.Entry {
	return ledger.Entry{MemberID: memberID, PaymentID: paymentID, Kind: ledger.KindAward, Points: points, Month: "2026-10"}
}

// concurrently runs every fn at the same time and waits for all of them.
func concurrently(fns ...func()) {
	var start, done sync.WaitGroup
	start.Add(1)
	for _, fn := range fns {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			fn()
		}()
	}
	start.Done()
	done.Wait()
}

func mustAward(t *testing.T, s *ledger.MemoryStore, e ledger.Entry) ledger.Entry {
	t.Helper()
	got, err := s.Award(ctx, e, monthlyCap)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func mustBalance(t *testing.T, s *ledger.MemoryStore, member string) int64 {
	t.Helper()
	b, err := s.Balance(ctx, member)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAward_ConcurrentCallsForOnePaymentWriteOnce(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(2 * time.Millisecond))
	var mu sync.Mutex
	var got []ledger.Entry
	var fns []func()
	for i := 0; i < 20; i++ {
		fns = append(fns, func() {
			e, err := s.Award(ctx, award("pay_1", "mbr_1", 2000), monthlyCap)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		})
	}
	concurrently(fns...)

	if b := mustBalance(t, s, "mbr_1"); b != 2000 {
		t.Fatalf("balance = %d, want 2000 (payment written more than once)", b)
	}
	for _, e := range got {
		if e != got[0] {
			t.Fatalf("Award returned different entries for one payment: %+v vs %+v", e, got[0])
		}
	}
}

func TestAward_ConcurrentCallsRespectMonthlyCap(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(2 * time.Millisecond))
	var fns []func()
	for i := 0; i < 15; i++ {
		id := fmt.Sprintf("pay_%d", i)
		fns = append(fns, func() {
			if _, err := s.Award(ctx, award(id, "mbr_1", 1000), monthlyCap); err != nil {
				t.Error(err)
			}
		})
	}
	concurrently(fns...)

	if b := mustBalance(t, s, "mbr_1"); b != monthlyCap {
		t.Fatalf("balance = %d, want %d (monthly cap)", b, monthlyCap)
	}
}

func TestAward_CapCutIsRecordedOnTheReturnedEntry(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 9500))
	if e := mustAward(t, s, award("pay_2", "mbr_1", 1000)); e.Points != 500 {
		t.Fatalf("points = %d, want 500 (the cap cut must be on the written entry)", e.Points)
	}
}

func TestAward_PointsEqualToRemainingRoomAreAwardedInFull(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 9000))
	if e := mustAward(t, s, award("pay_2", "mbr_1", 1000)); e.Points != 1000 {
		t.Fatalf("points = %d, want 1000 (exactly the remaining room must not be cut)", e.Points)
	}
}

func TestAward_ZeroPointAwardIsWrittenAndReturnedOnRedelivery(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 10_000))
	first := mustAward(t, s, award("pay_2", "mbr_1", 700))
	if first.Points != 0 {
		t.Fatalf("points = %d, want 0", first.Points)
	}
	if again := mustAward(t, s, award("pay_2", "mbr_1", 700)); again != first {
		t.Fatalf("redelivery returned %+v, want the existing entry %+v", again, first)
	}
	if b := mustBalance(t, s, "mbr_1"); b != 10_000 {
		t.Fatalf("balance = %d, want 10000", b)
	}
}

func TestAward_ReversalDoesNotGiveCapRoomBack(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 10_000))
	if _, err := s.Reverse(ctx, "pay_1"); err != nil {
		t.Fatal(err)
	}
	if e := mustAward(t, s, award("pay_2", "mbr_1", 1000)); e.Points != 0 {
		t.Fatalf("points = %d, want 0 (a reversal must not give cap room back)", e.Points)
	}
}

func TestAward_RepublishedPaymentWithDifferentAmountKeepsFirstWrite(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 1800))
	if e := mustAward(t, s, award("pay_1", "mbr_1", 9999)); e.Points != 1800 {
		t.Fatalf("points = %d, want 1800 (first write wins)", e.Points)
	}
	if b := mustBalance(t, s, "mbr_1"); b != 1800 {
		t.Fatalf("balance = %d, want 1800", b)
	}
}

func TestAward_ForcesKindAward(t *testing.T) {
	s := ledger.NewMemoryStore()
	e := award("pay_1", "mbr_1", 100)
	e.Kind = ledger.KindReversal
	if got := mustAward(t, s, e); got.Kind != ledger.KindAward {
		t.Fatalf("kind = %q, want %q", got.Kind, ledger.KindAward)
	}
	if _, err := s.Reverse(ctx, "pay_1"); err != nil {
		t.Fatalf("Reverse could not find the award: %v", err)
	}
}

func TestAward_FaultLeavesNoEffect(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithFaults(func(op string) error {
		if op == "award" {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	if _, err := s.Award(ctx, award("pay_1", "mbr_1", 100), monthlyCap); !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if b := mustBalance(t, s, "mbr_1"); b != 0 {
		t.Fatalf("balance = %d, want 0 (a failed call must write nothing)", b)
	}
}

func TestReverse_RemovesExactlyTheAwardedPoints(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 9500))
	mustAward(t, s, award("pay_2", "mbr_1", 1000)) // capped: 500 awarded
	e, err := s.Reverse(ctx, "pay_2")
	if err != nil {
		t.Fatal(err)
	}
	if e.Points != -500 {
		t.Fatalf("points = %d, want -500 (reverse what was awarded, not the payment amount)", e.Points)
	}
	if b := mustBalance(t, s, "mbr_1"); b != 9500 {
		t.Fatalf("balance = %d, want 9500", b)
	}
}

func TestReverse_ConcurrentCallsWriteOnce(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(2 * time.Millisecond))
	mustAward(t, s, award("pay_1", "mbr_1", 2000))
	mustAward(t, s, award("pay_2", "mbr_1", 1500))
	var fns []func()
	for i := 0; i < 8; i++ {
		fns = append(fns, func() {
			if _, err := s.Reverse(ctx, "pay_2"); err != nil {
				t.Error(err)
			}
		})
	}
	concurrently(fns...)

	if b := mustBalance(t, s, "mbr_1"); b != 2000 {
		t.Fatalf("balance = %d, want 2000 (reversal written more than once)", b)
	}
}

func TestReverse_UnknownPaymentReturnsErrNotAwarded(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, err := s.Reverse(ctx, "pay_never"); !errors.Is(err, ledger.ErrNotAwarded) {
		t.Fatalf("err = %v, want ErrNotAwarded", err)
	}
}

func TestReverse_CopiesMemberAndMonthFromAward(t *testing.T) {
	s := ledger.NewMemoryStore()
	e := award("pay_1", "mbr_1", 300)
	e.Month = "2026-09"
	mustAward(t, s, e)
	rev, err := s.Reverse(ctx, "pay_1")
	if err != nil {
		t.Fatal(err)
	}
	want := ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_1", Kind: ledger.KindReversal, Points: -300, Month: "2026-09"}
	if rev != want {
		t.Fatalf("reversal = %+v, want %+v", rev, want)
	}
}

func TestAppend_RejectsDuplicatePaymentAndKind(t *testing.T) {
	s := ledger.NewMemoryStore()
	e := award("pay_1", "mbr_1", 100)
	if err := s.Append(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, e); !errors.Is(err, ledger.ErrDuplicate) {
		t.Fatalf("second Append err = %v, want ErrDuplicate", err)
	}
	rev := e
	rev.Kind, rev.Points = ledger.KindReversal, -100
	if err := s.Append(ctx, rev); err != nil {
		t.Fatalf("same payment with a different kind must be accepted: %v", err)
	}
	if b := mustBalance(t, s, "mbr_1"); b != 0 {
		t.Fatalf("balance = %d, want 0", b)
	}
}
