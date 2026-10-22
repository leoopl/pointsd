package rewards_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

func TestHandle_LedgerUnavailableIsNackedNotAcked(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithFaults(func(string) error { return ledger.ErrUnavailable }))
	msg := newMessage(t, settled("pay_1", "mbr_1", 1200))

	newHandler(store).Handle(context.Background(), msg)

	if msg.outcome(t) != nacked {
		t.Fatal("ledger unavailable was acked; the payment would be lost")
	}
}

func TestHandle_CancelledContextIsNacked(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := newMessage(t, settled("pay_1", "mbr_1", 1200))

	newHandler(store).Handle(ctx, msg)

	if msg.outcome(t) != nacked {
		t.Fatal("cancelled context was acked; the payment would be lost")
	}
	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestHandle_TransientErrorOnReversalIsRetried(t *testing.T) {
	var failures atomic.Int32
	failures.Store(2)
	store := ledger.NewMemoryStore(ledger.WithFaults(func(op string) error {
		if op == "reverse" && failures.Add(-1) >= 0 {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 2000))
	deliver(t, h, reversed("pay_1", "mbr_1", 2000))

	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0 (reversal lost on a transient ledger error)", got)
	}
}

func TestReversal_ConcurrentRedeliveryAcrossReplicasRemovesPointsOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store)}
	deliver(t, replicas[0], settled("pay_1", "mbr_1", 2000))
	deliver(t, replicas[0], settled("pay_2", "mbr_1", 1500))
	ev := reversed("pay_2", "mbr_1", 1500)

	var fns []func()
	for i := 0; i < 8; i++ {
		h := replicas[i%2]
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000 (reversal applied more than once)", got)
	}
}

func TestReversal_OfZeroPointAwardLeavesBalanceUnchanged(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)
	deliver(t, h, settled("pay_1", "mbr_1", 10_000))
	deliver(t, h, settled("pay_2", "mbr_1", 500)) // capped: 0 awarded
	deliver(t, h, reversed("pay_2", "mbr_1", 500))

	if got := balance(t, store, "mbr_1"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000 (reversing a zero-point award must remove nothing)", got)
	}
}

func TestHandle_ReversalForUnknownPaymentIsLoggedForFinance(t *testing.T) {
	var logs bytes.Buffer
	h := rewards.NewHandler(ledger.NewMemoryStore(), rewards.DefaultPolicy, &fakeDLQ{}, slog.New(slog.NewTextHandler(&logs, nil)))
	msg := newMessage(t, reversed("pay_never", "mbr_7", 1000))

	h.Handle(context.Background(), msg)

	if msg.outcome(t) != acked {
		t.Fatal("reversal of an unknown payment must be acked")
	}
	for _, want := range []string{"pay_never", "mbr_7"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log does not mention %q; finance cannot reconcile from it:\n%s", want, logs.String())
		}
	}
}
