// Package rewards turns payment events into points ledger entries.
package rewards

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
)

// ErrUnknownPayment is returned for a reversal of a payment that was never awarded.
var ErrUnknownPayment = errors.New("rewards: reversal for unknown payment")

type Handler struct {
	store      ledger.Store
	policy     Policy
	deadLetter pubsub.Publisher
	log        *slog.Logger
}

func NewHandler(store ledger.Store, policy Policy, deadLetter pubsub.Publisher, log *slog.Logger) *Handler {
	return &Handler{store: store, policy: policy, deadLetter: deadLetter, log: log}
}

// Handle processes one delivery. The subscriber calls it concurrently (MaxOutstandingMessages per replica), and pointsd runs several replicas.
//
// Exactly one of Ack or Nack is called on every path. Ack means "done, never
// redeliver": it is only said once the ledger confirmed the write, or when a
// retry could never change the outcome. Nack asks the broker to redeliver
// after a backoff; the ledger operations are idempotent, so a retry after a
// write that did succeed is a no-op.
func (h *Handler) Handle(ctx context.Context, msg pubsub.Message) {
	ev, err := events.Decode(msg.Data())
	if err != nil {
		h.log.Warn("dead-lettering invalid event", "msg_id", msg.ID(), "err", err)
		if err := h.deadLetter.Publish(ctx, msg.Data(), map[string]string{"reason": err.Error()}); err != nil {
			h.log.Error("dead-letter publish failed", "msg_id", msg.ID(), "err", err)
			msg.Nack()
			return
		}
		msg.Ack()
		return
	}

	switch err := h.process(ctx, ev); {
	case err == nil:
		msg.Ack()
	case errors.Is(err, ErrUnknownPayment):
		h.log.Warn("reversal for unknown payment; acked for finance reconciliation",
			"payment_id", ev.PaymentID, "member_id", ev.MemberID, "amount_cents", ev.AmountCents)
		msg.Ack()
	default:
		h.log.Error("processing failed; nacking for redelivery",
			"payment_id", ev.PaymentID, "type", ev.Type, "attempt", msg.DeliveryAttempt(), "err", err)
		msg.Nack()
	}
}

func (h *Handler) process(ctx context.Context, ev events.Payment) error {
	switch ev.Type {
	case events.TypeSettled:
		return h.award(ctx, ev)
	case events.TypeReversed:
		return h.reverse(ctx, ev)
	}
	return nil
}

// award hands the ledger the uncapped points; the ledger applies the monthly cap and the once-per-payment rule in a single step.
func (h *Handler) award(ctx context.Context, ev events.Payment) error {
	month := Month(ev.OccurredAt)
	_, err := h.store.Award(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindAward,
		Points:    h.policy.Points(ev.AmountCents),
		Month:     month,
	}, h.policy.MonthlyCap)
	return err
}

// reverse lets the ledger remove exactly the points the award entry holds.
func (h *Handler) reverse(ctx context.Context, ev events.Payment) error {
	_, err := h.store.Reverse(ctx, ev.PaymentID)
	if errors.Is(err, ledger.ErrNotAwarded) {
		return fmt.Errorf("%w: %s", ErrUnknownPayment, ev.PaymentID)
	}
	return err
}
