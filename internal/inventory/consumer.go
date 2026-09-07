package inventory

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
)

// This narrow boundary allows tests to prove that failed processing never calls
// CommitMessages or fetches a later message. Reader lifetime belongs to main.
type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

func Consume(ctx context.Context, reader Reader, apply func(context.Context, event.OrderCreated) error, logger *slog.Logger) error {
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		e, err := event.Decode(m.Value)
		if err != nil {
			return fmt.Errorf("invalid event at %s/%d/%d: %w", m.Topic, m.Partition, m.Offset, err)
		}
		if string(m.Key) != e.OrderID {
			return fmt.Errorf("event key mismatch at %s/%d/%d", m.Topic, m.Partition, m.Offset)
		}
		log := logger.With("eventId", e.EventID, "orderId", e.OrderID, "productId", e.ProductID,
			"quantity", e.Quantity, "topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
		log.Info("event_received")
		dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = apply(dbCtx, e)
		cancel()
		if err != nil {
			log.Error("inventory_failed", "error", err)
			return fmt.Errorf("inventory processing: %w", err)
		}
		log.Info("inventory_committed")
		commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = reader.CommitMessages(commitCtx, m)
		cancel()
		if err != nil {
			log.Error("offset_commit_failed", "error", err)
			return fmt.Errorf("offset commit: %w", err)
		}
		log.Info("offset_committed", "committedOffset", m.Offset+1)
	}
}
