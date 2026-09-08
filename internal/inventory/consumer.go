package inventory

import (
	"context"
	"crypto/sha256"
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

// Nil hooks preserve the normal path. Only the local fault harness supplies them.
type Hooks struct {
	BeforeDB, BeforeCommit, AfterCommit func(context.Context, event.OrderCreated) error
}

func Consume(ctx context.Context, reader Reader, apply func(context.Context, event.OrderCreated) error, logger *slog.Logger, hooks Hooks) error {
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		logger.Info("record_fetched", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset,
			"key", string(m.Key), "valueSHA256", fmt.Sprintf("%x", sha256.Sum256(m.Value)))
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
		if hooks.BeforeDB != nil {
			if err := hooks.BeforeDB(ctx, e); err != nil {
				return err
			}
		}
		dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = apply(dbCtx, e)
		cancel()
		if err != nil {
			log.Error("inventory_failed", "error", err)
			return fmt.Errorf("inventory processing: %w", err)
		}
		log.Info("inventory_committed")
		if hooks.AfterCommit != nil {
			if err := hooks.AfterCommit(ctx, e); err != nil {
				return err
			}
		}
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
