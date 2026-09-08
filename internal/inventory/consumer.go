package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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

func Consume(ctx context.Context, reader Reader, apply func(context.Context, event.OrderCreated) error, logger *slog.Logger, hooks Hooks, policies ...*FailurePolicy) error {
	var policy *FailurePolicy
	if len(policies) > 0 {
		policy = policies[0]
	}
	if policy != nil {
		if err := policy.Validate(); err != nil {
			return err
		}
	}
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
		log := logger.With("topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
		md := retryMetadata{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset}
		validMetadata := true
		code := ""
		retryable := false
		if policy != nil {
			md, err = policy.metadata(m)
			if err != nil {
				validMetadata = false
				code = "INVALID_RETRY_METADATA"
			} else if policy.RetrySource {
				log.Info("retry_wait", "retryCount", md.Count, "nextAttemptAt", md.Next)
				if err = waitAttempt(ctx, md.Next); err != nil {
					return err
				}
			}
		}
		var e event.OrderCreated
		if err == nil {
			e, err = event.Decode(m.Value)
			if err != nil {
				code = "EVENT_CONTRACT"
				if !json.Valid(m.Value) {
					code = "MALFORMED_JSON"
				} else if e.SchemaVersion != 1 {
					code = "UNSUPPORTED_SCHEMA"
				}
			} else if string(m.Key) != e.OrderID {
				code = "KEY_MISMATCH"
				err = fmt.Errorf("event key mismatch")
			}
		}
		if err == nil {
			log = log.With("eventId", e.EventID, "orderId", e.OrderID, "productId", e.ProductID, "quantity", e.Quantity)
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
				code, retryable = classifyDB(err)
			} else {
				log.Info("inventory_committed")
				if hooks.AfterCommit != nil {
					if err := hooks.AfterCommit(ctx, e); err != nil {
						return err
					}
				}
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if policy == nil || code == "" {
				return fmt.Errorf("processing at %s/%d/%d: %w", m.Topic, m.Partition, m.Offset, err)
			}
			destination, publishErr := policy.route(ctx, m, md, code, retryable, validMetadata)
			if publishErr != nil {
				return publishErr
			}
			log.Info("failure_published", "destination", destination, "errorCode", code, "retryCount", md.Count)
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
