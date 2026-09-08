// Package fault contains only the three explicit local Phase 2 boundaries.
package fault

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/inventory"
)

func Configure(point, eventID string, release <-chan struct{}, logger *slog.Logger) (inventory.Hooks, error) {
	var hooks inventory.Hooks
	if point == "" && eventID == "" {
		return hooks, nil
	}
	if eventID == "" {
		return hooks, fmt.Errorf("fault-event-id is required with fault-point")
	}
	hit := func(ctx context.Context, e event.OrderCreated) error {
		if e.EventID != eventID {
			return nil
		}
		logger.Info("fault_reached", "point", point, "eventId", e.EventID, "orderId", e.OrderID, "productId", e.ProductID)
		switch point {
		case "before-db":
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case "before-db-commit":
			// Deliberately bypass Go defers: MySQL must roll back the open tx.
			os.Exit(86)
		case "after-db-commit":
			// tx.Commit has already returned nil; CommitMessages is not reached.
			os.Exit(87)
		}
		return nil
	}
	switch point {
	case "before-db":
		if release == nil {
			return hooks, fmt.Errorf("before-db requires a local stdin release barrier")
		}
		hooks.BeforeDB = hit
	case "before-db-commit":
		hooks.BeforeCommit = hit
	case "after-db-commit":
		hooks.AfterCommit = hit
	default:
		return hooks, fmt.Errorf("unknown fault-point")
	}
	return hooks, nil
}
