package inventory

import (
	"context"
	"database/sql"
	"fmt"

	"kafka-recovery-lab/internal/event"
)

type Store struct {
	DB             *sql.DB
	Hooks          Hooks
	LegacyBaseline bool // Explicit Phase 2 regression only; never the default.
}

func (s Store) Decrement(ctx context.Context, e event.OrderCreated) (Result, error) {
	if err := e.Validate(); err != nil {
		return Result{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	if !s.LegacyBaseline {
		duplicate, err := registerEvent(ctx, tx, e)
		if err != nil {
			return Result{}, err
		}
		if duplicate {
			// No UPDATE occurred. Release the read/duplicate-key locks safely.
			if err := tx.Rollback(); err != nil {
				return Result{}, err
			}
			return Result{Duplicate: true}, nil
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE inventory
		SET available_quantity = available_quantity - ?, updated_at = UTC_TIMESTAMP(6)
		WHERE product_id = ? AND available_quantity >= ?`, e.Quantity, e.ProductID, e.Quantity)
	if err != nil {
		return Result{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Result{}, err
	}
	if rows == 0 {
		var id int64
		err := tx.QueryRowContext(ctx, "SELECT product_id FROM inventory WHERE product_id=?", e.ProductID).Scan(&id)
		if err == sql.ErrNoRows {
			return Result{}, ErrProductMissing
		}
		if err != nil {
			return Result{}, err
		}
		return Result{}, ErrInsufficientInventory
	}
	if rows != 1 {
		return Result{}, fmt.Errorf("unexpected affected row count: %d", rows)
	}
	// A successful return means the DB transaction committed, not just UPDATE.
	if s.Hooks.BeforeCommit != nil {
		if err := s.Hooks.BeforeCommit(ctx, e); err != nil {
			return Result{}, err
		}
	}
	return Result{}, tx.Commit()
}
