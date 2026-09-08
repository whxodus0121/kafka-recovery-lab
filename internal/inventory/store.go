package inventory

import (
	"context"
	"database/sql"
	"fmt"

	"kafka-recovery-lab/internal/event"
)

type Store struct {
	DB    *sql.DB
	Hooks Hooks
}

func (s Store) Decrement(ctx context.Context, e event.OrderCreated) error {
	if err := e.Validate(); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE inventory
		SET available_quantity = available_quantity - ?, updated_at = UTC_TIMESTAMP(6)
		WHERE product_id = ? AND available_quantity >= ?`, e.Quantity, e.ProductID, e.Quantity)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		var id int64
		err := tx.QueryRowContext(ctx, "SELECT product_id FROM inventory WHERE product_id=?", e.ProductID).Scan(&id)
		if err == sql.ErrNoRows {
			return ErrProductMissing
		}
		if err != nil {
			return err
		}
		return ErrInsufficientInventory
	}
	if rows != 1 {
		return fmt.Errorf("unexpected affected row count: %d", rows)
	}
	// A successful return means the DB transaction committed, not just UPDATE.
	if s.Hooks.BeforeCommit != nil {
		if err := s.Hooks.BeforeCommit(ctx, e); err != nil {
			return err
		}
	}
	return tx.Commit()
}
