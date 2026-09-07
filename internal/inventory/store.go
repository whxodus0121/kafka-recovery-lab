package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kafka-recovery-lab/internal/event"
)

type Store struct{ DB *sql.DB }

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
		return errors.New("product missing or insufficient inventory")
	}
	if rows != 1 {
		return fmt.Errorf("unexpected affected row count: %d", rows)
	}
	// A successful return means the DB transaction committed, not just UPDATE.
	return tx.Commit()
}
