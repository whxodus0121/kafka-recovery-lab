package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"

	mysql "github.com/go-sql-driver/mysql"
	"kafka-recovery-lab/internal/event"
)

type Result struct{ Duplicate bool }

// Hash the validated, typed v1 event (all six immutable contract fields), not
// raw JSON formatting or Kafka retry headers. UTC normalizes Z versus +00:00.
func PayloadHash(e event.OrderCreated) ([32]byte, error) {
	if err := e.Validate(); err != nil {
		return [32]byte{}, err
	}
	e.CreatedAt = e.CreatedAt.UTC()
	b, err := json.Marshal(e)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

func registerEvent(ctx context.Context, tx *sql.Tx, e event.OrderCreated) (bool, error) {
	hash, err := PayloadHash(e)
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO processed_events
		(event_id,payload_hash,order_id,product_id,quantity,processed_at)
		VALUES (?,?,?,?,?,UTC_TIMESTAMP(6))`, e.EventID, hash[:], e.OrderID, e.ProductID, e.Quantity)
	if err == nil {
		return false, nil
	}
	var server *mysql.MySQLError
	if !errors.As(err, &server) || server.Number != 1062 {
		return false, err
	}
	// A duplicate INSERT waits for the competing transaction. A locking/current
	// read then sees its committed row even under InnoDB REPEATABLE READ.
	var storedHash []byte
	var orderID string
	var productID, quantity int64
	if err := tx.QueryRowContext(ctx, `SELECT payload_hash,order_id,product_id,quantity
		FROM processed_events WHERE event_id=? FOR SHARE`, e.EventID).Scan(&storedHash, &orderID, &productID, &quantity); err != nil {
		return false, err
	}
	if !bytes.Equal(storedHash, hash[:]) || orderID != e.OrderID || productID != e.ProductID || quantity != e.Quantity {
		return false, ErrEventIDConflict
	}
	return true, nil
}
