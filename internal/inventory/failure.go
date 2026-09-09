package inventory

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"

	mysql "github.com/go-sql-driver/mysql"
)

var (
	ErrProductMissing        = errors.New("product missing")
	ErrInsufficientInventory = errors.New("insufficient inventory")
	ErrEventIDConflict       = errors.New("event ID payload conflict")
)

// Only recognized transient DB errors may leave the source for retry.
// Unrecognized SQL, authentication and programming errors stop the worker.
func classifyDB(err error) (code string, retryable bool) {
	switch {
	case errors.Is(err, ErrEventIDConflict):
		return "EVENT_ID_CONFLICT", false
	case errors.Is(err, ErrProductMissing):
		return "PRODUCT_MISSING", false
	case errors.Is(err, ErrInsufficientInventory):
		return "INSUFFICIENT_INVENTORY", false
	case errors.Is(err, context.Canceled):
		return "", false
	case errors.Is(err, context.DeadlineExceeded):
		return "DB_TIMEOUT", true
	case errors.Is(err, driver.ErrBadConn), errors.Is(err, mysql.ErrInvalidConn), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "DB_CONNECTION", true
	}
	var server *mysql.MySQLError
	if errors.As(err, &server) {
		switch server.Number {
		case 1213:
			return "DB_DEADLOCK", true
		case 1205:
			return "DB_LOCK_TIMEOUT", true
		case 2006, 2013:
			return "DB_CONNECTION", true
		}
		return "", false
	}
	var network *net.OpError
	if errors.As(err, &network) {
		var dns *net.DNSError
		if errors.As(err, &dns) && !dns.IsTimeout && !dns.IsTemporary {
			return "", false
		}
		var address *net.AddrError
		if errors.As(err, &address) {
			return "", false
		}
		if network.Timeout() {
			return "DB_TIMEOUT", true
		}
		return "DB_CONNECTION", true
	}
	return "", false
}
