package event

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

const OrdersTopic = "orders.created.v1"

type OrderCreated struct {
	SchemaVersion int       `json:"schemaVersion"`
	EventID       string    `json:"eventId"`
	OrderID       string    `json:"orderId"`
	ProductID     int64     `json:"productId"`
	Quantity      int64     `json:"quantity"`
	CreatedAt     time.Time `json:"createdAt"`
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func New(productID, quantity int64) (OrderCreated, error) {
	e := OrderCreated{SchemaVersion: 1, ProductID: productID, Quantity: quantity, CreatedAt: time.Now().UTC()}
	var err error
	if e.EventID, err = newID(); err != nil {
		return e, err
	}
	if e.OrderID, err = newID(); err != nil {
		return e, err
	}
	return e, e.Validate()
}

func (e OrderCreated) Validate() error {
	_, offset := e.CreatedAt.Zone()
	if e.SchemaVersion != 1 || !uuidV4.MatchString(e.EventID) || !uuidV4.MatchString(e.OrderID) ||
		e.ProductID <= 0 || e.Quantity <= 0 || e.CreatedAt.IsZero() || offset != 0 {
		return errors.New("invalid OrderCreated: schemaVersion=1, UUID v4 IDs, positive productId/quantity and UTC createdAt required")
	}
	return nil
}

func Decode(value []byte) (OrderCreated, error) {
	var e OrderCreated
	d := json.NewDecoder(bytes.NewReader(value))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return e, errors.New("expected exactly one JSON event")
	}
	return e, e.Validate()
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6], b[8] = (b[6]&0x0f)|0x40, (b[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
