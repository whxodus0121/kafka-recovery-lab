package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventContract(t *testing.T) {
	e, err := New(1, 3)
	if err != nil || e.EventID == e.OrderID {
		t.Fatalf("event generation: %v", err)
	}
	value, _ := json.Marshal(e)
	if _, err := Decode(value); err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{[]byte(`{}`), append(append([]byte{}, value...), []byte(` {}`)...)} {
		if _, err := Decode(value); err == nil {
			t.Fatal("invalid event accepted")
		}
	}
	for name, change := range map[string]func(*OrderCreated){
		"schema":   func(e *OrderCreated) { e.SchemaVersion = 2 },
		"id":       func(e *OrderCreated) { e.EventID = "" },
		"quantity": func(e *OrderCreated) { e.Quantity = 0 },
		"product":  func(e *OrderCreated) { e.ProductID = -1 },
		"time":     func(e *OrderCreated) { e.CreatedAt = e.CreatedAt.In(time.FixedZone("KST", 9*3600)) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := e
			change(&bad)
			if bad.Validate() == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
}
