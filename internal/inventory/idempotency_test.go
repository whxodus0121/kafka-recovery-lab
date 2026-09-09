package inventory

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
)

func TestCanonicalPayloadHash(t *testing.T) {
	e, _ := event.New(1, 2)
	e.CreatedAt = time.Date(2026, 9, 9, 0, 0, 0, 123000000, time.UTC)
	h, err := PayloadHash(e)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.MarshalIndent(e, "", "  ")
	var decoded event.OrderCreated
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.CreatedAt = decoded.CreatedAt.In(time.FixedZone("zero", 0))
	if other, err := PayloadHash(decoded); err != nil || h != other {
		t.Fatal("format or zero-offset zone changed canonical hash")
	}
	for _, mutate := range []func(*event.OrderCreated){func(e *event.OrderCreated) { e.Quantity++ }, func(e *event.OrderCreated) { e.ProductID++ }, func(e *event.OrderCreated) { e.CreatedAt = e.CreatedAt.Add(time.Nanosecond) }, func(e *event.OrderCreated) { e.OrderID = "a7f7d32e-96f1-4ee5-b0e6-7ec02d7c5b7c" }} {
		other := e
		mutate(&other)
		got, err := PayloadHash(other)
		if err != nil || got == h {
			t.Fatal("semantic change not detected")
		}
	}
}

func TestDuplicateCommitsWithoutRouting(t *testing.T) {
	e, _ := event.New(1, 2)
	b, _ := json.Marshal(e)
	trace := []string{}
	r := &readerStub{message: kafka.Message{Topic: "main", Key: []byte(e.OrderID), Value: b}, trace: &trace}
	p := &FailurePolicy{RetryTopic: "retry", DLQTopic: "dlq", MaxRetries: 3, Delay: time.Second, Publish: func(context.Context, kafka.Message) error { t.Fatal("duplicate routed"); return nil }}
	_ = Consume(context.Background(), r, func(context.Context, event.OrderCreated) (Result, error) {
		trace = append(trace, "db")
		return Result{Duplicate: true}, nil
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Hooks{AfterCommit: func(context.Context, event.OrderCreated) error {
		t.Fatal("duplicate treated as new DB commit")
		return nil
	}}, p)
	if !reflect.DeepEqual(trace, []string{"fetch", "db", "commit", "fetch_next"}) {
		t.Fatal(trace)
	}
	if code, retry := classifyDB(ErrEventIDConflict); code != "EVENT_ID_CONFLICT" || retry {
		t.Fatal("conflict policy")
	}
}
