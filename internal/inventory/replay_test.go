package inventory

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

const testReplayID = "123e4567-e89b-42d3-a456-426614174000"

func TestBuildReplayMessagePreservesOriginalAndResetsRetry(t *testing.T) {
	d := DeadLetter{
		ID: "dlq-1", Key: []byte("order-1"), Value: []byte("{bad"),
		Headers: []kafka.Header{{Key: "trace-id", Value: []byte("trace")}, {Key: "retry-count", Value: []byte("3")}},
		Topic:   "orders.created.v1", Partition: 2, Offset: 17, RetryCount: intPointer(3),
		ErrorCode: "MALFORMED_JSON", ErrorMessage: "MALFORMED_JSON", FailedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(d)
	out, count, err := BuildReplayMessage(kafka.Message{Topic: "inventory.dlq.v1", Partition: 0, Offset: 10, Value: payload}, "inventory.recovery.v1", testReplayID)
	if err != nil || count != 1 || string(out.Key) != "order-1" || string(out.Value) != "{bad" {
		t.Fatal("replay build failed", count, err)
	}
	for key, want := range map[string]string{
		"trace-id": "trace", "original-topic": "orders.created.v1", "original-partition": "2", "original-offset": "17",
		"replay-id": testReplayID, "replay-count": "1", "replayed-from-dlq-topic": "inventory.dlq.v1", "replayed-from-dlq-partition": "0", "replayed-from-dlq-offset": "10",
	} {
		if got := headerValue(out.Headers, key); got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
	for _, key := range retryControlKeys {
		if headerValue(out.Headers, key) != "" {
			t.Fatal("old retry scheduling header preserved", key)
		}
	}
}

func TestReplayLineageAndEnvelopeValidation(t *testing.T) {
	part, off, count := 1, int64(9), 1
	d := DeadLetter{ID: "dlq-2", Key: []byte("key"), Value: []byte("value"), Topic: "main", Partition: 0, Offset: 4, ErrorCode: "PRODUCT_MISSING", ErrorMessage: "PRODUCT_MISSING", FailedAt: time.Now().UTC(), ReplayID: testReplayID, ReplayCount: &count, ReplayedFromDLQTopic: "dlq", ReplayedFromDLQPartition: &part, ReplayedFromDLQOffset: &off}
	payload, _ := json.Marshal(d)
	_, gotCount, err := BuildReplayMessage(kafka.Message{Topic: "dlq", Partition: 0, Offset: 11, Value: payload}, "recovery", "223e4567-e89b-42d3-a456-426614174000")
	if err != nil || gotCount != 2 {
		t.Fatal("lineage count", gotCount, err)
	}

	invalid := []DeadLetter{
		{ID: "missing-key", Value: []byte("v"), Topic: "main", ErrorCode: "E", FailedAt: time.Now().UTC()},
		{ID: "missing-value", Key: []byte("k"), Topic: "main", ErrorCode: "E", FailedAt: time.Now().UTC()},
		{ID: "bad-topic", Key: []byte("k"), Value: []byte("v"), ErrorCode: "E", FailedAt: time.Now().UTC()},
		{ID: "bad-partition", Key: []byte("k"), Value: []byte("v"), Topic: "main", Partition: -1, ErrorCode: "E", FailedAt: time.Now().UTC()},
		{ID: "bad-offset", Key: []byte("k"), Value: []byte("v"), Topic: "main", Offset: -1, ErrorCode: "E", FailedAt: time.Now().UTC()},
		{ID: "partial-replay", Key: []byte("k"), Value: []byte("v"), Topic: "main", ErrorCode: "E", FailedAt: time.Now().UTC(), ReplayID: testReplayID},
	}
	for _, bad := range invalid {
		b, _ := json.Marshal(bad)
		if _, err := DecodeDeadLetter(b); err == nil {
			t.Fatal("accepted invalid envelope", bad.ID)
		}
	}
	if _, err := DecodeDeadLetter([]byte("{bad")); err == nil {
		t.Fatal("accepted malformed envelope")
	}
}

func intPointer(v int) *int { return &v }

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
