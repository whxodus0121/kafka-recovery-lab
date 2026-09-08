package inventory

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestDelayStrategies(t *testing.T) {
	for _, strategy := range []string{"", "fixed", "exponential", "jitter"} {
		p := FailurePolicy{Strategy: strategy, Delay: time.Second, Cap: 5 * time.Second, Seed: 42}
		q := p
		for count := 1; count <= 100; count++ {
			upper := 5 * time.Second
			if count <= 3 {
				upper = time.Second * time.Duration(1<<(count-1))
			}
			got := p.delayFor(count)
			if strategy == "" || strategy == "fixed" {
				upper = time.Second
			}
			if strategy == "jitter" {
				if got < 0 || got > upper || got != q.delayFor(count) {
					t.Fatalf("jitter count=%d value=%v upper=%v", count, got, upper)
				}
			} else if got != upper {
				t.Fatalf("%s count=%d got=%v want=%v", strategy, count, got, upper)
			}
		}
	}
	// Saturation must not overflow, including caps smaller than base.
	for _, base := range []time.Duration{time.Nanosecond, time.Hour} {
		p := FailurePolicy{Strategy: "exponential", Delay: base, Cap: time.Second}
		if p.delayFor(100) != time.Second {
			t.Fatal("cap/overflow")
		}
	}
	for _, p := range []FailurePolicy{{Strategy: "unknown"}, {Strategy: "jitter"}, {Strategy: "exponential", Cap: 2 * time.Hour}} {
		if p.validateDelay() == nil {
			t.Fatal("invalid strategy accepted")
		}
	}
}

func TestScheduledHeaderSurvivesNewPolicy(t *testing.T) {
	for _, strategy := range []string{"fixed", "exponential", "jitter"} {
		var out kafka.Message
		p := FailurePolicy{Strategy: strategy, Delay: time.Second, Cap: 5 * time.Second, Seed: 123, MaxRetries: 5, RetryTopic: "retry", DLQTopic: "dlq", Publish: func(_ context.Context, m kafka.Message) error { out = m; return nil }}
		original := kafka.Message{Topic: "main", Partition: 2, Offset: 7, Key: []byte("key"), Value: []byte("value")}
		md := retryMetadata{Topic: original.Topic, Partition: 2, Offset: 7}
		for count := 1; count <= 5; count++ {
			before := time.Now().UTC()
			if _, err := p.route(context.Background(), original, md, "DB_CONNECTION", true, true); err != nil {
				t.Fatal(err)
			}
			after := time.Now().UTC()
			consumer := FailurePolicy{RetrySource: true, MaxRetries: 5, Strategy: "jitter", Seed: 999}
			read, err := consumer.metadata(out)
			if err != nil || read.Count != count || read.Topic != "main" || read.Offset != 7 || read.Partition != 2 {
				t.Fatalf("metadata: %+v %v", read, err)
			}
			upper := p.Delay
			if strategy != "fixed" {
				upper = 5 * time.Second
				if count <= 3 {
					upper = time.Second * time.Duration(1<<(count-1))
				}
			}
			if read.Next.Before(before) || read.Next.After(after.Add(upper)) {
				t.Fatal("scheduled range")
			}
			if strategy != "jitter" && read.Next.Before(before.Add(upper)) {
				t.Fatal("short fixed/exponential delay")
			}
			if consumer.random != nil {
				t.Fatal("reading redrew jitter")
			}
			again, err := consumer.metadata(out)
			if err != nil || again.Next != read.Next {
				t.Fatal("persisted schedule changed")
			}
			md = read
		}
	}
}

func TestWaitDoesNotStartEarly(t *testing.T) {
	at := time.Now().Add(20 * time.Millisecond)
	if err := waitAttempt(context.Background(), at); err != nil || time.Now().Before(at) {
		t.Fatal("early attempt", err)
	}
}
