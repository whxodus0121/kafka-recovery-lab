package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPacerSpacingAndCancellation(t *testing.T) {
	canceledImmediately, cancelImmediately := context.WithCancel(context.Background())
	cancelImmediately()
	fresh, _ := New(20)
	if err := fresh.Wait(canceledImmediately); !errors.Is(err, context.Canceled) {
		t.Fatalf("first wait cancellation=%v", err)
	}

	pacer, err := New(20)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	start := time.Now()
	var waited time.Duration
	for range 3 {
		duration, err := pacer.WaitDuration(ctx)
		if err != nil {
			t.Fatal(err)
		}
		waited += duration
	}
	if elapsed := time.Since(start); elapsed < 95*time.Millisecond {
		t.Fatalf("pacer ran too quickly: %v", elapsed)
	}
	if waited < 95*time.Millisecond {
		t.Fatalf("reported wait too short: %v", waited)
	}

	slow, _ := New(0.1)
	if err := slow.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := slow.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation=%v", err)
	}
	for _, invalid := range []float64{0, -1, 1e12} {
		if _, err := New(invalid); err == nil {
			t.Fatalf("accepted rate %v", invalid)
		}
	}
}
