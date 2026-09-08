package fault

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"kafka-recovery-lab/internal/event"
)

func TestFaultIsOptInAndTargeted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hooks, err := Configure("", "", nil, logger)
	if err != nil || hooks.BeforeDB != nil || hooks.BeforeCommit != nil || hooks.AfterCommit != nil {
		t.Fatal("default path must have no hooks")
	}
	if _, err := Configure("after-db-commit", "", nil, logger); err == nil {
		t.Fatal("untargeted crash accepted")
	}
	if _, err := Configure("invalid", "id", nil, logger); err == nil {
		t.Fatal("unknown point accepted")
	}
	e, _ := event.New(1, 1)
	hooks, err = Configure("after-db-commit", "another-event", nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := hooks.AfterCommit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	hooks, err = Configure("before-db", e.EventID, release, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := hooks.BeforeDB(ctx, e); err != context.Canceled {
		t.Fatalf("canceled barrier: %v", err)
	}
	close(release)
	if err := hooks.BeforeDB(context.Background(), e); err != nil {
		t.Fatal(err)
	}
}
