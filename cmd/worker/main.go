package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/fault"
	"kafka-recovery-lab/internal/inventory"
	"kafka-recovery-lab/internal/kafka"
	"kafka-recovery-lab/internal/mysql"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("pid", os.Getpid())
	if err := run(logger); err != nil {
		logger.Error("worker_stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	stdinShutdown := flag.Bool("shutdown-on-stdin-close", false, "gracefully stop when a local supervisor closes stdin")
	topic := flag.String("topic", event.OrdersTopic, "consumer topic; override only for isolated local scenarios")
	faultPoint := flag.String("fault-point", "", "local only: before-db, before-db-commit, after-db-commit")
	faultEvent := flag.String("fault-event-id", "", "the sole event targeted by the local fault")
	retryWorker := flag.Bool("retry-worker", false, "consume the retry topic with inventory logic")
	recoveryWorker := flag.Bool("recovery-worker", false, "consume the recovery topic with inventory logic")
	retryTopic := flag.String("retry-topic", "inventory.retry.v1", "retry destination and retry worker source")
	dlqTopic := flag.String("dlq-topic", "inventory.dlq.v1", "dead letter destination")
	recoveryTopic := flag.String("recovery-topic", "inventory.recovery.v1", "recovery worker source")
	maxRetries := flag.Int("max-retries", 3, "additional attempts after the initial failure")
	delay := flag.Duration("retry-delay", 2*time.Second, "fixed delay, or base delay for exponential/jitter")
	strategy := flag.String("retry-strategy", "fixed", "fixed, exponential or jitter (full jitter)")
	capDelay := flag.Duration("retry-cap", 30*time.Second, "exponential/jitter upper delay cap")
	seed := flag.Int64("retry-seed", time.Now().UnixNano(), "per-process jitter RNG seed; recorded on startup")
	baseline := flag.Bool("phase2-baseline", false, "local Phase 2 regression only: no retry/DLQ or idempotency")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var release chan struct{}
	if *stdinShutdown {
		release = make(chan struct{})
	}
	hooks, err := fault.Configure(*faultPoint, *faultEvent, release, logger)
	if err != nil {
		return err
	}
	if *stdinShutdown {
		go func() {
			scanner := bufio.NewScanner(os.Stdin)
			released := false
			for scanner.Scan() {
				if scanner.Text() == "continue" && !released {
					close(release)
					released = true
				}
			}
			stop()
		}()
	}
	brokers, err := config.Brokers()
	if err != nil {
		return err
	}
	if *retryWorker {
		*topic = *retryTopic
	}
	if *recoveryWorker {
		*topic = *recoveryTopic
	}
	if *retryWorker && *recoveryWorker {
		return fmt.Errorf("retry-worker and recovery-worker are mutually exclusive")
	}
	if *baseline && (*retryWorker || *recoveryWorker) {
		return fmt.Errorf("phase2-baseline cannot run as retry or recovery worker")
	}
	var policy *inventory.FailurePolicy
	if !*baseline {
		writer := kafka.NewFailureWriter(brokers)
		defer writer.Close()
		policy = &inventory.FailurePolicy{RetrySource: *retryWorker, RecoverySource: *recoveryWorker, RetryTopic: *retryTopic, DLQTopic: *dlqTopic, MaxRetries: *maxRetries, Delay: *delay, Strategy: *strategy, Cap: *capDelay, Seed: *seed, Publish: func(ctx context.Context, m kafkago.Message) error { return writer.WriteMessages(ctx, m) }}
		if err := policy.Validate(); err != nil {
			return err
		}
		if *recoveryTopic == *retryTopic || *recoveryTopic == *dlqTopic || *topic == *dlqTopic {
			return fmt.Errorf("source and destination topics conflict")
		}
		if !*retryWorker && !*recoveryWorker && (*topic == *retryTopic || *topic == *recoveryTopic) {
			return fmt.Errorf("source and destination topics conflict")
		}
	}
	db, err := mysql.Pool()
	if err != nil {
		return err
	}
	defer db.Close()
	group := config.ConsumerGroup()
	if *retryWorker && os.Getenv("KAFKA_CONSUMER_GROUP") == "" {
		group = "inventory-retry-v1"
	}
	if *recoveryWorker && os.Getenv("KAFKA_CONSUMER_GROUP") == "" {
		group = "inventory-recovery-v1"
	}
	reader := kafka.NewConsumer(brokers, group, *topic)
	defer reader.Close()
	logger.Info("worker_started", "groupId", group, "topic", *topic, "retryWorker", *retryWorker, "recoveryWorker", *recoveryWorker, "phase2Baseline", *baseline,
		"retryStrategy", *strategy, "retryBase", delay.String(), "retryCap", capDelay.String(), "retrySeed", *seed)
	err = inventory.Consume(ctx, reader, (inventory.Store{DB: db, Hooks: hooks, LegacyBaseline: *baseline}).Decrement, logger, hooks, policy)
	if ctx.Err() != nil {
		logger.Info("worker_stopped", "reason", "context canceled")
		return nil
	}
	return err
}
