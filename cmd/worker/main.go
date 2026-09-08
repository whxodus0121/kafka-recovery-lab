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
	retryWorker := flag.Bool("retry-worker", false, "consume the fixed retry topic with inventory logic")
	retryTopic := flag.String("retry-topic", "inventory.retry.v1", "retry destination and retry worker source")
	dlqTopic := flag.String("dlq-topic", "inventory.dlq.v1", "dead letter destination")
	maxRetries := flag.Int("max-retries", 3, "additional attempts after the initial failure")
	delay := flag.Duration("retry-delay", 2*time.Second, "fixed delay before each retry")
	baseline := flag.Bool("phase2-baseline", false, "local regression only: stop on failures without retry/DLQ")
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
	var policy *inventory.FailurePolicy
	if !*baseline {
		writer := kafka.NewFailureWriter(brokers)
		defer writer.Close()
		policy = &inventory.FailurePolicy{RetrySource: *retryWorker, RetryTopic: *retryTopic, DLQTopic: *dlqTopic, MaxRetries: *maxRetries, Delay: *delay, Publish: func(ctx context.Context, m kafkago.Message) error { return writer.WriteMessages(ctx, m) }}
		if err := policy.Validate(); err != nil {
			return err
		}
		if *topic == *dlqTopic || (!*retryWorker && *topic == *retryTopic) {
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
	reader := kafka.NewConsumer(brokers, group, *topic)
	defer reader.Close()
	logger.Info("worker_started", "groupId", group, "topic", *topic, "phase2Baseline", *baseline)
	err = inventory.Consume(ctx, reader, (inventory.Store{DB: db, Hooks: hooks}).Decrement, logger, hooks, policy)
	if ctx.Err() != nil {
		logger.Info("worker_stopped", "reason", "context canceled")
		return nil
	}
	return err
}
