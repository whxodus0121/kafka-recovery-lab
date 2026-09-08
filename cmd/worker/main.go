package main

import (
	"bufio"
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	db, err := mysql.Open(connectCtx)
	cancel()
	if err != nil {
		return err
	}
	defer db.Close()
	reader := kafka.NewConsumer(brokers, config.ConsumerGroup(), *topic)
	defer reader.Close()
	logger.Info("worker_started", "groupId", config.ConsumerGroup())
	err = inventory.Consume(ctx, reader, (inventory.Store{DB: db, Hooks: hooks}).Decrement, logger, hooks)
	if ctx.Err() != nil {
		logger.Info("worker_stopped", "reason", "context canceled")
		return nil
	}
	return err
}
