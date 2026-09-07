package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/inventory"
	"kafka-recovery-lab/internal/kafka"
	"kafka-recovery-lab/internal/mysql"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker_stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	stdinShutdown := flag.Bool("shutdown-on-stdin-close", false, "gracefully stop when a local supervisor closes stdin")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *stdinShutdown {
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); stop() }()
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
	reader := kafka.NewConsumer(brokers, config.ConsumerGroup())
	defer reader.Close()
	logger.Info("worker_started", "groupId", config.ConsumerGroup())
	err = inventory.Consume(ctx, reader, (inventory.Store{DB: db}).Decrement, logger)
	if ctx.Err() != nil {
		logger.Info("worker_stopped", "reason", "context canceled")
		return nil
	}
	return err
}
