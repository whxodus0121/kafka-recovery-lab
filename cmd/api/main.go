package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/kafka"
	"kafka-recovery-lab/internal/order"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("api_stopped", "error", err)
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
	producer := kafka.NewProducer(brokers)
	defer producer.Close()
	listener, err := net.Listen("tcp", config.APIAddress())
	if err != nil {
		return err
	}
	server := &http.Server{Handler: order.Handler(producer.Publish, logger),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	logger.Info("api_started", "address", listener.Addr().String())
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
			return err
		}
		logger.Info("api_stopped")
		return nil
	}
}
