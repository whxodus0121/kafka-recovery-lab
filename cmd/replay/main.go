package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/inventory"
	internalKafka "kafka-recovery-lab/internal/kafka"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("replay_failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	dlqTopic := flag.String("dlq-topic", "inventory.dlq.v1", "DLQ topic containing the selected record")
	partition := flag.Int("partition", -1, "DLQ partition")
	offset := flag.Int64("offset", -1, "DLQ record offset")
	recoveryTopic := flag.String("recovery-topic", "inventory.recovery.v1", "recovery destination topic")
	flag.Parse()
	if *dlqTopic == "" || *partition < 0 || *offset < 0 || *recoveryTopic == "" || *dlqTopic == *recoveryTopic {
		return fmt.Errorf("dlq-topic, non-negative partition/offset and distinct recovery-topic are required")
	}
	brokers, err := config.Brokers()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: *dlqTopic, Partition: *partition, MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second})
	defer reader.Close()
	if err := reader.SetOffset(*offset); err != nil {
		return fmt.Errorf("select DLQ offset: %w", err)
	}
	record, err := reader.ReadMessage(readCtx)
	if err != nil {
		return fmt.Errorf("read DLQ record %s/%d/%d: %w", *dlqTopic, *partition, *offset, err)
	}
	if record.Offset != *offset {
		return fmt.Errorf("selected DLQ offset %d but broker returned %d", *offset, record.Offset)
	}
	replayID, err := inventory.NewReplayID()
	if err != nil {
		return err
	}
	out, count, err := inventory.BuildReplayMessage(record, *recoveryTopic, replayID)
	if err != nil {
		return err
	}
	writer := internalKafka.NewFailureWriter(brokers)
	defer writer.Close()
	publishCtx, publishCancel := context.WithTimeout(ctx, 10*time.Second)
	defer publishCancel()
	if err := writer.WriteMessages(publishCtx, out); err != nil {
		return fmt.Errorf("publish recovery record: %w", err)
	}
	logger.Info("replay_published", "replayId", replayID, "replayCount", count,
		"dlqTopic", record.Topic, "dlqPartition", record.Partition, "dlqOffset", record.Offset,
		"recoveryTopic", *recoveryTopic)
	return nil
}
