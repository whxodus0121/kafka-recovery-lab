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
	"kafka-recovery-lab/internal/ratelimit"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger, os.Args[1:]); err != nil {
		logger.Error("replay_failed", "error", err)
		os.Exit(1)
	}
}

type options struct {
	dlqTopic, recoveryTopic    string
	partition                  int
	offset, startOffset, limit int64
	publishRate                float64
}

func parseOptions(args []string) (options, error) {
	var o options
	set := flag.NewFlagSet("replay", flag.ContinueOnError)
	set.StringVar(&o.dlqTopic, "dlq-topic", "inventory.dlq.v1", "DLQ topic containing the selected record")
	set.StringVar(&o.recoveryTopic, "recovery-topic", "orders.recovery.v1", "destination recovery topic")
	set.IntVar(&o.partition, "partition", -1, "DLQ partition")
	set.Int64Var(&o.offset, "offset", -1, "single DLQ record offset")
	set.Int64Var(&o.startOffset, "start-offset", -1, "first DLQ offset for bulk replay")
	set.Int64Var(&o.limit, "limit", 0, "maximum records for bulk replay")
	set.Float64Var(&o.publishRate, "publish-rate", 0, "maximum publication starts per second; 0 is unlimited")
	if err := set.Parse(args); err != nil {
		return o, err
	}
	single := o.offset >= 0 && o.startOffset == -1 && o.limit == 0
	bulk := o.offset == -1 && o.startOffset >= 0 && o.limit > 0
	if o.dlqTopic == "" || o.partition < 0 || o.recoveryTopic == "" || o.dlqTopic == o.recoveryTopic || o.publishRate < 0 || (!single && !bulk) || set.NArg() != 0 {
		return o, fmt.Errorf("choose either -offset or both -start-offset and positive -limit; topics must be distinct, partition non-negative and publish-rate non-negative")
	}
	if single {
		o.startOffset, o.limit = o.offset, 1
	}
	return o, nil
}

func run(ctx context.Context, logger *slog.Logger, args []string) error {
	o, err := parseOptions(args)
	if err != nil {
		return err
	}
	brokers, err := config.Brokers()
	if err != nil {
		return err
	}
	var pacer *ratelimit.Pacer
	if o.publishRate > 0 {
		pacer, err = ratelimit.New(o.publishRate)
		if err != nil {
			return err
		}
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: o.dlqTopic, Partition: o.partition, MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second})
	defer reader.Close()
	if err := reader.SetOffset(o.startOffset); err != nil {
		return fmt.Errorf("select DLQ offset: %w", err)
	}
	writer := internalKafka.NewFailureWriter(brokers)
	defer writer.Close()
	batchID, err := inventory.NewReplayID()
	if err != nil {
		return err
	}
	started := time.Now().UTC()
	var firstPublished, lastPublished time.Time
	for index := int64(0); index < o.limit; index++ {
		expected := o.startOffset + index
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		record, readErr := reader.ReadMessage(readCtx)
		cancel()
		if readErr != nil {
			return fmt.Errorf("batch=%s published=%d failed=%s/%d/%d: read DLQ: %w", batchID, index, o.dlqTopic, o.partition, expected, readErr)
		}
		if record.Offset != expected {
			return fmt.Errorf("batch=%s published=%d failed=%s/%d/%d: broker returned offset %d", batchID, index, o.dlqTopic, o.partition, expected, record.Offset)
		}
		replayID, replayErr := inventory.NewReplayID()
		if replayErr != nil {
			return replayErr
		}
		out, count, buildErr := inventory.BuildReplayMessage(record, o.recoveryTopic, replayID)
		if buildErr != nil {
			return fmt.Errorf("batch=%s published=%d failed=%s/%d/%d: %w", batchID, index, record.Topic, record.Partition, record.Offset, buildErr)
		}
		if pacer != nil {
			if waitErr := pacer.Wait(ctx); waitErr != nil {
				return fmt.Errorf("batch=%s published=%d failed=%s/%d/%d: publication wait: %w", batchID, index, record.Topic, record.Partition, record.Offset, waitErr)
			}
		}
		publishCtx, publishCancel := context.WithTimeout(ctx, 10*time.Second)
		publishErr := writer.WriteMessages(publishCtx, out)
		publishCancel()
		if publishErr != nil {
			return fmt.Errorf("batch=%s published=%d failed=%s/%d/%d: publish recovery record: %w", batchID, index, record.Topic, record.Partition, record.Offset, publishErr)
		}
		publishedAt := time.Now().UTC()
		if firstPublished.IsZero() {
			firstPublished = publishedAt
		}
		lastPublished = publishedAt
		logger.Info("replay_published", "batchId", batchID, "batchIndex", index, "publishedAt", publishedAt,
			"replayId", replayID, "replayCount", count, "dlqTopic", record.Topic,
			"dlqPartition", record.Partition, "dlqOffset", record.Offset, "recoveryTopic", o.recoveryTopic)
	}
	logger.Info("replay_completed", "batchId", batchID, "recordCount", o.limit, "publishRate", o.publishRate,
		"startedAt", started, "firstPublishedAt", firstPublished, "lastPublishedAt", lastPublished,
		"completedAt", time.Now().UTC())
	return nil
}
