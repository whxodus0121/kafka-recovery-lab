package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/config"
)

const smokeTopic = "phase0.smoke.v1"

func kafkaSmoke(ctx context.Context) error {
	brokers, err := config.Brokers()
	if err != nil {
		return err
	}
	// No group is needed for this one-partition transport probe. Start at the
	// current tail so a previous run's message can never satisfy this check.
	dialer := &kafka.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialLeader(ctx, "tcp", brokers[0], smokeTopic, 0)
	if err != nil {
		return fmt.Errorf("Kafka connect (initialize topics first): %w", err)
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return err
	}
	start, err := conn.ReadLastOffset()
	closeErr := conn.Close()
	if err != nil {
		return fmt.Errorf("Kafka tail offset: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}

	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("phase0-%x", token))
	value := append([]byte("transport-probe:"), key...)
	writer := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: smokeTopic,
		RequiredAcks: kafka.RequireAll, Async: false, MaxAttempts: 1,
		AllowAutoTopicCreation: false, WriteTimeout: 10 * time.Second,
	}
	err = writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
	closeErr = writer.Close()
	if err != nil {
		return fmt.Errorf("Kafka produce: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Printf("KAFKA_PRODUCE_PASS topic=%s key=%s\n", smokeTopic, key)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: smokeTopic, Partition: 0, Dialer: dialer,
		MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second,
	})
	defer reader.Close()
	if err := reader.SetOffset(start); err != nil {
		return err
	}
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("Kafka consume: %w", err)
		}
		if !bytes.Equal(msg.Key, key) {
			continue
		}
		if !bytes.Equal(msg.Value, value) {
			return fmt.Errorf("Kafka payload mismatch")
		}
		fmt.Printf("KAFKA_PASS topic=%s partition=%d offset=%d key=%s payload_match=true\n", msg.Topic, msg.Partition, msg.Offset, msg.Key)
		return nil
	}
}
