package kafka

import (
	"context"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
)

type Producer struct{ writer *kafkago.Writer }

func NewFailureWriter(brokers []string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr: kafkago.TCP(brokers...), Balancer: &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll, Async: false, MaxAttempts: 1,
		AllowAutoTopicCreation: false, BatchTimeout: 10 * time.Millisecond,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
	}
}

func NewProducer(brokers []string) *Producer {
	return &Producer{writer: &kafkago.Writer{
		Addr: kafkago.TCP(brokers...), Topic: event.OrdersTopic,
		Balancer: &kafkago.Hash{}, RequiredAcks: kafkago.RequireAll,
		Async: false, AllowAutoTopicCreation: false, MaxAttempts: 1,
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  10 * time.Second, WriteTimeout: 10 * time.Second,
	}}
}

func (p *Producer) Publish(ctx context.Context, e event.OrderCreated) error {
	if err := e.Validate(); err != nil {
		return err
	}
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(e.OrderID), Value: value})
}

func (p *Producer) Close() error { return p.writer.Close() }

func NewConsumer(brokers []string, groupID, topic string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: groupID,
		// Zero means synchronous EXPLICIT CommitMessages, not automatic commits.
		CommitInterval: 0, StartOffset: kafkago.FirstOffset,
		QueueCapacity: 1, MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second,
		SessionTimeout: 10 * time.Second, HeartbeatInterval: 3 * time.Second,
	})
}
