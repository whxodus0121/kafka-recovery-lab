package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
)

type readerStub struct {
	message   kafka.Message
	trace     *[]string
	commitErr error
	fetched   bool
}

func (r *readerStub) FetchMessage(context.Context) (kafka.Message, error) {
	if r.fetched {
		*r.trace = append(*r.trace, "fetch_next")
		return kafka.Message{}, io.EOF
	}
	r.fetched = true
	*r.trace = append(*r.trace, "fetch")
	return r.message, nil
}
func (r *readerStub) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	if len(messages) != 1 || messages[0].Offset != r.message.Offset {
		return errors.New("wrong commit")
	}
	*r.trace = append(*r.trace, "commit")
	return r.commitErr
}

func TestCommitOnlyAfterSuccessfulDBProcessing(t *testing.T) {
	e, _ := event.New(1, 1)
	value, _ := json.Marshal(e)
	for _, scenario := range []struct {
		name             string
		dbErr, commitErr error
		invalid          bool
		want             []string
	}{
		{"success", nil, nil, false, []string{"fetch", "db", "commit", "fetch_next"}},
		{"db failure stops without commit", errors.New("DB unavailable"), nil, false, []string{"fetch", "db"}},
		{"commit failure stops", nil, errors.New("commit rejected"), false, []string{"fetch", "db", "commit"}},
		{"invalid event stops", nil, nil, true, []string{"fetch"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var trace []string
			r := &readerStub{message: kafka.Message{Topic: event.OrdersTopic, Key: []byte(e.OrderID), Value: value, Offset: 42}, trace: &trace, commitErr: scenario.commitErr}
			if scenario.invalid {
				r.message.Value = []byte(`{}`)
			}
			err := Consume(context.Background(), r, func(context.Context, event.OrderCreated) error { trace = append(trace, "db"); return scenario.dbErr }, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err == nil || !reflect.DeepEqual(trace, scenario.want) {
				t.Fatalf("trace=%v error=%v", trace, err)
			}
		})
	}
}
