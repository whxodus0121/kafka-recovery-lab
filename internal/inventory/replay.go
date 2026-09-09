package inventory

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

var replayHeaderKeys = []string{"replay-id", "replay-count", "replayed-from-dlq-topic", "replayed-from-dlq-partition", "replayed-from-dlq-offset"}
var replayUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type replayMetadata struct {
	Present   bool
	ID        string
	Count     int
	Topic     string
	Partition int
	Offset    int64
}

func parseReplayHeaders(headers []kafka.Header, required bool) (replayMetadata, error) {
	var md replayMetadata
	values := map[string]string{}
	for _, h := range headers {
		for _, key := range replayHeaderKeys {
			if h.Key != key {
				continue
			}
			if _, exists := values[key]; exists {
				return md, fmt.Errorf("duplicate replay header")
			}
			values[key] = string(h.Value)
		}
	}
	if len(values) == 0 && !required {
		return md, nil
	}
	if len(values) != len(replayHeaderKeys) {
		return md, fmt.Errorf("incomplete replay metadata")
	}
	count, err := strconv.Atoi(values["replay-count"])
	if err != nil || count < 1 || count > 1_000_000 || strconv.Itoa(count) != values["replay-count"] {
		return md, fmt.Errorf("invalid replay count")
	}
	part, err := strconv.Atoi(values["replayed-from-dlq-partition"])
	if err != nil || part < 0 || strconv.Itoa(part) != values["replayed-from-dlq-partition"] {
		return md, fmt.Errorf("invalid replay DLQ partition")
	}
	off, err := strconv.ParseInt(values["replayed-from-dlq-offset"], 10, 64)
	if err != nil || off < 0 || strconv.FormatInt(off, 10) != values["replayed-from-dlq-offset"] {
		return md, fmt.Errorf("invalid replay DLQ offset")
	}
	if !replayUUID.MatchString(values["replay-id"]) || values["replayed-from-dlq-topic"] == "" || len(values["replayed-from-dlq-topic"]) > 249 {
		return md, fmt.Errorf("invalid replay identity")
	}
	return replayMetadata{true, values["replay-id"], count, values["replayed-from-dlq-topic"], part, off}, nil
}

func NewReplayID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6], b[8] = (b[6]&0x0f)|0x40, (b[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func DecodeDeadLetter(value []byte) (DeadLetter, error) {
	var d DeadLetter
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&d); err != nil {
		return d, fmt.Errorf("decode DLQ envelope: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return d, fmt.Errorf("decode DLQ envelope: expected one JSON value")
	}
	if d.ID == "" || len(d.Key) == 0 || len(d.Value) == 0 || d.Topic == "" || len(d.Topic) > 249 || d.Partition < 0 || d.Offset < 0 || d.ErrorCode == "" || d.FailedAt.IsZero() {
		return d, fmt.Errorf("invalid DLQ envelope")
	}
	fields := 0
	if d.ReplayID != "" {
		fields++
	}
	if d.ReplayCount != nil {
		fields++
	}
	if d.ReplayedFromDLQTopic != "" {
		fields++
	}
	if d.ReplayedFromDLQPartition != nil {
		fields++
	}
	if d.ReplayedFromDLQOffset != nil {
		fields++
	}
	if fields != 0 {
		if fields != 5 || !replayUUID.MatchString(d.ReplayID) || *d.ReplayCount < 1 || *d.ReplayCount > 1_000_000 || d.ReplayedFromDLQTopic == "" || len(d.ReplayedFromDLQTopic) > 249 || *d.ReplayedFromDLQPartition < 0 || *d.ReplayedFromDLQOffset < 0 {
			return d, fmt.Errorf("invalid DLQ replay metadata")
		}
	}
	return d, nil
}

func BuildReplayMessage(dlqRecord kafka.Message, recoveryTopic, replayID string) (kafka.Message, int, error) {
	if dlqRecord.Topic == "" || dlqRecord.Partition < 0 || dlqRecord.Offset < 0 || recoveryTopic == "" || len(recoveryTopic) > 249 || !replayUUID.MatchString(replayID) {
		return kafka.Message{}, 0, fmt.Errorf("invalid replay request")
	}
	d, err := DecodeDeadLetter(dlqRecord.Value)
	if err != nil {
		return kafka.Message{}, 0, err
	}
	count := 1
	if d.ReplayCount != nil {
		if *d.ReplayCount == 1_000_000 {
			return kafka.Message{}, 0, fmt.Errorf("replay count limit reached")
		}
		count = *d.ReplayCount + 1
	}
	out := kafka.Message{Topic: recoveryTopic, Key: d.Key, Value: d.Value, Time: time.Now().UTC()}
	reserved := append(append([]string{}, metadataKeys...), replayHeaderKeys...)
	for _, h := range d.Headers {
		keep := true
		for _, key := range reserved {
			if h.Key == key {
				keep = false
				break
			}
		}
		if keep {
			out.Headers = append(out.Headers, h)
		}
	}
	values := []string{d.Topic, strconv.Itoa(d.Partition), strconv.FormatInt(d.Offset, 10), replayID, strconv.Itoa(count), dlqRecord.Topic, strconv.Itoa(dlqRecord.Partition), strconv.FormatInt(dlqRecord.Offset, 10)}
	keys := append(append([]string{}, originKeys...), replayHeaderKeys...)
	for i, key := range keys {
		out.Headers = append(out.Headers, kafka.Header{Key: key, Value: []byte(values[i])})
	}
	return out, count, nil
}
