package inventory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	mathrand "math/rand"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
)

type FailurePolicy struct {
	RetrySource, RecoverySource bool
	RetryTopic, DLQTopic        string
	MaxRetries                  int
	Delay                       time.Duration
	Strategy                    string
	Cap                         time.Duration
	Seed                        int64
	random                      *mathrand.Rand
	Publish                     func(context.Context, kafka.Message) error
	BeforeApply                 func(context.Context, kafka.Message, event.OrderCreated) error
}

type retryMetadata struct {
	Count       int
	Next, First time.Time
	Code, Topic string
	Partition   int
	Offset      int64
}

var retryControlKeys = []string{"retry-count", "next-attempt-at", "first-failed-at", "last-error-code"}
var originKeys = []string{"original-topic", "original-partition", "original-offset"}
var metadataKeys = append(append([]string{}, retryControlKeys...), originKeys...)

func (p *FailurePolicy) Validate() error {
	if p.RetrySource && p.RecoverySource {
		return fmt.Errorf("retry and recovery source modes are mutually exclusive")
	}
	if p.MaxRetries < 0 || p.MaxRetries > 100 || p.Delay <= 0 || p.Delay > time.Hour || p.RetryTopic == "" || p.DLQTopic == "" || p.RetryTopic == p.DLQTopic || p.Publish == nil {
		return fmt.Errorf("invalid retry policy: retries 0..100, delay (0,1h], distinct topics and publisher required")
	}
	return p.validateDelay()
}

func (p *FailurePolicy) metadata(m kafka.Message) (retryMetadata, error) {
	md := retryMetadata{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset}
	values := map[string]string{}
	for _, h := range m.Headers {
		for _, key := range metadataKeys {
			if h.Key != key {
				continue
			}
			if _, exists := values[key]; exists {
				return md, fmt.Errorf("duplicate retry header")
			}
			values[key] = string(h.Value)
		}
	}
	if p.RecoverySource {
		for _, key := range retryControlKeys {
			if _, exists := values[key]; exists {
				return md, fmt.Errorf("retry scheduling headers on recovery source")
			}
		}
		origin, err := parseOrigin(values)
		if err != nil {
			return md, err
		}
		if _, err := parseReplayHeaders(m.Headers, true); err != nil {
			return md, err
		}
		return origin, nil
	}
	if !p.RetrySource {
		if len(values) != 0 {
			return md, fmt.Errorf("retry headers on main source")
		}
		replay, err := parseReplayHeaders(m.Headers, false)
		if err != nil {
			return md, err
		}
		if replay.Present {
			return md, fmt.Errorf("replay headers on main source")
		}
		return md, nil
	}
	if len(values) != len(metadataKeys) {
		return md, fmt.Errorf("incomplete retry metadata")
	}
	count, err := strconv.Atoi(values["retry-count"])
	if err != nil || count < 1 || count > p.MaxRetries || strconv.Itoa(count) != values["retry-count"] {
		return md, fmt.Errorf("invalid retry count")
	}
	origin, err := parseOrigin(values)
	if err != nil {
		return md, err
	}
	first, err := time.Parse(time.RFC3339Nano, values["first-failed-at"])
	if err != nil {
		return md, fmt.Errorf("invalid first failure time")
	}
	next, err := time.Parse(time.RFC3339Nano, values["next-attempt-at"])
	if err != nil || first.IsZero() || next.Before(first) || next.After(time.Now().Add(time.Hour)) {
		return md, fmt.Errorf("invalid next attempt time")
	}
	switch values["last-error-code"] {
	case "DB_CONNECTION", "DB_TIMEOUT", "DB_DEADLOCK", "DB_LOCK_TIMEOUT":
	default:
		return md, fmt.Errorf("invalid last error code")
	}
	if _, err := parseReplayHeaders(m.Headers, false); err != nil {
		return md, err
	}
	return retryMetadata{count, next, first, values["last-error-code"], origin.Topic, origin.Partition, origin.Offset}, nil
}

func parseOrigin(values map[string]string) (retryMetadata, error) {
	md := retryMetadata{}
	if values["original-topic"] == "" || len(values["original-topic"]) > 249 {
		return md, fmt.Errorf("invalid original topic")
	}
	part, err := strconv.Atoi(values["original-partition"])
	if err != nil || part < 0 || strconv.Itoa(part) != values["original-partition"] {
		return md, fmt.Errorf("invalid original partition")
	}
	off, err := strconv.ParseInt(values["original-offset"], 10, 64)
	if err != nil || off < 0 || strconv.FormatInt(off, 10) != values["original-offset"] {
		return md, fmt.Errorf("invalid original offset")
	}
	return retryMetadata{Topic: values["original-topic"], Partition: part, Offset: off}, nil
}

func waitAttempt(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// []byte JSON encoding preserves arbitrary original key/value bytes as Base64.
type DeadLetter struct {
	ID                       string         `json:"dlqId"`
	Key                      []byte         `json:"originalKey"`
	Value                    []byte         `json:"originalRawValue"`
	Headers                  []kafka.Header `json:"sourceHeaders"`
	Topic                    string         `json:"originalTopic"`
	Partition                int            `json:"originalPartition"`
	Offset                   int64          `json:"originalOffset"`
	RetryCount               *int           `json:"retryCount"`
	ErrorCode                string         `json:"errorCode"`
	ErrorMessage             string         `json:"errorMessage"`
	FailedAt                 time.Time      `json:"failedAt"`
	ReplayID                 string         `json:"replayId,omitempty"`
	ReplayCount              *int           `json:"replayCount,omitempty"`
	ReplayedFromDLQTopic     string         `json:"replayedFromDlqTopic,omitempty"`
	ReplayedFromDLQPartition *int           `json:"replayedFromDlqPartition,omitempty"`
	ReplayedFromDLQOffset    *int64         `json:"replayedFromDlqOffset,omitempty"`
}

func (p *FailurePolicy) route(ctx context.Context, m kafka.Message, md retryMetadata, code string, retryable, validMetadata bool) (string, error) {
	now := time.Now().UTC()
	out := kafka.Message{Key: m.Key}
	if retryable && validMetadata && md.Count < p.MaxRetries {
		if md.First.IsZero() {
			md.First = now
		}
		out.Topic = p.RetryTopic
		out.Value = m.Value
		// Preserve application headers, replacing our complete metadata atomically.
		for _, h := range m.Headers {
			reserved := false
			for _, key := range metadataKeys {
				if h.Key == key {
					reserved = true
				}
			}
			if !reserved {
				out.Headers = append(out.Headers, h)
			}
		}
		// Calculate once at publication; consuming a persisted header never draws again.
		values := []string{strconv.Itoa(md.Count + 1), now.Add(p.delayFor(md.Count + 1)).Format(time.RFC3339Nano), md.First.Format(time.RFC3339Nano), code, md.Topic, strconv.Itoa(md.Partition), strconv.FormatInt(md.Offset, 10)}
		for i, key := range metadataKeys {
			out.Headers = append(out.Headers, kafka.Header{Key: key, Value: []byte(values[i])})
		}
	} else {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return "", err
		}
		count := &md.Count
		if !validMetadata {
			count = nil
		} // Never silently reset malformed retry counts.
		message := code
		if retryable {
			message = "retry limit reached: " + code
		}
		// Error messages are bounded, policy-owned text; no raw DSN/SQL/driver data.
		dlq := DeadLetter{ID: hex.EncodeToString(id[:]), Key: m.Key, Value: m.Value, Headers: m.Headers, Topic: md.Topic, Partition: md.Partition, Offset: md.Offset, RetryCount: count, ErrorCode: code, ErrorMessage: message, FailedAt: now}
		if replay, replayErr := parseReplayHeaders(m.Headers, false); replayErr == nil && replay.Present {
			dlq.ReplayID = replay.ID
			dlq.ReplayCount = &replay.Count
			dlq.ReplayedFromDLQTopic = replay.Topic
			dlq.ReplayedFromDLQPartition = &replay.Partition
			dlq.ReplayedFromDLQOffset = &replay.Offset
		}
		payload, err := json.Marshal(dlq)
		if err != nil {
			return "", err
		}
		out.Topic = p.DLQTopic
		out.Value = payload
	}
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.Publish(publishCtx, out); err != nil {
		return "", fmt.Errorf("publish failure destination=%s: %w", out.Topic, err)
	}
	return out.Topic, nil
}
