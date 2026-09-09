package inventory

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
)

func TestDBClassification(t *testing.T) {
	cases := []struct {
		err   error
		code  string
		retry bool
	}{
		{ErrProductMissing, "PRODUCT_MISSING", false}, {ErrInsufficientInventory, "INSUFFICIENT_INVENTORY", false},
		{driver.ErrBadConn, "DB_CONNECTION", true}, {mysql.ErrInvalidConn, "DB_CONNECTION", true},
		{io.EOF, "DB_CONNECTION", true}, {context.DeadlineExceeded, "DB_TIMEOUT", true},
		{&mysql.MySQLError{Number: 1213}, "DB_DEADLOCK", true}, {&mysql.MySQLError{Number: 1205}, "DB_LOCK_TIMEOUT", true},
		{&mysql.MySQLError{Number: 1064}, "", false}, {&mysql.MySQLError{Number: 1045}, "", false},
		{errors.New("bug"), "", false}, {context.Canceled, "", false},
		{&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "invalid"}}, "", false},
		{&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, "DB_CONNECTION", true},
	}
	for _, c := range cases {
		code, retry := classifyDB(fmt.Errorf("wrapped: %w", c.err))
		if code != c.code || retry != c.retry {
			t.Fatalf("%v => %s %v", c.err, code, retry)
		}
	}
}

func TestRouteBeforeSourceCommitAndStopOnUnknown(t *testing.T) {
	e, _ := event.New(1, 2)
	value, _ := json.Marshal(e)
	for _, tc := range []struct {
		name              string
		value             []byte
		key               string
		dbErr, publishErr error
		code, dest        string
		trace             []string
	}{
		{"retry", value, e.OrderID, driver.ErrBadConn, nil, "DB_CONNECTION", "retry", []string{"fetch", "db", "publish", "commit", "fetch_next"}},
		{"domain", value, e.OrderID, ErrInsufficientInventory, nil, "INSUFFICIENT_INVENTORY", "dlq", []string{"fetch", "db", "publish", "commit", "fetch_next"}},
		{"poison", []byte("{bad"), e.OrderID, nil, nil, "MALFORMED_JSON", "dlq", []string{"fetch", "publish", "commit", "fetch_next"}},
		{"key", value, "wrong", nil, nil, "KEY_MISMATCH", "dlq", []string{"fetch", "publish", "commit", "fetch_next"}},
		{"publish failure", value, e.OrderID, driver.ErrBadConn, errors.New("broker down"), "DB_CONNECTION", "retry", []string{"fetch", "db", "publish"}},
		{"DLQ failure", []byte("{"), e.OrderID, nil, errors.New("broker down"), "MALFORMED_JSON", "dlq", []string{"fetch", "publish"}},
		{"SQL bug", value, e.OrderID, &mysql.MySQLError{Number: 1064}, nil, "", "", []string{"fetch", "db"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := []string{}
			r := &readerStub{trace: &trace, message: kafka.Message{Topic: "main", Partition: 2, Offset: 7, Key: []byte(tc.key), Value: tc.value}}
			p := &FailurePolicy{RetryTopic: "retry", DLQTopic: "dlq", MaxRetries: 3, Delay: 2 * time.Second, Publish: func(_ context.Context, m kafka.Message) error {
				trace = append(trace, "publish")
				if m.Topic != tc.dest || string(m.Key) != tc.key {
					t.Fatal("routing/key mismatch")
				}
				if m.Topic == "dlq" {
					var d DeadLetter
					if err := json.Unmarshal(m.Value, &d); err != nil || d.ErrorCode != tc.code || string(d.Value) != string(tc.value) || d.Topic != "main" || d.Partition != 2 || d.Offset != 7 {
						t.Fatal("DLQ not lossless")
					}
				}
				return tc.publishErr
			}}
			_ = Consume(context.Background(), r, func(context.Context, event.OrderCreated) (Result, error) {
				trace = append(trace, "db")
				return Result{}, tc.dbErr
			}, slog.New(slog.NewTextHandler(io.Discard, nil)), Hooks{}, p)
			if !reflect.DeepEqual(trace, tc.trace) {
				t.Fatalf("trace=%v want=%v", trace, tc.trace)
			}
		})
	}
}

func TestMetadataBoundedRetriesAndDelay(t *testing.T) {
	var out kafka.Message
	p := &FailurePolicy{RetryTopic: "retry", DLQTopic: "dlq", MaxRetries: 3, Delay: 2 * time.Second, Publish: func(_ context.Context, m kafka.Message) error { out = m; return nil }}
	m := kafka.Message{Topic: "main", Partition: 2, Offset: 7, Key: []byte{0xff}, Value: []byte{0xff, 0xfe}}
	md, _ := p.metadata(m)
	first := time.Time{}
	for count := 1; count <= 3; count++ {
		if _, err := p.route(context.Background(), m, md, "DB_DEADLOCK", true, true); err != nil {
			t.Fatal(err)
		}
		p.RetrySource = true
		var err error
		md, err = p.metadata(out)
		if err != nil || md.Count != count || md.Topic != "main" || md.Partition != 2 || md.Offset != 7 || time.Until(md.Next) < time.Second {
			t.Fatalf("metadata=%+v err=%v", md, err)
		}
		if count == 1 {
			first = md.First
		} else if !md.First.Equal(first) {
			t.Fatal("first failed time changed")
		}
		m = out
	}
	valid := out
	_, err := p.route(context.Background(), m, md, "DB_DEADLOCK", true, true)
	var d DeadLetter
	if err != nil || out.Topic != "dlq" || json.Unmarshal(out.Value, &d) != nil || d.RetryCount == nil || *d.RetryCount != 3 || string(d.Value) != string(m.Value) {
		t.Fatal("exhaustion")
	}
	for _, bad := range []string{"-1", "0", "4", "abc", "999999999999999999999999"} {
		damaged := valid
		damaged.Headers = append([]kafka.Header(nil), valid.Headers...)
		damaged.Headers[0].Value = []byte(bad)
		if _, err := p.metadata(damaged); err == nil {
			t.Fatalf("accepted bad count %s", bad)
		}
	}
	duplicate := valid
	duplicate.Headers = append(append([]kafka.Header(nil), valid.Headers...), valid.Headers[0])
	if _, err := p.metadata(duplicate); err == nil {
		t.Fatal("duplicate accepted")
	}
	missing := valid
	missing.Headers = valid.Headers[:6]
	if _, err := p.metadata(missing); err == nil {
		t.Fatal("missing header accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitAttempt(ctx, time.Now().Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cannot cancel")
	}
}

func TestRecoveryStartsNewRetryChainAndCarriesReplayLineage(t *testing.T) {
	d := DeadLetter{ID: "dlq", Key: []byte("order"), Value: []byte("value"), Topic: "orders.created.v1", Partition: 0, Offset: 8, ErrorCode: "PRODUCT_MISSING", ErrorMessage: "PRODUCT_MISSING", FailedAt: time.Now().UTC()}
	payload, _ := json.Marshal(d)
	recovery, _, err := BuildReplayMessage(kafka.Message{Topic: "dlq", Partition: 0, Offset: 4, Value: payload}, "recovery", testReplayID)
	if err != nil {
		t.Fatal(err)
	}
	mainPolicy := &FailurePolicy{RetryTopic: "retry", DLQTopic: "dlq", MaxRetries: 3, Delay: time.Second, Publish: func(context.Context, kafka.Message) error { return nil }}
	if _, err := mainPolicy.metadata(recovery); err == nil {
		t.Fatal("main source accepted recovery metadata")
	}
	var out kafka.Message
	p := &FailurePolicy{RecoverySource: true, RetryTopic: "retry", DLQTopic: "dlq", MaxRetries: 3, Delay: time.Second, Publish: func(_ context.Context, m kafka.Message) error { out = m; return nil }}
	md, err := p.metadata(recovery)
	if err != nil || md.Count != 0 || md.Topic != "orders.created.v1" || md.Offset != 8 {
		t.Fatal("recovery metadata", md, err)
	}
	if _, err := p.route(context.Background(), recovery, md, "DB_CONNECTION", true, true); err != nil {
		t.Fatal(err)
	}
	if out.Topic != "retry" || headerValue(out.Headers, "retry-count") != "1" || headerValue(out.Headers, "replay-id") != testReplayID || headerValue(out.Headers, "original-offset") != "8" {
		t.Fatal("recovery retry did not reset/preserve metadata")
	}
	p.RecoverySource, p.RetrySource = false, true
	md, err = p.metadata(out)
	if err != nil || md.Count != 1 {
		t.Fatal("recovery retry metadata unreadable", md, err)
	}
	if _, err := p.route(context.Background(), out, md, "PRODUCT_MISSING", false, true); err != nil {
		t.Fatal(err)
	}
	letter, err := DecodeDeadLetter(out.Value)
	if err != nil || letter.ReplayID != testReplayID || letter.ReplayCount == nil || *letter.ReplayCount != 1 || letter.Topic != "orders.created.v1" || letter.Offset != 8 {
		t.Fatal("replay lineage lost in DLQ", letter, err)
	}
}
