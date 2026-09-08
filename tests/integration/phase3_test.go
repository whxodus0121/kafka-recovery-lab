//go:build integration && phase2 && phase3

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/inventory"
	"kafka-recovery-lab/internal/mysql"
)

func TestPhase3(t *testing.T) {
	for _, name := range []string{"Normal", "Poison", "Recovery", "Exhaustion", "Metadata", "Domain", "RetryPublishFailure", "DLQPublishFailure"} {
		t.Run(name, func(t *testing.T) { phase3(t, name) })
	}
}

func phase3(t *testing.T, name string) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := mysql.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	historical := inventoryRows(t, ctx, db)
	e, err := event.New(time.Now().UnixNano(), 2)
	if err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(e)
	if _, err := db.ExecContext(ctx, "INSERT INTO inventory (product_id,available_quantity,updated_at) VALUES (?,100,UTC_TIMESTAMP(6))", e.ProductID); err != nil {
		t.Fatal(err)
	}
	id := "phase3." + e.EventID
	topics := []string{id + ".main", id + ".retry", id + ".dlq"}
	evidence := map[string]any{"scenario": name, "runId": id, "event": e, "startedAt": time.Now().UTC(), "status": "FAIL", "stage": "initialization", "initialInventory": stock(t, ctx, db, e.ProductID), "topics": topics, "maxRetries": 3, "delaySeconds": 2}
	workers := []*child{}
	defer func() {
		for _, w := range workers {
			if !w.stopped {
				w.stop(t)
			}
		}
		logs := []map[string]any{}
		for _, w := range workers {
			logs = append(logs, map[string]any{"pid": w.cmd.Process.Pid, "exitCode": w.cmd.ProcessState.ExitCode(), "log": w.log.text()})
		}
		evidence["workers"] = logs
		evidence["finishedAt"] = time.Now().UTC()
		if t.Failed() {
			evidence["status"] = "FAIL"
		}
		savePhase3(t, root, evidence)
	}()
	brokers, err := config.Brokers()
	if err != nil {
		t.Fatal(err)
	}
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second, Transport: transport}
	configs := []kafka.TopicConfig{}
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range created.Errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	// CreateTopics acknowledgment precedes leader readiness on a new partition.
	for _, topic := range topics {
		await(t, "new partition leader", func() bool {
			conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
			if err == nil {
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				_, err = conn.ReadLastOffset()
				conn.Close()
			}
			if errors.Is(err, kafka.NotLeaderForPartition) || errors.Is(err, kafka.LeaderNotAvailable) {
				return false
			}
			if err != nil {
				t.Fatal(err)
			}
			return true
		})
	}
	inspectors := make([]inspector, 3)
	for index, topic := range topics {
		group := id + "-" + strconv.Itoa(index)
		await(t, "coordinator", func() bool {
			r, err := client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{Addr: kafka.TCP(brokers...), Key: group, KeyType: kafka.CoordinatorKeyTypeConsumer})
			if err != nil {
				t.Fatal(err)
			}
			if errors.Is(r.Error, kafka.GroupCoordinatorNotAvailable) {
				return false
			}
			if r.Error != nil {
				t.Fatal(r.Error)
			}
			client.Addr = kafka.TCP(net.JoinHostPort(r.Coordinator.Host, strconv.Itoa(r.Coordinator.Port)))
			return true
		})
		inspectors[index] = inspector{t: t, ctx: ctx, brokers: brokers, client: client, group: group, topic: topic, partitions: []int{0}}
	}
	mainIn, retryIn, dlqIn := inspectors[0], inspectors[1], inspectors[2]
	evidence["mainGroup"] = mainIn.group
	evidence["retryGroup"] = retryIn.group
	evidence["before"] = []snapshot{mainIn.snapshot(), retryIn.snapshot(), dlqIn.snapshot()}
	evidence["stage"] = "scenario"
	publish := func(topic string, messages ...kafka.Message) {
		conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if err := conn.SetRequiredAcks(-1); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		_, err = conn.WriteMessages(messages...)
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	raw := kafka.Message{Key: []byte(e.OrderID), Value: value}
	if name == "Poison" || name == "DLQPublishFailure" {
		raw.Value = []byte("{invalid-json")
	}
	if name == "Domain" {
		bad := e
		bad.Quantity = 101
		raw.Value, _ = json.Marshal(bad)
	}
	if name == "Metadata" {
		raw.Headers = []kafka.Header{{Key: "retry-count", Value: []byte("-1")}}
		publish(topics[1], raw)
	} else {
		publish(topics[0], raw)
		if name == "Poison" {
			publish(topics[0], kafka.Message{Key: []byte(e.OrderID), Value: value})
		}
		if name == "Domain" {
			missing, _ := event.New(e.ProductID+1, 1)
			missingValue, _ := json.Marshal(missing)
			evidence["missingProductEvent"] = missing
			publish(topics[0], kafka.Message{Key: []byte(missing.OrderID), Value: missingValue})
		}
	}
	source := mainIn
	if name == "Metadata" {
		source = retryIn
	}
	observed := source.read(0, 0)
	if !bytes.Equal(observed.Value, raw.Value) || !bytes.Equal(observed.Key, raw.Key) {
		t.Fatal("source record mismatch")
	}
	evidence["source"] = observed
	binary := build(t, root, "worker")
	retryTopic, dlqTopic := topics[1], topics[2]
	// Missing destination + disabled automatic creation deterministically fails publish.
	if name == "RetryPublishFailure" {
		retryTopic = id + ".missing"
	}
	if name == "DLQPublishFailure" {
		dlqTopic = id + ".missing"
	}
	delay := "2s"
	if name == "Recovery" {
		delay = "12s"
		evidence["delaySeconds"] = 12
	}
	startWorker := func(retry bool, barrier bool) *child {
		group := mainIn.group
		args := []string{"-topic", topics[0], "-retry-topic", retryTopic, "-dlq-topic", dlqTopic, "-retry-delay", delay}
		if retry {
			args = append(args, "-retry-worker")
			group = retryIn.group
		}
		if barrier {
			args = append(args, "-fault-point", "before-db", "-fault-event-id", e.EventID)
		}
		w := start(t, root, binary, []string{"KAFKA_CONSUMER_GROUP=" + group}, args...)
		workers = append(workers, w)
		return w
	}
	outage := name == "Recovery" || name == "Exhaustion" || name == "RetryPublishFailure"
	main := startWorker(name == "Metadata", outage)
	restore := func() {
		docker(t, root, "compose", "up", "-d", "--wait", "--wait-timeout", "120", "mysql")
		_ = db.Close()
		db, err = mysql.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if outage {
		await(t, "before DB fault barrier", func() bool { return main.log.has("fault_reached") })
		defer func() { restore(); db.Close() }()
		docker(t, root, "compose", "stop", "mysql")
		if _, err := io.WriteString(main.stdin, "continue\n"); err != nil {
			t.Fatal(err)
		}
	}
	if name == "RetryPublishFailure" || name == "DLQPublishFailure" {
		expectedExit(t, main, 1)
		if mainIn.committed()[0] != -1 {
			t.Fatal("source committed after publication failure")
		}
		if !main.log.has("worker_stopped") {
			t.Fatal("failure not surfaced")
		}
		if outage {
			restore()
		}
		evidence["after"] = []snapshot{mainIn.snapshot(), retryIn.snapshot(), dlqIn.snapshot()}
		evidence["finalInventory"] = stock(t, ctx, db, e.ProductID)
		if evidence["finalInventory"] != int64(100) || retryIn.ends()[0] != 0 || dlqIn.ends()[0] != 0 {
			t.Fatal("failed publication changed downstream state")
		}
	} else if outage {
		await(t, "source committed after retry publication", func() bool { return mainIn.committed()[0] == 1 })
		main.stop(t)
		evidence["afterMainFailure"] = []snapshot{mainIn.snapshot(), retryIn.snapshot(), dlqIn.snapshot()}
		if retryIn.ends()[0] != 1 || dlqIn.ends()[0] != 0 {
			t.Fatal("wrong first routing")
		}
		if name == "Recovery" {
			restore()
			evidence["inventoryBeforeRetry"] = stock(t, ctx, db, e.ProductID)
		}
		worker := startWorker(true, false)
		if name == "Recovery" {
			await(t, "retry delay barrier", func() bool { return worker.log.has("retry_wait") })
			record := retryIn.read(0, 0)
			next, err := time.Parse(time.RFC3339Nano, header(record, "next-attempt-at"))
			if err != nil {
				t.Fatal(err)
			}
			if time.Now().Before(next) && stock(t, ctx, db, e.ProductID) != 100 {
				t.Fatal("processed before delay")
			}
			await(t, "recovery DB and offset", func() bool { return retryIn.committed()[0] == 1 && stock(t, ctx, db, e.ProductID) == 98 })
			worker.stop(t)
			received, err := time.Parse(time.RFC3339Nano, worker.log.field("event_received", "time"))
			if err != nil || received.Before(next) {
				t.Fatal("retry ran before next-attempt-at")
			}
			evidence["nextAttemptAt"] = next
			evidence["retryProcessedAt"] = received
			if retryIn.ends()[0] != 1 || dlqIn.ends()[0] != 0 {
				t.Fatal("recovery unexpectedly retried again")
			}
		} else {
			await(t, "bounded exhaustion", func() bool { return retryIn.committed()[0] == 3 && dlqIn.ends()[0] == 1 })
			worker.stop(t)
			restore()
			if retryIn.ends()[0] != 3 || stock(t, ctx, db, e.ProductID) != 100 {
				t.Fatal("retry limit or stock violated")
			}
		}
	} else {
		want := int64(1)
		if name == "Poison" || name == "Domain" {
			want = 2
		}
		await(t, "source offset", func() bool { return source.committed()[0] == want })
		main.stop(t)
		wantStock := int64(98)
		wantDLQ := int64(0)
		switch name {
		case "Poison":
			wantDLQ = 1
		case "Domain":
			wantDLQ = 2
			wantStock = 100
		case "Metadata":
			wantDLQ = 1
			wantStock = 100
		}
		if stock(t, ctx, db, e.ProductID) != wantStock || dlqIn.ends()[0] != wantDLQ {
			t.Fatal("normal/poison/domain result mismatch")
		}
		if name != "Metadata" && retryIn.ends()[0] != 0 {
			t.Fatal("unexpected retry")
		}
	}
	// Independently read every output record and verify metadata, not just logs.
	retries := []kafka.Message{}
	for offset := int64(0); offset < retryIn.ends()[0]; offset++ {
		m := retryIn.read(0, offset)
		retries = append(retries, m)
		if name != "Metadata" {
			if header(m, "retry-count") != strconv.FormatInt(offset+1, 10) || header(m, "original-topic") != topics[0] || header(m, "original-partition") != "0" || header(m, "original-offset") != "0" || !bytes.Equal(m.Value, value) || !bytes.Equal(m.Key, raw.Key) {
				t.Fatal("retry metadata/bytes mismatch")
			}
			if header(m, "last-error-code") != "DB_CONNECTION" {
				t.Fatal("outage not classified as connection error")
			}
			first, err := time.Parse(time.RFC3339Nano, header(m, "first-failed-at"))
			if err != nil {
				t.Fatal(err)
			}
			next, err := time.Parse(time.RFC3339Nano, header(m, "next-attempt-at"))
			if err != nil || next.Before(first) {
				t.Fatal("retry timestamps invalid")
			}
			if offset > 0 && header(m, "first-failed-at") != header(retries[0], "first-failed-at") {
				t.Fatal("first failure changed")
			}
		}
	}
	letters := []inventory.DeadLetter{}
	for offset := int64(0); offset < dlqIn.ends()[0]; offset++ {
		m := dlqIn.read(0, offset)
		var d inventory.DeadLetter
		if err := json.Unmarshal(m.Value, &d); err != nil {
			t.Fatal(err)
		}
		if d.ID == "" || d.FailedAt.IsZero() || len(d.ErrorMessage) > 256 || !bytes.Equal(d.Key, m.Key) {
			t.Fatal("invalid DLQ envelope")
		}
		wantTopic := topics[0]
		if name == "Metadata" {
			wantTopic = topics[1]
		}
		if d.Topic != wantTopic || d.Partition != 0 || d.Offset != offset {
			t.Fatal("original coordinate lost")
		}
		original := source.read(0, offset)
		if !bytes.Equal(d.Value, original.Value) || !bytes.Equal(d.Key, original.Key) {
			t.Fatal("DLQ not lossless")
		}
		switch name {
		case "Poison":
			if d.ErrorCode != "MALFORMED_JSON" || d.RetryCount == nil || *d.RetryCount != 0 {
				t.Fatal("poison routing")
			}
		case "Metadata":
			if d.ErrorCode != "INVALID_RETRY_METADATA" || d.RetryCount != nil {
				t.Fatal("metadata reset count")
			}
		case "Exhaustion":
			if d.ErrorCode != "DB_CONNECTION" || d.RetryCount == nil || *d.RetryCount != 3 {
				t.Fatal("exhaustion count")
			}
		case "Domain":
			want := "INSUFFICIENT_INVENTORY"
			if offset == 1 {
				want = "PRODUCT_MISSING"
			}
			if d.ErrorCode != want {
				t.Fatal("domain classification")
			}
		}
		letters = append(letters, d)
	}
	afterRows := inventoryRows(t, ctx, db)
	delete(afterRows, e.ProductID)
	if !reflect.DeepEqual(historical, afterRows) {
		t.Fatal("historical rows changed")
	}
	evidence["historicalInventoryUnchanged"] = true
	evidence["retryRecords"] = retries
	evidence["deadLetters"] = letters
	evidence["after"] = []snapshot{mainIn.snapshot(), retryIn.snapshot(), dlqIn.snapshot()}
	evidence["finalInventory"] = stock(t, ctx, db, e.ProductID)
	evidence["status"] = "PASS"
	evidence["stage"] = "complete"
	t.Logf("%s stock=%v retries=%d dlq=%d sourceCommitted=%d", name, evidence["finalInventory"], len(retries), len(letters), source.committed()[0])
}

func header(m kafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func savePhase3(t *testing.T, root string, run map[string]any) {
	t.Helper()
	var document struct {
		InitializationFailures []json.RawMessage `json:"initializationFailures,omitempty"`
		Regression             json.RawMessage   `json:"regression"`
		Scenarios              []json.RawMessage `json:"scenarios"`
	}
	path := filepath.Join(root, "docs/phase-3-evidence.json")
	if override := os.Getenv("PHASE3_EVIDENCE_PATH"); override != "" {
		path = override
	}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &document); err != nil {
			t.Error(err)
			return
		}
	} else if !os.IsNotExist(err) {
		t.Error(err)
		return
	}
	if b, err := os.ReadFile(filepath.Join(root, "bin/phase3-regression.json")); err == nil {
		document.Regression = b
	}
	b, err := json.Marshal(run)
	if err != nil {
		t.Error(err)
		return
	}
	document.Scenarios = append(document.Scenarios, b)
	b, err = json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if err = os.WriteFile(path, append(b, '\n'), 0644); err != nil {
		t.Error(err)
	}
}
