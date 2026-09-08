//go:build integration && phase2

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/mysql"
)

func TestPhase2A(t *testing.T) { scenario(t, "A") }
func TestPhase2B(t *testing.T) { scenario(t, "B") }
func TestPhase2C(t *testing.T) { scenario(t, "C") }
func TestPhase2D(t *testing.T) { scenario(t, "D") }

// All state oracles run outside the worker: independent SQL and broker APIs.
// Each run owns a fresh product/topic/group; no old rows or group offsets reset.
func scenario(t *testing.T, name string) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	db, err := mysql.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := inventoryRows(t, ctx, db)
	e, err := event.New(time.Now().UnixNano(), 2)
	if err != nil {
		t.Fatal(err)
	}
	topic := "phase2." + strings.ToLower(name) + "." + e.EventID
	group := "phase2-" + strings.ToLower(name) + "-" + e.EventID
	evidence := map[string]any{"scenario": name, "runId": e.EventID, "startedAt": time.Now().UTC(), "status": "FAIL", "topic": topic, "group": group, "initialInventory": 100, "offsetReset": false}
	var first, second *child
	defer func() {
		evidence["finishedAt"] = time.Now().UTC()
		if first != nil {
			evidence["workerPID"] = first.cmd.Process.Pid
			evidence["workerLog"] = first.log.text()
		}
		if second != nil {
			evidence["restartWorkerPID"] = second.cmd.Process.Pid
			evidence["restartWorkerLog"] = second.log.text()
		}
		if t.Failed() {
			evidence["status"] = "FAIL"
		}
		savePhase2(t, root, evidence)
	}()
	if _, err = db.ExecContext(ctx, "INSERT INTO inventory (product_id,available_quantity,updated_at) VALUES (?,100,UTC_TIMESTAMP(6))", e.ProductID); err != nil {
		t.Fatal(err)
	}
	brokers, err := config.Brokers()
	if err != nil {
		t.Fatal(err)
	}
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second, Transport: transport}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range created.Errors {
		if err != nil {
			t.Fatal(err)
		}
	}
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
	in := inspector{t: t, ctx: ctx, brokers: brokers, client: client, group: group, topic: topic, partitions: []int{0}}
	before := in.snapshot()
	evidence["before"] = before
	if before.End[0] != 0 || before.Committed[0] != -1 {
		t.Fatal("new topic/group not empty")
	}
	if name == "D" {
		e.SchemaVersion = 2
	}
	evidence["event"] = e
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	messages := []kafka.Message{{Key: []byte(e.OrderID), Value: value}}
	if name == "D" {
		tail, err := event.New(e.ProductID, 1)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(tail)
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, kafka.Message{Key: []byte(tail.OrderID), Value: payload})
		evidence["successor"] = tail
	}
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err = conn.SetRequiredAcks(-1); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	_, err = conn.WriteMessages(messages...)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	for offset, want := range messages {
		actual := in.read(0, int64(offset))
		if !bytes.Equal(actual.Value, want.Value) || !bytes.Equal(actual.Key, want.Key) {
			t.Fatal("record mismatch")
		}
	}
	evidence["partition"] = 0
	evidence["recordOffset"] = 0
	hash := fmt.Sprintf("%x", sha256.Sum256(value))
	evidence["valueSHA256"] = hash
	evidence["afterPublish"] = in.snapshot()
	binary := build(t, root, "worker")
	args := []string{"-topic", topic}
	point := map[string]string{"A": "before-db", "B": "before-db-commit", "C": "after-db-commit"}[name]
	if point != "" {
		args = append(args, "-fault-point", point, "-fault-event-id", e.EventID)
	}
	first = start(t, root, binary, []string{"KAFKA_CONSUMER_GROUP=" + group}, args...)
	if name == "A" {
		await(t, "before DB barrier", func() bool { return first.log.has("fault_reached") })
		// Recovery is registered before stopping MySQL, including failed-test paths.
		restored := false
		defer func() {
			if !restored {
				docker(t, root, "compose", "up", "-d", "--wait", "--wait-timeout", "120", "mysql")
			}
		}()
		docker(t, root, "compose", "stop", "mysql")
		id := strings.TrimSpace(docker(t, root, "compose", "ps", "-aq", "mysql"))
		state := strings.TrimSpace(docker(t, root, "inspect", "--format", "{{.State.Status}}", id))
		evidence["mysqlDuringFault"] = state
		if state != "exited" {
			t.Fatal("MySQL still running")
		}
		if _, err = io.WriteString(first.stdin, "continue\n"); err != nil {
			t.Fatal(err)
		}
		expectedExit(t, first, 1)
		evidence["exitCode"] = 1
		if !first.log.has("inventory_failed") {
			t.Fatal("no DB failure")
		}
		evidence["duringFault"] = in.snapshot()
		if in.committed()[0] != -1 {
			t.Fatal("outage committed record")
		}
		evidence["faultInventory"] = nil // SQL cannot run while MySQL is stopped.
		docker(t, root, "compose", "up", "-d", "--wait", "--wait-timeout", "120", "mysql")
		restored = true
		// The observer's old connection was also severed by the real outage.
		// Reconnect the external oracle; do not change worker failure behavior.
		_ = db.Close()
		db, err = mysql.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		evidence["restoredBeforeRestartInventory"] = stock(t, ctx, db, e.ProductID)
		if stock(t, ctx, db, e.ProductID) != 100 {
			t.Fatal("outage changed stock")
		}
	} else {
		code := map[string]int{"B": 86, "C": 87, "D": 1}[name]
		expectedExit(t, first, code)
		evidence["exitCode"] = code
		faultStock := stock(t, ctx, db, e.ProductID)
		evidence["faultInventory"] = faultStock
		expected := int64(100)
		if name == "C" {
			expected -= e.Quantity
		}
		if faultStock != expected {
			t.Fatalf("fault stock=%d expected=%d", faultStock, expected)
		}
		evidence["duringFault"] = in.snapshot()
		if in.committed()[0] != -1 {
			t.Fatal("crash/poison committed record")
		}
		if name != "D" && !first.log.has("fault_reached") {
			t.Fatal("hook not reached")
		}
	}
	second = start(t, root, binary, []string{"KAFKA_CONSUMER_GROUP=" + group}, "-topic", topic)
	expected := int64(100) - e.Quantity
	if name == "C" {
		expected -= e.Quantity
	}
	if name == "D" {
		expected = 100
		expectedExit(t, second, 1)
		evidence["restartExitCode"] = 1
		if second.log.count("record_fetched") != 1 || first.log.count("record_fetched") != 1 || second.log.has("event_received") {
			t.Fatal("poison successor progressed")
		}
		if in.ends()[0] != 2 || in.committed()[0] != -1 {
			t.Fatal("poison not blocking")
		}
		evidence["successorBlocked"] = true
	} else {
		await(t, "restart DB and offset commits", func() bool { return stock(t, ctx, db, e.ProductID) == expected && in.committed()[0] == 1 })
		second.stop(t)
		evidence["restartExitCode"] = 0
		if !first.log.ordered(e.EventID, "event_received") || !second.log.ordered(e.EventID, "event_received", "inventory_committed", "offset_committed") {
			t.Fatal("event identity/order mismatch")
		}
	}
	for _, worker := range []*child{first, second} {
		matched := false
		for _, entry := range worker.log.entries() {
			if entry["msg"] == "record_fetched" && entry["topic"] == topic && entry["partition"] == float64(0) && entry["offset"] == float64(0) && entry["valueSHA256"] == hash {
				matched = true
			}
		}
		if !matched {
			t.Fatal("same coordinate and bytes redelivery not proven")
		}
	}
	if first.cmd.Process.Pid == second.cmd.Process.Pid {
		t.Fatal("PID did not change")
	}
	final := stock(t, ctx, db, e.ProductID)
	if final != expected {
		t.Fatalf("final stock=%d expected=%d", final, expected)
	}
	afterRows := inventoryRows(t, ctx, db)
	delete(afterRows, e.ProductID)
	if !reflect.DeepEqual(old, afterRows) {
		t.Fatal("historical inventory changed")
	}
	evidence["historicalInventoryUnchanged"] = true
	evidence["finalInventory"] = final
	evidence["afterRestart"] = in.snapshot()
	evidence["status"] = "PASS"
	t.Logf("scenario=%s topic=%s partition=0 offset=0 stock=100->%v->%d committed=-1->%d PID=%d->%d", name, topic, evidence["faultInventory"], final, in.committed()[0], first.cmd.Process.Pid, second.cmd.Process.Pid)
}

func expectedExit(t *testing.T, c *child, want int) {
	t.Helper()
	select {
	case err := <-c.done:
		c.stopped = true
		_ = c.stdin.Close()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != want {
			t.Fatalf("exit=%v want=%d logs=%s", err, want, c.log.text())
		}
	case <-time.After(45 * time.Second):
		t.Fatal("expected worker exit timed out")
	}
}
func docker(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v %s", args, err, output)
	}
	return string(output)
}
func inventoryRows(t *testing.T, ctx context.Context, db *sql.DB) map[int64]int64 {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT product_id,available_quantity FROM inventory")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[int64]int64{}
	for rows.Next() {
		var id, qty int64
		if err := rows.Scan(&id, &qty); err != nil {
			t.Fatal(err)
		}
		result[id] = qty
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
func savePhase2(t *testing.T, root string, run map[string]any) {
	t.Helper()
	// RawMessage preserves 64-bit IDs from older evidence without float conversion.
	var document struct {
		Regression json.RawMessage   `json:"regression"`
		Scenarios  []json.RawMessage `json:"scenarios"`
	}
	path := filepath.Join(root, "docs/phase-2-evidence.json")
	existing, err := os.ReadFile(path)
	if err == nil {
		if err = json.Unmarshal(existing, &document); err != nil {
			t.Error(err)
			return
		}
	} else if !os.IsNotExist(err) {
		t.Error(err)
		return
	}
	regression, err := os.ReadFile(filepath.Join(root, "bin/phase2-regression.json"))
	if err == nil {
		document.Regression = regression
	}
	raw, err := json.Marshal(run)
	if err != nil {
		t.Error(err)
		return
	}
	document.Scenarios = append(document.Scenarios, raw)
	output, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if err = os.WriteFile(path, append(output, '\n'), 0644); err != nil {
		t.Error(err)
	}
}
