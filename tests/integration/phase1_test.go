//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/mysql"
)

type snapshot struct {
	End       map[int]int64 `json:"end"`
	Committed map[int]int64 `json:"committed"`
}
type observed struct {
	Event     event.OrderCreated `json:"event"`
	Partition int                `json:"partition"`
	Offset    int64              `json:"offset"`
	Stock     int64              `json:"stock"`
	Committed int64              `json:"committedOffset"`
}
type inspector struct {
	t          *testing.T
	ctx        context.Context
	brokers    []string
	client     *kafka.Client
	group      string
	partitions []int
}

func TestPhase1Flow(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	brokers, err := config.Brokers()
	if err != nil {
		t.Fatal(err)
	}
	db, err := mysql.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	in := inspector{t: t, ctx: ctx, brokers: brokers, group: config.ConsumerGroup(), client: &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second, Transport: transport}}
	// Group APIs target the discovered coordinator. Discovery also initializes
	// Kafka's internal group metadata before inspecting a never-used group.
	await(t, "group coordinator", func() bool {
		response, err := in.client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{Addr: kafka.TCP(brokers...), Key: in.group, KeyType: kafka.CoordinatorKeyTypeConsumer})
		if err != nil {
			t.Fatal(err)
		}
		if response.Error != nil {
			if errors.Is(response.Error, kafka.GroupCoordinatorNotAvailable) {
				return false
			}
			t.Fatal(response.Error)
		}
		in.client.Addr = kafka.TCP(net.JoinHostPort(response.Coordinator.Host, strconv.Itoa(response.Coordinator.Port)))
		return true
	})
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	parts, err := conn.ReadPartitions(event.OrdersTopic)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range parts {
		in.partitions = append(in.partitions, p.ID)
	}
	if len(in.partitions) != 6 {
		t.Fatalf("expected 6 partitions, got %d", len(in.partitions))
	}
	if in.members() != 0 {
		t.Fatal("stop existing group consumers before integration verification")
	}
	before := in.snapshot()
	for p, end := range before.End {
		committed := before.Committed[p]
		if committed == -1 && end == 0 {
			continue
		}
		if committed != end {
			t.Fatalf("pending partition %d: committed=%d end=%d; refusing seed/reset", p, committed, end)
		}
	}
	// Reuse the exact documented schema and seed SQL, after checking that the
	// group has no active members or unfinished records. Never reset offsets.
	for _, file := range []string{"migrations/001_inventory.sql", "scripts/seed-inventory.sql"} {
		value, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, string(value)); err != nil {
			t.Fatal(err)
		}
	}
	if got := stock(t, ctx, db); got != 100 {
		t.Fatalf("seed=%d", got)
	}
	var engine string
	if err := db.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='inventory'").Scan(&engine); err != nil || engine != "InnoDB" {
		t.Fatalf("engine=%s error=%v", engine, err)
	}
	// Check the database constraint within a transaction that is always rolled
	// back. This is schema validation, not a Kafka fault-injection feature.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, constraintErr := tx.ExecContext(ctx, "UPDATE inventory SET available_quantity=-1 WHERE product_id=1")
	_ = tx.Rollback()
	if constraintErr == nil {
		t.Fatal("negative stock was allowed by schema")
	}

	apiBin, workerBin := build(t, root, "api"), build(t, root, "worker")
	api := start(t, root, apiBin, []string{"API_ADDR=127.0.0.1:0"})
	await(t, "API ready", func() bool { return api.log.has("api_started") })
	url := "http://" + api.log.field("api_started", "address") + "/orders"
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	var records []observed
	current := before
	wantedStock := int64(100)
	var worker *child
	for index, quantity := range []int64{1, 3, 1, 2, 3, 4, 5} {
		response := post(t, client, url, fmt.Sprintf(`{"productId":1,"quantity":%d}`, quantity), 202)
		await(t, "exactly one published record", func() bool {
			ends := in.ends()
			return sum(ends)-sum(current.End) == 1
		})
		next := in.snapshot()
		var message kafka.Message
		found := false
		for partition, end := range next.End {
			if end == current.End[partition]+1 {
				message = in.read(partition, current.End[partition])
				found = true
			}
		}
		if !found {
			t.Fatal("published record coordinate not found")
		}
		e, err := event.Decode(message.Value)
		if err != nil || e.EventID != response["eventId"] || e.OrderID != response["orderId"] || e.ProductID != 1 || e.Quantity != quantity || string(message.Key) != e.OrderID {
			t.Fatalf("HTTP/Kafka event mismatch: %v", err)
		}
		if index == 0 {
			if stock(t, ctx, db) != 100 || !reflect.DeepEqual(before.Committed, next.Committed) {
				t.Fatal("stock/offset changed before worker start")
			}
			worker = start(t, root, workerBin, nil)
			await(t, "consumer group assignment", func() bool { return in.members() == 1 })
		}
		wantedStock -= quantity
		await(t, "DB and broker commit", func() bool {
			return stock(t, ctx, db) == wantedStock && in.committed()[message.Partition] == message.Offset+1
		})
		// The actual worker log sequence is additional ordering evidence; broker
		// OffsetFetch and independent SQL reads above are the state oracle.
		await(t, "worker commit log", func() bool {
			return worker.log.ordered(e.EventID, "event_received", "inventory_committed", "offset_committed")
		})
		records = append(records, observed{e, message.Partition, message.Offset, wantedStock, message.Offset + 1})
		t.Logf("event=%s order=%s product=1 quantity=%d partition=%d recordOffset=%d committed=%d stock=%d", e.EventID, e.OrderID, quantity, message.Partition, message.Offset, message.Offset+1, wantedStock)
		current = in.snapshot()
	}
	validEnd := in.snapshot()
	invalid := []string{`{"productId":1,"quantity":0}`, `{"quantity":1}`, `{"productId":1}`, `{"productId":1,"quantity":1} {}`, `{"productId":1,"quantity":1.5}`}
	for _, input := range invalid {
		post(t, client, url, input, 400)
	}
	if got := in.snapshot(); !reflect.DeepEqual(got, validEnd) || stock(t, ctx, db) != 81 {
		t.Fatal("invalid HTTP changed Kafka offsets or inventory")
	}
	worker.stop(t)
	await(t, "old worker left group", func() bool { return in.members() == 0 })
	restarted := start(t, root, workerBin, nil)
	if restarted.cmd.Process.Pid == worker.cmd.Process.Pid {
		t.Fatal("worker process did not change")
	}
	await(t, "new worker owns partitions", func() bool { return in.members() == 1 })
	// Wait after a confirmed new group assignment, not merely process startup.
	// MaxWait is 1 second; this observes five fetch intervals with no new data.
	time.Sleep(5 * time.Second)
	afterRestart := in.snapshot()
	if stock(t, ctx, db) != 81 || !reflect.DeepEqual(afterRestart, validEnd) || restarted.log.has("event_received") {
		t.Fatal("committed normal messages were processed again after restart")
	}
	restarted.stop(t)
	api.stop(t)
	if worker.log.count("inventory_committed") != 7 || worker.log.count("offset_committed") != 7 || api.log.count("order_published") != 7 {
		t.Fatal("unexpected processing/publish log count")
	}
	evidence := map[string]any{
		"verifiedAt": time.Now().UTC(), "group": in.group,
		"initialStock": 100, "finalStock": 81, "quantitySum": 19,
		"acceptedHTTP": 7, "rejectedHTTP": len(invalid), "records": records,
		"before": before, "after": validEnd, "afterRestart": afterRestart,
		"workerPID": worker.cmd.Process.Pid, "restartedWorkerPID": restarted.cmd.Process.Pid,
		"restartObservationSeconds": 5, "restartedProcessed": restarted.log.count("event_received"),
		"apiLog": api.log.text(), "workerLog": worker.log.text(), "restartedWorkerLog": restarted.log.text(),
	}
	value, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs/phase-1-evidence.json"), append(value, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("initial=100 quantity_sum=19 final=81 accepted=7 rejected=%d before=%+v after=%+v workerPID=%d restartedPID=%d restarted_processed=0", len(invalid), before, validEnd, worker.cmd.Process.Pid, restarted.cmd.Process.Pid)
}

func stock(t *testing.T, ctx context.Context, db *sql.DB) int64 {
	t.Helper()
	var quantity int64
	if err := db.QueryRowContext(ctx, "SELECT available_quantity FROM inventory WHERE product_id=1").Scan(&quantity); err != nil {
		t.Fatal(err)
	}
	return quantity
}
func sum(values map[int]int64) int64 {
	var n int64
	for _, v := range values {
		n += v
	}
	return n
}
func await(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func post(t *testing.T, client *http.Client, url, body string, status int) map[string]string {
	t.Helper()
	r, err := client.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var result map[string]string
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != status {
		t.Fatalf("HTTP status=%d expected=%d response=%v", r.StatusCode, status, result)
	}
	return result
}
func (i inspector) ends() map[int]int64 {
	result := map[int]int64{}
	for _, partition := range i.partitions {
		conn, err := kafka.DialLeader(i.ctx, "tcp", i.brokers[0], event.OrdersTopic, partition)
		if err != nil {
			i.t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		end, err := conn.ReadLastOffset()
		conn.Close()
		if err != nil {
			i.t.Fatal(err)
		}
		result[partition] = end
	}
	return result
}
func (i inspector) committed() map[int]int64 {
	response, err := i.client.OffsetFetch(i.ctx, &kafka.OffsetFetchRequest{GroupID: i.group, Topics: map[string][]int{event.OrdersTopic: i.partitions}})
	if err != nil {
		i.t.Fatal(err)
	}
	if response.Error != nil && !errors.Is(response.Error, kafka.GroupIdNotFound) {
		i.t.Fatal(response.Error)
	}
	result := map[int]int64{}
	for _, p := range i.partitions {
		result[p] = -1
	}
	for _, p := range response.Topics[event.OrdersTopic] {
		if p.Error != nil {
			i.t.Fatal(p.Error)
		}
		result[p.Partition] = p.CommittedOffset
	}
	return result
}
func (i inspector) snapshot() snapshot { return snapshot{i.ends(), i.committed()} }
func (i inspector) members() int {
	response, err := i.client.DescribeGroups(i.ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{i.group}})
	if err != nil {
		i.t.Fatal(err)
	}
	if len(response.Groups) != 1 {
		i.t.Fatal("missing group description")
	}
	g := response.Groups[0]
	if g.Error != nil && !errors.Is(g.Error, kafka.GroupIdNotFound) {
		i.t.Fatal(g.Error)
	}
	if len(g.Members) > 0 && g.GroupState != "Stable" {
		return -1
	}
	return len(g.Members)
}
func (i inspector) read(partition int, offset int64) kafka.Message {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: i.brokers, Topic: event.OrdersTopic, Partition: partition, MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second})
	defer r.Close()
	if err := r.SetOffset(offset); err != nil {
		i.t.Fatal(err)
	}
	m, err := r.ReadMessage(i.ctx)
	if err != nil {
		i.t.Fatal(err)
	}
	if m.Offset != offset {
		i.t.Fatal("wrong record offset")
	}
	return m
}

type logBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *logBuffer) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buffer.String() }
func (b *logBuffer) entries() []map[string]any {
	var entries []map[string]any
	for _, line := range strings.Split(b.text(), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) == nil {
			entries = append(entries, entry)
		}
	}
	return entries
}
func (b *logBuffer) count(message string) int {
	count := 0
	for _, e := range b.entries() {
		if e["msg"] == message {
			count++
		}
	}
	return count
}
func (b *logBuffer) has(message string) bool { return b.count(message) > 0 }
func (b *logBuffer) field(message, key string) string {
	for _, e := range b.entries() {
		if e["msg"] == message {
			v, _ := e[key].(string)
			return v
		}
	}
	return ""
}
func (b *logBuffer) ordered(id string, messages ...string) bool {
	next := 0
	for _, e := range b.entries() {
		if e["eventId"] == id && next < len(messages) && e["msg"] == messages[next] {
			next++
		}
	}
	return next == len(messages)
}

type child struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	log     *logBuffer
	done    chan error
	stopped bool
}

func build(t *testing.T, root, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", path, "./cmd/"+name)
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	return path
}
func start(t *testing.T, root, binary string, overrides []string) *child {
	t.Helper()
	c := &child{cmd: exec.Command(binary, "-shutdown-on-stdin-close"), log: &logBuffer{}, done: make(chan error, 1)}
	c.cmd.Dir = root
	c.cmd.Env = append(os.Environ(), overrides...)
	c.cmd.Stdout, c.cmd.Stderr = c.log, c.log
	var err error
	c.stdin, err = c.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { c.done <- c.cmd.Wait() }()
	t.Cleanup(func() {
		c.stop(t)
		if t.Failed() {
			t.Log(c.log.text())
		}
	})
	return c
}
func (c *child) stop(t *testing.T) {
	t.Helper()
	if c.stopped {
		return
	}
	c.stopped = true
	_ = c.stdin.Close()
	select {
	case err := <-c.done:
		if err != nil {
			t.Errorf("child exit: %v logs=%s", err, c.log.text())
		}
	case <-time.After(20 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
		t.Errorf("child did not stop gracefully: %s", c.log.text())
	}
}
