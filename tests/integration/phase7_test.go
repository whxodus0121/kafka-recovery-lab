//go:build integration && phase2 && phase3 && phase4 && phase5 && phase6 && phase7

package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/inventory"
	"kafka-recovery-lab/internal/mysql"
)

const phase7Input = 120

type phase7Fixture struct {
	t                 *testing.T
	root, worker, cli string
	ctx               context.Context
	cancel            context.CancelFunc
	db                *sql.DB
	brokers           []string
	client            *kafka.Client
	runID             string
	topics            map[string]string
	recovery          inspector
	dlq               inspector
	retry             inspector
	workers           []*child
	raw               map[string]any
	products          []int64
	events            []event.OrderCreated
}

type phase7Command struct {
	cmd        *exec.Cmd
	log        *logBuffer
	done       chan struct{}
	err        error
	startedAt  time.Time
	finishedAt time.Time
}

func TestPhase7(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	worker, cli := build(t, root, "worker"), build(t, root, "replay")
	for _, name := range []string{"unlimited", "publication-limited", "recovery-limited-backlog", "bulk-duplicate", "single-regression", "partial-failure"} {
		t.Run(name, func(t *testing.T) {
			f := newPhase7(t, root, worker, cli, name)
			defer f.close()
			switch name {
			case "unlimited":
				f.comparison(0, 0, false)
			case "publication-limited":
				f.comparison(20, 0, false)
			case "recovery-limited-backlog":
				f.comparison(0, 20, true)
			case "bulk-duplicate":
				f.bulkDuplicate()
			case "single-regression":
				f.singleRegression()
			case "partial-failure":
				f.partialFailure()
			}
			f.raw["status"] = "PASS"
		})
	}
}

func newPhase7(t *testing.T, root, worker, cli, strategy string) *phase7Fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	db, err := mysql.Open(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	brokers, err := config.Brokers()
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	id, err := event.New(time.Now().UnixNano(), 1)
	if err != nil {
		t.Fatal(err)
	}
	runID := "phase7." + id.EventID
	topics := map[string]string{"dlq": runID + ".dlq", "recovery": runID + ".recovery", "retry": runID + ".retry"}
	transport := &kafka.Transport{}
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 30 * time.Second, Transport: transport}
	configs := []kafka.TopicConfig{}
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		t.Fatal(err)
	}
	for _, createErr := range created.Errors {
		if createErr != nil {
			t.Fatal(createErr)
		}
	}
	for _, topic := range topics {
		await(t, "phase7 topic leader", func() bool {
			conn, dialErr := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
			if dialErr == nil {
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				_, dialErr = conn.ReadLastOffset()
				conn.Close()
			}
			if errors.Is(dialErr, kafka.NotLeaderForPartition) || errors.Is(dialErr, kafka.LeaderNotAvailable) {
				return false
			}
			if dialErr != nil {
				t.Fatal(dialErr)
			}
			return true
		})
	}
	f := &phase7Fixture{t: t, root: root, worker: worker, cli: cli, ctx: ctx, cancel: cancel, db: db, brokers: brokers, client: client, runID: runID, topics: topics, raw: map[string]any{"runId": runID, "strategy": strategy, "status": "FAIL", "startedAt": time.Now().UTC(), "offsetReset": false}}
	f.recovery = f.newInspector("recovery", topics["recovery"])
	f.dlq = f.newInspector("dlq-observer", topics["dlq"])
	f.retry = f.newInspector("retry-observer", topics["retry"])
	return f
}

func (f *phase7Fixture) close() {
	for _, worker := range f.workers {
		if !worker.stopped {
			worker.stop(f.t)
		}
	}
	f.raw["finishedAt"] = time.Now().UTC()
	f.raw["topics"] = f.topics
	savePhase7Raw(f.t, f.root, f.raw)
	f.client.Transport.(*kafka.Transport).CloseIdleConnections()
	f.db.Close()
	f.cancel()
}

func (f *phase7Fixture) newInspector(role, topic string) inspector {
	group := f.runID + "." + role
	client := &kafka.Client{Addr: kafka.TCP(f.brokers...), Timeout: 5 * time.Second}
	await(f.t, "phase7 coordinator", func() bool {
		response, err := client.FindCoordinator(f.ctx, &kafka.FindCoordinatorRequest{Addr: kafka.TCP(f.brokers...), Key: group, KeyType: kafka.CoordinatorKeyTypeConsumer})
		if err != nil {
			f.t.Fatal(err)
		}
		if errors.Is(response.Error, kafka.GroupCoordinatorNotAvailable) {
			return false
		}
		if response.Error != nil {
			f.t.Fatal(response.Error)
		}
		client.Addr = kafka.TCP(net.JoinHostPort(response.Coordinator.Host, strconv.Itoa(response.Coordinator.Port)))
		return true
	})
	return inspector{t: f.t, ctx: f.ctx, brokers: f.brokers, client: client, group: group, topic: topic, partitions: []int{0}}
}

func (f *phase7Fixture) seed(count int) {
	f.t.Helper()
	base := time.Now().UnixNano()
	for i := 0; i < 12; i++ {
		product := base + int64(i)
		if _, err := f.db.ExecContext(f.ctx, "INSERT INTO inventory (product_id,available_quantity,updated_at) VALUES (?,1000,UTC_TIMESTAMP(6))", product); err != nil {
			f.t.Fatal(err)
		}
		f.products = append(f.products, product)
	}
	messages := make([]kafka.Message, 0, count)
	for i := 0; i < count; i++ {
		e, err := event.New(f.products[i%len(f.products)], 1)
		if err != nil {
			f.t.Fatal(err)
		}
		f.events = append(f.events, e)
		value, _ := json.Marshal(e)
		retries := 3
		d := inventory.DeadLetter{ID: fmt.Sprintf("fixture-%s", e.EventID), Key: []byte(e.OrderID), Value: value, Topic: event.OrdersTopic, Partition: 0, Offset: int64(i), RetryCount: &retries, ErrorCode: "DB_CONNECTION", ErrorMessage: "fixture for replay load experiment", FailedAt: time.Now().UTC()}
		payload, _ := json.Marshal(d)
		messages = append(messages, kafka.Message{Key: []byte(e.OrderID), Value: payload})
	}
	f.publishDLQ(messages...)
	if f.dlq.ends()[0] != int64(count) {
		f.t.Fatal("DLQ fixture count mismatch")
	}
}

func (f *phase7Fixture) publishDLQ(messages ...kafka.Message) {
	f.t.Helper()
	conn, err := kafka.DialLeader(f.ctx, "tcp", f.brokers[0], f.topics["dlq"], 0)
	if err != nil {
		f.t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if err := conn.SetRequiredAcks(-1); err != nil {
		f.t.Fatal(err)
	}
	if _, err := conn.WriteMessages(messages...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *phase7Fixture) startRecovery(rate float64) *child {
	args := []string{"-recovery-worker", "-recovery-topic", f.topics["recovery"], "-retry-topic", f.topics["retry"], "-dlq-topic", f.topics["dlq"], "-recovery-rate", strconv.FormatFloat(rate, 'f', -1, 64)}
	w := start(f.t, f.root, f.worker, []string{"KAFKA_CONSUMER_GROUP=" + f.recovery.group}, args...)
	f.workers = append(f.workers, w)
	await(f.t, "recovery worker start", func() bool { return w.log.has("worker_started") })
	return w
}

func (f *phase7Fixture) startReplay(startOffset, limit int64, rate float64, single bool) *phase7Command {
	args := []string{"-dlq-topic", f.topics["dlq"], "-partition", "0", "-recovery-topic", f.topics["recovery"], "-publish-rate", strconv.FormatFloat(rate, 'f', -1, 64)}
	if single {
		args = append(args, "-offset", strconv.FormatInt(startOffset, 10))
	} else {
		args = append(args, "-start-offset", strconv.FormatInt(startOffset, 10), "-limit", strconv.FormatInt(limit, 10))
	}
	r := &phase7Command{cmd: exec.Command(f.cli, args...), log: &logBuffer{}, done: make(chan struct{}), startedAt: time.Now().UTC()}
	r.cmd.Dir, r.cmd.Env, r.cmd.Stdout, r.cmd.Stderr = f.root, os.Environ(), r.log, r.log
	if err := r.cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() {
		r.err = r.cmd.Wait()
		r.finishedAt = time.Now().UTC()
		close(r.done)
	}()
	return r
}

func (r *phase7Command) wait(t *testing.T, wantSuccess bool) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(90 * time.Second):
		_ = r.cmd.Process.Kill()
		<-r.done
		t.Fatal("replay CLI timeout")
	}
	if wantSuccess != (r.err == nil) {
		t.Fatalf("replay CLI success=%v error=%v log=%s", wantSuccess, r.err, r.log.text())
	}
}

func (f *phase7Fixture) comparison(publishRate, recoveryRate float64, backlog bool) {
	f.seed(phase7Input)
	initialInventory := int64(len(f.products) * 1000)
	var worker *child
	if !backlog {
		worker = f.startRecovery(recoveryRate)
	}
	cli := f.startReplay(0, phase7Input, publishRate, false)
	peakLag := int64(0)
	if backlog {
		cli.wait(f.t, true)
		if end := f.recovery.ends()[0]; end != phase7Input || f.recovery.committed()[0] != -1 {
			f.t.Fatal("Recovery backlog not established", end, f.recovery.committed()[0])
		}
		peakLag = phase7Input
		worker = f.startRecovery(recoveryRate)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case <-cli.done:
			if cli.err != nil {
				f.t.Fatalf("replay CLI failed: %v log=%s", cli.err, cli.log.text())
			}
		default:
		}
		end, committed := f.recovery.ends()[0], f.recovery.committed()[0]
		base := committed
		if base < 0 {
			base = 0
		}
		if lag := end - base; lag > peakLag {
			peakLag = lag
		}
		if committed == phase7Input {
			break
		}
		if time.Now().After(deadline) {
			f.t.Fatal("business recovery timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cli.wait(f.t, true)
	businessCompleted := time.Now().UTC()
	worker.stop(f.t)
	published := logTimes(f.t, cli.log, "replay_published", "publishedAt")
	processedTimes := logTimes(f.t, worker.log, "recovery_processing_started", "processingStartedAt")
	if len(published) != phase7Input || len(processedTimes) != phase7Input || worker.log.count("inventory_committed") != phase7Input {
		f.t.Fatal("measurement count mismatch", len(published), len(processedTimes), worker.log.count("inventory_committed"))
	}
	finalInventory, markerCount := f.databaseState()
	dlqCount := int(f.dlq.ends()[0]) - phase7Input
	success, unfinished := markerCount, phase7Input-markerCount-dlqCount
	if finalInventory != initialInventory-phase7Input || markerCount != phase7Input || dlqCount != 0 || unfinished != 0 || success+dlqCount+unfinished != phase7Input || f.retry.ends()[0] != 0 {
		f.t.Fatal("reconciliation failed", finalInventory, markerCount, dlqCount, unfinished)
	}
	publicationWindow := elapsedWindow(published)
	processingWindow := elapsedWindow(processedTimes)
	if publishRate > 0 && (publicationWindow < 5700*time.Millisecond || averageRPS(published) > 21 || peakRPS(published) > 22) {
		f.t.Fatal("publication pacing exceeded tolerance", publicationWindow, averageRPS(published), peakRPS(published))
	}
	if recoveryRate > 0 && (processingWindow < 5700*time.Millisecond || averageRPS(processedTimes) > 21 || peakRPS(processedTimes) > 22) {
		f.t.Fatal("recovery pacing exceeded tolerance", processingWindow, averageRPS(processedTimes), peakRPS(processedTimes))
	}
	f.raw["inputCount"] = phase7Input
	f.raw["productCount"] = len(f.products)
	f.raw["publishRate"] = publishRate
	f.raw["recoveryRate"] = recoveryRate
	f.raw["dlqStartOffset"] = 0
	f.raw["dlqEndOffset"] = phase7Input
	f.raw["recoveryEndOffset"] = f.recovery.ends()[0]
	f.raw["recoveryCommittedOffset"] = f.recovery.committed()[0]
	f.raw["publicationTimestamps"] = published
	f.raw["firstPublicationTimestamp"] = published[0]
	f.raw["lastPublicationTimestamp"] = published[len(published)-1]
	f.raw["publicationAttemptsPerSecond"] = perSecond(published)
	f.raw["publicationDurationMs"] = publicationWindow.Milliseconds()
	f.raw["cliDurationMs"] = cli.finishedAt.Sub(cli.startedAt).Milliseconds()
	f.raw["publicationRPS"] = averageRPS(published)
	f.raw["publicationPeakRPS"] = peakRPS(published)
	f.raw["processingTimestamps"] = processedTimes
	f.raw["firstProcessingStartTimestamp"] = processedTimes[0]
	f.raw["lastProcessingStartTimestamp"] = processedTimes[len(processedTimes)-1]
	f.raw["processingAttemptsPerSecond"] = perSecond(processedTimes)
	f.raw["processingDurationMs"] = processingWindow.Milliseconds()
	f.raw["processingRPS"] = averageRPS(processedTimes)
	f.raw["processingPeakRPS"] = peakRPS(processedTimes)
	f.raw["recoveryPeakLag"] = peakLag
	f.raw["businessRecoveryDurationMs"] = businessCompleted.Sub(cli.startedAt).Milliseconds()
	f.raw["success"] = success
	f.raw["dlq"] = dlqCount
	f.raw["unfinished"] = unfinished
	f.raw["initialInventory"] = initialInventory
	f.raw["finalInventory"] = finalInventory
	f.raw["processedEvents"] = markerCount
	f.raw["duplicate"] = 0
	f.raw["workerPID"] = worker.cmd.Process.Pid
	f.raw["cliPID"] = cli.cmd.Process.Pid
	f.raw["cliLog"] = cli.log.text()
	f.raw["workerLog"] = worker.log.text()
}

func (f *phase7Fixture) bulkDuplicate() {
	const count = 24
	f.seed(count)
	initialInventory := int64(len(f.products) * 1000)
	worker := f.startRecovery(0)
	first := f.startReplay(0, count, 0, false)
	first.wait(f.t, true)
	await(f.t, "first bulk recovery", func() bool { return f.recovery.committed()[0] == count })
	firstInventory, firstMarkers := f.databaseState()
	second := f.startReplay(0, count, 0, false)
	second.wait(f.t, true)
	await(f.t, "duplicate bulk recovery", func() bool { return f.recovery.committed()[0] == count*2 })
	worker.stop(f.t)
	finalInventory, finalMarkers := f.databaseState()
	if firstInventory != initialInventory-count || finalInventory != firstInventory || firstMarkers != count || finalMarkers != count || worker.log.count("inventory_committed") != count || worker.log.count("inventory_duplicate") != count || f.dlq.ends()[0] != count || f.retry.ends()[0] != 0 {
		f.t.Fatal("bulk duplicate idempotency mismatch")
	}
	f.raw["inputCount"] = count
	f.raw["firstReplay"] = map[string]any{"inventory": firstInventory, "processedEvents": firstMarkers, "recoveryCommittedOffset": count, "cliPID": first.cmd.Process.Pid}
	f.raw["secondReplay"] = map[string]any{"inventory": finalInventory, "processedEvents": finalMarkers, "recoveryCommittedOffset": count * 2, "cliPID": second.cmd.Process.Pid}
	f.raw["newEffects"] = count
	f.raw["duplicates"] = count
	f.raw["additionalInventoryChange"] = 0
	f.raw["workerPID"] = worker.cmd.Process.Pid
}

func (f *phase7Fixture) singleRegression() {
	f.seed(1)
	worker := f.startRecovery(0)
	cli := f.startReplay(0, 1, 0, true)
	cli.wait(f.t, true)
	await(f.t, "single replay recovery", func() bool { return f.recovery.committed()[0] == 1 })
	worker.stop(f.t)
	finalInventory, markers := f.databaseState()
	if finalInventory != int64(len(f.products)*1000-1) || markers != 1 || f.recovery.ends()[0] != 1 || f.dlq.ends()[0] != 1 {
		f.t.Fatal("Phase 6 single replay regression")
	}
	f.raw["inputCount"] = 1
	f.raw["finalInventory"] = finalInventory
	f.raw["processedEvents"] = markers
	f.raw["recoveryEndOffset"] = 1
	f.raw["recoveryCommittedOffset"] = 1
	f.raw["cliPID"] = cli.cmd.Process.Pid
	f.raw["workerPID"] = worker.cmd.Process.Pid
}

func (f *phase7Fixture) partialFailure() {
	f.seed(0)
	first, _ := event.New(f.products[0], 1)
	second, _ := event.New(f.products[1], 1)
	f.events = append(f.events, first, second)
	valid := func(e event.OrderCreated, offset int64) kafka.Message {
		value, _ := json.Marshal(e)
		count := 3
		d := inventory.DeadLetter{ID: "fixture-" + e.EventID, Key: []byte(e.OrderID), Value: value, Topic: event.OrdersTopic, Partition: 0, Offset: offset, RetryCount: &count, ErrorCode: "DB_CONNECTION", ErrorMessage: "fixture", FailedAt: time.Now().UTC()}
		payload, _ := json.Marshal(d)
		return kafka.Message{Key: []byte(e.OrderID), Value: payload}
	}
	f.publishDLQ(valid(first, 0), kafka.Message{Key: []byte("malformed"), Value: []byte("{bad")}, valid(second, 2))
	cli := f.startReplay(0, 3, 0, false)
	cli.wait(f.t, false)
	if f.recovery.ends()[0] != 1 || f.dlq.ends()[0] != 3 || f.dlq.committed()[0] != -1 || !strings.Contains(cli.log.text(), "/0/1") || !strings.Contains(cli.log.text(), "published=1") {
		f.t.Fatal("partial failure boundary mismatch", cli.log.text())
	}
	f.raw["inputCount"] = 3
	f.raw["publishedBeforeFailure"] = 1
	f.raw["failedDLQOffset"] = 1
	f.raw["recoveryEndOffset"] = 1
	f.raw["dlqEndOffset"] = 3
	f.raw["dlqCommittedOffset"] = -1
	f.raw["cliPID"] = cli.cmd.Process.Pid
	f.raw["cliLog"] = cli.log.text()
}

func (f *phase7Fixture) databaseState() (int64, int) {
	f.t.Helper()
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(f.products)), ",")
	args := make([]any, len(f.products))
	for i, product := range f.products {
		args[i] = product
	}
	var inventoryTotal int64
	if err := f.db.QueryRowContext(f.ctx, "SELECT COALESCE(SUM(available_quantity),0) FROM inventory WHERE product_id IN ("+placeholders+")", args...).Scan(&inventoryTotal); err != nil {
		f.t.Fatal(err)
	}
	var markerCount int
	if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM processed_events WHERE product_id IN ("+placeholders+")", args...).Scan(&markerCount); err != nil {
		f.t.Fatal(err)
	}
	return inventoryTotal, markerCount
}

func logTimes(t *testing.T, log *logBuffer, message, field string) []time.Time {
	t.Helper()
	result := []time.Time{}
	for _, entry := range log.entries() {
		if entry["msg"] != message {
			continue
		}
		value, ok := entry[field].(string)
		if !ok {
			t.Fatal("missing timestamp", message, field)
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, parsed)
	}
	return result
}

func elapsedWindow(values []time.Time) time.Duration {
	if len(values) < 2 {
		return 0
	}
	return values[len(values)-1].Sub(values[0])
}

func averageRPS(values []time.Time) float64 {
	window := elapsedWindow(values)
	if len(values) < 2 || window <= 0 {
		return 0
	}
	return float64(len(values)-1) / window.Seconds()
}

func peakRPS(values []time.Time) int {
	peak, right := 0, 0
	for left := range values {
		if right < left {
			right = left
		}
		for right < len(values) && values[right].Sub(values[left]) < time.Second {
			right++
		}
		if count := right - left; count > peak {
			peak = count
		}
	}
	return peak
}

func perSecond(values []time.Time) map[string]int {
	result := map[string]int{}
	for _, value := range values {
		result[value.UTC().Truncate(time.Second).Format(time.RFC3339)]++
	}
	return result
}

func savePhase7Raw(t *testing.T, root string, raw map[string]any) {
	t.Helper()
	dir := filepath.Join(root, "experiments/phase7")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Error(err)
		return
	}
	strategy, _ := raw["strategy"].(string)
	runID, _ := raw["runId"].(string)
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, strategy+"-"+runID+".json"), append(data, '\n'), 0644); err != nil {
		t.Error(err)
	}
}

func TestPhase7Audit(t *testing.T) {
	root, _ := filepath.Abs("../..")
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(module), "golang.org/x/time") {
		t.Fatal("external rate limiter dependency added")
	}
	for _, name := range []string{"003_replay_runs.sql", "003_replay_partitions.sql"} {
		if _, err := os.Stat(filepath.Join(root, "migrations", name)); !os.IsNotExist(err) {
			t.Fatal("persistent replay state added", name, err)
		}
	}
	paths, err := filepath.Glob(filepath.Join(root, "experiments/phase7/*.json"))
	if err != nil {
		t.Fatal(err)
	}
	latest := map[string]map[string]any{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		raw["sourceFile"] = filepath.ToSlash(relative)
		raw["sourceSHA256"] = fmt.Sprintf("%x", sha256.Sum256(data))
		strategy, _ := raw["strategy"].(string)
		if raw["status"] != "PASS" {
			continue
		}
		if previous, exists := latest[strategy]; !exists || fmt.Sprint(raw["startedAt"]) > fmt.Sprint(previous["startedAt"]) {
			latest[strategy] = raw
		}
	}
	required := []string{"unlimited", "publication-limited", "recovery-limited-backlog", "bulk-duplicate", "single-regression", "partial-failure"}
	for _, strategy := range required {
		if latest[strategy] == nil {
			t.Fatal("missing passing Phase 7 raw", strategy)
		}
	}
	comparison := []map[string]any{latest["unlimited"], latest["publication-limited"], latest["recovery-limited-backlog"]}
	for _, raw := range comparison {
		if int(raw["success"].(float64)+raw["dlq"].(float64)+raw["unfinished"].(float64)) != phase7Input {
			t.Fatal("raw reconciliation failed")
		}
	}
	document := map[string]any{"generatedAt": time.Now().UTC(), "status": "PASS", "inputCountPerComparison": phase7Input, "comparison": comparison, "bulkDuplicate": latest["bulk-duplicate"], "singleReplayRegression": latest["single-regression"], "partialFailure": latest["partial-failure"], "phase4PerformanceRerun": false, "phase8Implemented": false}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs/phase-7-evidence.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
