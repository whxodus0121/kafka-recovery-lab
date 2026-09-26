//go:build integration && phase2 && phase3 && phase4 && phase5 && phase6

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

type phase6Fixture struct {
	t                      *testing.T
	root, worker, replayID string
	ctx                    context.Context
	cancel                 context.CancelFunc
	db                     *sql.DB
	brokers                []string
	client                 *kafka.Client
	topics                 map[string]string
	inspect                map[string]inspector
	workers                []*child
	evidence               map[string]any
}

func TestPhase6(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	worker, replay := build(t, root, "worker"), build(t, root, "replay")
	for _, name := range []string{"ExhaustionRecoveryTwice", "RecoveryRetry", "RecoveryDomainDLQ", "ReplayFailures"} {
		t.Run(name, func(t *testing.T) {
			f := newPhase6(t, root, worker, replay, name)
			defer f.close()
			switch name {
			case "ExhaustionRecoveryTwice":
				f.exhaustionRecoveryTwice()
			case "RecoveryRetry":
				f.recoveryRetry()
			case "RecoveryDomainDLQ":
				f.recoveryDomainDLQ()
			case "ReplayFailures":
				f.replayFailures()
			}
			f.evidence["status"] = "PASS"
		})
	}
}

func newPhase6(t *testing.T, root, worker, replay, name string) *phase6Fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	e, err := event.New(time.Now().UnixNano(), 2)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "phase6." + e.EventID
	topics := map[string]string{"main": prefix + ".main", "retry": prefix + ".retry", "dlq": prefix + ".dlq", "recovery": prefix + ".recovery"}
	transport := &kafka.Transport{}
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 30 * time.Second, Transport: transport}
	configs := make([]kafka.TopicConfig, 0, len(topics))
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
		await(t, "phase6 topic leader", func() bool {
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
	f := &phase6Fixture{t: t, root: root, worker: worker, replayID: replay, ctx: ctx, cancel: cancel, db: db, brokers: brokers, client: client, topics: topics, inspect: map[string]inspector{}, evidence: map[string]any{"scenario": name, "status": "FAIL", "startedAt": time.Now().UTC(), "topics": topics, "offsetReset": false}}
	for role, topic := range topics {
		f.inspect[role] = f.inspector(role, topic)
	}
	return f
}

func (f *phase6Fixture) close() {
	for _, worker := range f.workers {
		if !worker.stopped {
			worker.stop(f.t)
		}
	}
	f.evidence["finishedAt"] = time.Now().UTC()
	logs := []map[string]any{}
	for _, worker := range f.workers {
		logs = append(logs, map[string]any{"pid": worker.cmd.Process.Pid, "exitCode": worker.cmd.ProcessState.ExitCode(), "log": worker.log.text()})
	}
	f.evidence["workers"] = logs
	savePhase6(f.t, f.root, f.evidence)
	f.client.Transport.(*kafka.Transport).CloseIdleConnections()
	f.db.Close()
	f.cancel()
}

func (f *phase6Fixture) inspector(role, topic string) inspector {
	group := strings.Join([]string{"phase6", role, strconv.FormatInt(time.Now().UnixNano(), 10)}, "-")
	client := &kafka.Client{Addr: kafka.TCP(f.brokers...), Timeout: 5 * time.Second}
	await(f.t, "phase6 coordinator", func() bool {
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

func (f *phase6Fixture) publish(role string, messages ...kafka.Message) {
	f.t.Helper()
	conn, err := kafka.DialLeader(f.ctx, "tcp", f.brokers[0], f.topics[role], 0)
	if err != nil {
		f.t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.SetRequiredAcks(-1); err != nil {
		f.t.Fatal(err)
	}
	if _, err := conn.WriteMessages(messages...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *phase6Fixture) startWorker(role string, badDB bool, extra ...string) *child {
	f.t.Helper()
	args := []string{"-topic", f.topics["main"], "-retry-topic", f.topics["retry"], "-dlq-topic", f.topics["dlq"], "-recovery-topic", f.topics["recovery"], "-retry-delay", "100ms", "-max-retries", "2"}
	if role == "retry" {
		args = append(args, "-retry-worker")
	}
	if role == "recovery" {
		args = append(args, "-recovery-worker")
	}
	args = append(args, extra...)
	overrides := []string{"KAFKA_CONSUMER_GROUP=" + f.inspect[role].group}
	if badDB {
		overrides = append(overrides, "MYSQL_PORT=1")
	}
	w := start(f.t, f.root, f.worker, overrides, args...)
	f.workers = append(f.workers, w)
	return w
}

func (f *phase6Fixture) replay(dlqOffset int64, recoveryTopic string, wantSuccess bool) map[string]any {
	f.t.Helper()
	cmd := exec.Command(f.replayID, "-dlq-topic", f.topics["dlq"], "-partition", "0", "-offset", strconv.FormatInt(dlqOffset, 10), "-recovery-topic", recoveryTopic)
	cmd.Dir = f.root
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if wantSuccess != (err == nil) {
		f.t.Fatalf("replay success=%v err=%v output=%s", wantSuccess, err, output)
	}
	entry := map[string]any{"exitSuccess": err == nil, "output": string(output)}
	for _, line := range strings.Split(string(output), "\n") {
		var parsed map[string]any
		if json.Unmarshal([]byte(line), &parsed) == nil && (parsed["msg"] == "replay_published" || parsed["msg"] == "replay_failed") {
			entry["event"] = parsed
		}
	}
	return entry
}

func (f *phase6Fixture) seed(e event.OrderCreated) {
	f.t.Helper()
	if _, err := f.db.ExecContext(f.ctx, "INSERT INTO inventory (product_id,available_quantity,updated_at) VALUES (?,100,UTC_TIMESTAMP(6))", e.ProductID); err != nil {
		f.t.Fatal(err)
	}
}

func (f *phase6Fixture) exhaustionRecoveryTwice() {
	e, _ := event.New(time.Now().UnixNano(), 2)
	f.seed(e)
	value, _ := json.Marshal(e)
	f.publish("main", kafka.Message{Key: []byte(e.OrderID), Value: value})
	main := f.startWorker("main", true)
	await(f.t, "main retry publication", func() bool { return f.inspect["main"].committed()[0] == 1 && f.inspect["retry"].ends()[0] == 1 })
	main.stop(f.t)
	retry := f.startWorker("retry", true)
	await(f.t, "retry exhaustion DLQ", func() bool { return f.inspect["retry"].committed()[0] == 2 && f.inspect["dlq"].ends()[0] == 1 })
	retry.stop(f.t)
	dlqRecord := f.inspect["dlq"].read(0, 0)
	letter, err := inventory.DecodeDeadLetter(dlqRecord.Value)
	if err != nil || letter.RetryCount == nil || *letter.RetryCount != 2 || letter.Topic != f.topics["main"] || letter.Offset != 0 {
		f.t.Fatal("retry exhaustion envelope", letter, err)
	}
	if stock(f.t, f.ctx, f.db, e.ProductID) != 100 || processed(f.t, f.ctx, f.db, e.EventID).Count != 0 || f.inspect["dlq"].committed()[0] != -1 {
		f.t.Fatal("exhaustion changed business state or DLQ position")
	}
	firstCLI := f.replay(0, f.topics["recovery"], true)
	if f.inspect["recovery"].ends()[0] != 1 || stock(f.t, f.ctx, f.db, e.ProductID) != 100 || processed(f.t, f.ctx, f.db, e.EventID).Count != 0 || f.inspect["recovery"].committed()[0] != -1 {
		f.t.Fatal("publication incorrectly counted as business recovery")
	}
	afterPublicationInventory := stock(f.t, f.ctx, f.db, e.ProductID)
	afterPublicationMarker := processed(f.t, f.ctx, f.db, e.EventID).Count
	afterPublicationCommitted := f.inspect["recovery"].committed()[0]
	firstRecord := f.inspect["recovery"].read(0, 0)
	recovery := f.startWorker("recovery", false)
	await(f.t, "first business recovery", func() bool {
		return f.inspect["recovery"].committed()[0] == 1 && stock(f.t, f.ctx, f.db, e.ProductID) == 98 && processed(f.t, f.ctx, f.db, e.EventID).Count == 1
	})
	firstMarker := processed(f.t, f.ctx, f.db, e.EventID)
	afterFirstRecoveryInventory := stock(f.t, f.ctx, f.db, e.ProductID)
	afterFirstRecoveryCommitted := f.inspect["recovery"].committed()[0]
	secondCLI := f.replay(0, f.topics["recovery"], true)
	await(f.t, "duplicate business recovery", func() bool { return f.inspect["recovery"].committed()[0] == 2 })
	secondRecord := f.inspect["recovery"].read(0, 1)
	recovery.stop(f.t)
	secondMarker := processed(f.t, f.ctx, f.db, e.EventID)
	afterSecondReplayInventory := stock(f.t, f.ctx, f.db, e.ProductID)
	afterSecondReplayCommitted := f.inspect["recovery"].committed()[0]
	if stock(f.t, f.ctx, f.db, e.ProductID) != 98 || firstMarker != secondMarker || recovery.log.count("inventory_committed") != 1 || recovery.log.count("inventory_duplicate") != 1 || f.inspect["dlq"].ends()[0] != 1 {
		f.t.Fatal("replaying the same DLQ record changed the side effect")
	}
	if header(firstRecord, "replay-count") != "1" || header(secondRecord, "replay-count") != "1" || header(firstRecord, "replay-id") == header(secondRecord, "replay-id") || header(firstRecord, "original-topic") != f.topics["main"] || !bytes.Equal(firstRecord.Value, value) {
		f.t.Fatal("replay identity/original payload mismatch")
	}
	f.evidence["event"] = e
	f.evidence["selectedDLQ"] = map[string]any{"topic": dlqRecord.Topic, "partition": dlqRecord.Partition, "offset": dlqRecord.Offset, "envelope": letter}
	f.evidence["firstReplay"] = map[string]any{"cli": firstCLI, "record": firstRecord, "inventoryAfterPublication": 100, "markerAfterPublication": 0, "inventoryAfterRecovery": 98, "marker": firstMarker, "committedOffset": 1}
	f.evidence["secondReplay"] = map[string]any{"cli": secondCLI, "record": secondRecord, "inventory": 98, "marker": secondMarker, "committedOffset": 2}
	firstReplayEvent, firstReplayErr := event.Decode(firstRecord.Value)
	secondReplayEvent, secondReplayErr := event.Decode(secondRecord.Value)
	f.t.Logf("scenario=ExhaustionRecoveryTwice after_replay_publication inventory=%d marker=%d recovery_committed=%d after_first_recovery inventory=%d marker=%d recovery_committed=%d after_second_replay inventory=%d marker=%d recovery_committed=%d duplicate=%t", afterPublicationInventory, afterPublicationMarker, afterPublicationCommitted, afterFirstRecoveryInventory, firstMarker.Count, afterFirstRecoveryCommitted, afterSecondReplayInventory, secondMarker.Count, afterSecondReplayCommitted, recovery.log.count("inventory_duplicate") == 1)
	f.t.Logf("same_original_eventId=%t second_replay_new_db_side_effect=%t", firstReplayErr == nil && secondReplayErr == nil && firstReplayEvent.EventID == e.EventID && secondReplayEvent.EventID == e.EventID, firstMarker != secondMarker || recovery.log.count("inventory_committed") != 1)
}

func (f *phase6Fixture) recoveryRetry() {
	e, _ := event.New(time.Now().UnixNano(), 2)
	f.seed(e)
	value, _ := json.Marshal(e)
	f.publish("main", kafka.Message{Key: []byte(e.OrderID), Value: value})
	f.publishDeadLetter(e, value, "DB_CONNECTION")
	cli := f.replay(0, f.topics["recovery"], true)
	recovery := f.startWorker("recovery", true)
	await(f.t, "recovery new retry", func() bool { return f.inspect["recovery"].committed()[0] == 1 && f.inspect["retry"].ends()[0] == 1 })
	recovery.stop(f.t)
	retryRecord := f.inspect["retry"].read(0, 0)
	if header(retryRecord, "retry-count") != "1" || header(retryRecord, "replay-count") != "1" || header(retryRecord, "original-topic") != f.topics["main"] || stock(f.t, f.ctx, f.db, e.ProductID) != 100 || processed(f.t, f.ctx, f.db, e.EventID).Count != 0 {
		f.t.Fatal("recovery retry chain did not start at one")
	}
	retry := f.startWorker("retry", false)
	await(f.t, "recovery retry success", func() bool {
		return f.inspect["retry"].committed()[0] == 1 && stock(f.t, f.ctx, f.db, e.ProductID) == 98 && processed(f.t, f.ctx, f.db, e.EventID).Count == 1
	})
	retry.stop(f.t)
	f.evidence["event"] = e
	f.evidence["replayCLI"] = cli
	f.evidence["retryRecord"] = retryRecord
	f.evidence["finalInventory"] = 98
	f.evidence["processedEvent"] = processed(f.t, f.ctx, f.db, e.EventID)
	f.evidence["recoveryCommittedOffset"] = 1
	f.evidence["retryCommittedOffset"] = 1
}

func (f *phase6Fixture) recoveryDomainDLQ() {
	e, _ := event.New(time.Now().UnixNano(), 2)
	value, _ := json.Marshal(e)
	f.publish("main", kafka.Message{Key: []byte(e.OrderID), Value: value})
	main := f.startWorker("main", false)
	await(f.t, "initial domain DLQ", func() bool { return f.inspect["main"].committed()[0] == 1 && f.inspect["dlq"].ends()[0] == 1 })
	main.stop(f.t)
	cli := f.replay(0, f.topics["recovery"], true)
	recoveryRecord := f.inspect["recovery"].read(0, 0)
	recovery := f.startWorker("recovery", false)
	await(f.t, "recovery domain DLQ", func() bool { return f.inspect["recovery"].committed()[0] == 1 && f.inspect["dlq"].ends()[0] == 2 })
	recovery.stop(f.t)
	newDLQ := f.inspect["dlq"].read(0, 1)
	letter, err := inventory.DecodeDeadLetter(newDLQ.Value)
	if err != nil || letter.ErrorCode != "PRODUCT_MISSING" || letter.ReplayCount == nil || *letter.ReplayCount != 1 || letter.ReplayID != header(recoveryRecord, "replay-id") || letter.Topic != f.topics["main"] || letter.Offset != 0 || processed(f.t, f.ctx, f.db, e.EventID).Count != 0 || f.inspect["retry"].ends()[0] != 0 {
		f.t.Fatal("recovery domain failure evidence mismatch", letter, err)
	}
	secondCLI := f.replay(1, f.topics["recovery"], true)
	secondRecoveryRecord := f.inspect["recovery"].read(0, 1)
	recoveryAgain := f.startWorker("recovery", false)
	await(f.t, "second recovery domain DLQ", func() bool { return f.inspect["recovery"].committed()[0] == 2 && f.inspect["dlq"].ends()[0] == 3 })
	recoveryAgain.stop(f.t)
	thirdDLQ := f.inspect["dlq"].read(0, 2)
	secondLetter, err := inventory.DecodeDeadLetter(thirdDLQ.Value)
	var inventoryCount int
	if queryErr := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM inventory WHERE product_id=?", e.ProductID).Scan(&inventoryCount); queryErr != nil {
		f.t.Fatal(queryErr)
	}
	if err != nil || header(secondRecoveryRecord, "replay-count") != "2" || secondLetter.ReplayCount == nil || *secondLetter.ReplayCount != 2 || secondLetter.ReplayID != header(secondRecoveryRecord, "replay-id") || processed(f.t, f.ctx, f.db, e.EventID).Count != 0 || inventoryCount != 0 {
		f.t.Fatal("second replay lineage mismatch", secondLetter, err)
	}
	f.evidence["event"] = e
	f.evidence["replayCLI"] = cli
	f.evidence["recoveryRecord"] = recoveryRecord
	f.evidence["newDLQ"] = map[string]any{"topic": newDLQ.Topic, "partition": newDLQ.Partition, "offset": newDLQ.Offset, "envelope": letter}
	f.evidence["secondReplayCLI"] = secondCLI
	f.evidence["secondRecoveryRecord"] = secondRecoveryRecord
	f.evidence["secondRecoveryDLQ"] = map[string]any{"topic": thirdDLQ.Topic, "partition": thirdDLQ.Partition, "offset": thirdDLQ.Offset, "envelope": secondLetter}
	f.evidence["processedEvents"] = 0
	f.evidence["inventoryRows"] = inventoryCount
	f.evidence["recoveryCommittedOffset"] = 2
}

func (f *phase6Fixture) replayFailures() {
	f.publish("dlq", kafka.Message{Key: []byte("bad"), Value: []byte("{bad")})
	malformed := f.replay(0, f.topics["recovery"], false)
	e, _ := event.New(time.Now().UnixNano(), 1)
	value, _ := json.Marshal(e)
	f.publishDeadLetter(e, value, "PRODUCT_MISSING")
	missingTopic := f.topics["recovery"] + ".missing"
	publishFailure := f.replay(1, missingTopic, false)
	if f.inspect["dlq"].ends()[0] != 2 || f.inspect["dlq"].committed()[0] != -1 || f.inspect["recovery"].ends()[0] != 0 {
		f.t.Fatal("failed replay changed source or destination")
	}
	f.evidence["malformed"] = malformed
	f.evidence["publishFailure"] = publishFailure
	f.evidence["dlqEndOffset"] = 2
	f.evidence["dlqCommittedOffset"] = -1
	f.evidence["recoveryEndOffset"] = 0
}

func (f *phase6Fixture) publishDeadLetter(e event.OrderCreated, value []byte, code string) {
	f.t.Helper()
	count := 2
	d := inventory.DeadLetter{ID: "fixture-" + e.EventID, Key: []byte(e.OrderID), Value: value, Topic: f.topics["main"], Partition: 0, Offset: 0, RetryCount: &count, ErrorCode: code, ErrorMessage: code, FailedAt: time.Now().UTC()}
	payload, err := json.Marshal(d)
	if err != nil {
		f.t.Fatal(err)
	}
	f.publish("dlq", kafka.Message{Key: []byte(e.OrderID), Value: payload})
}

func savePhase6(t *testing.T, root string, run map[string]any) {
	t.Helper()
	path := filepath.Join(root, "docs/phase-6-evidence.json")
	var document struct {
		Scenarios []json.RawMessage `json:"scenarios"`
	}
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &document); err != nil {
			t.Error(err)
			return
		}
	}
	payload, err := json.Marshal(run)
	if err != nil {
		t.Error(err)
		return
	}
	document.Scenarios = append(document.Scenarios, payload)
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
		t.Error(err)
	}
}

func TestPhase6Evidence(t *testing.T) {
	root, _ := filepath.Abs("../..")
	data, err := os.ReadFile(filepath.Join(root, "docs/phase-6-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Scenarios []json.RawMessage `json:"scenarios"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	latest := map[string]string{}
	for _, raw := range document.Scenarios {
		var entry struct{ Scenario, Status string }
		if err := json.Unmarshal(raw, &entry); err != nil {
			t.Fatal(err)
		}
		latest[entry.Scenario] = entry.Status
	}
	for _, name := range []string{"ExhaustionRecoveryTwice", "RecoveryRetry", "RecoveryDomainDLQ", "ReplayFailures"} {
		if latest[name] != "PASS" {
			t.Fatal("missing passing Phase 6 evidence", name)
		}
	}
	for _, prior := range []struct {
		path string
		key  string
	}{
		{"docs/phase-4-evidence.json", "Status"},
		{"docs/phase-5-evidence.json", "regression"},
	} {
		data, err := os.ReadFile(filepath.Join(root, prior.path))
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]json.RawMessage
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		if prior.key == "Status" {
			var status string
			if json.Unmarshal(value[prior.key], &status) != nil || status != "PASS" {
				t.Fatal("Phase 4 preserved evidence not PASS")
			}
		} else {
			var regression struct{ Status string }
			if json.Unmarshal(value[prior.key], &regression) != nil || regression.Status != "PASS" {
				t.Fatal("Phase 5 preserved regression not PASS")
			}
		}
	}
}

func TestPhase6NoUnexpectedDependencies(t *testing.T) {
	root, _ := filepath.Abs("../..")
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "golang.org/x/time") {
		t.Fatal("Phase 7 rate limiter dependency added")
	}
	if _, err := os.Stat(filepath.Join(root, "migrations/003_replay_runs.sql")); !os.IsNotExist(err) {
		t.Fatal("Phase 7 replay state added")
	}
}
