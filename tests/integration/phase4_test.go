//go:build integration && phase2 && phase3 && phase4

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/config"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/mysql"
)

type phase4Controls struct {
	Events, Products, InitialStock, Quantity, Partitions, MainWorkers, RetryWorkers, Pool, MaxRetries int
	Base, Cap, RestoreAfter, ObserveFor, PublishInterval, SampleInterval                              time.Duration
	MainSeed, RetrySeed                                                                               int64
	DBTimeout                                                                                         time.Duration
}
type phase4Sample struct {
	At, FinishedAt   time.Time
	Main, Retry, DLQ snapshot
}
type phase4Publish struct {
	EventID                     string
	PlannedAt, StartedAt, AckAt time.Time
}
type phase4Process struct {
	PID, ExitCode int
	Log           string
}
type phase4Run struct {
	RunID, Scenario, Strategy, Status                                           string
	Repetition                                                                  int
	Controls                                                                    phase4Controls
	Topics, Groups                                                              []string
	Events                                                                      []event.OrderCreated
	InitialInventory, FinalInventory                                            map[int64]int64
	Before, After                                                               []snapshot
	FaultStart, RestoreRequestedAt, RestoreReturnedAt, FaultEnd, ObservationEnd time.Time
	FaultEndDefinition                                                          string
	UnavailableConfirmed                                                        bool
	Published                                                                   []phase4Publish
	Samples                                                                     []phase4Sample
	MainRecords, RetryRecords, DLQRecords                                       []kafka.Message
	Workers                                                                     []phase4Process
	HistoricalInventoryUnchanged                                                bool
	RawDirectory                                                                string `json:"-"`
}

type phase4RunOptions struct {
	Repetition          int
	MainSeed, RetrySeed int64
	RawDirectory        string
}

func TestPhase4Runs(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := build(t, root, "worker")
	for _, scenario := range []string{"A", "B"} {
		for _, strategy := range []string{"fixed", "exponential", "jitter"} {
			t.Run(scenario+"/"+strategy, func(t *testing.T) { phase4RunScenario(t, root, binary, scenario, strategy) })
		}
	}
}

func phase4RunScenario(t *testing.T, root, binary, scenario, strategy string) {
	phase4RunScenarioWithOptions(t, root, binary, scenario, strategy, phase4RunOptions{MainSeed: 4101, RetrySeed: 4102})
}

func phase4RunScenarioWithOptions(t *testing.T, root, binary, scenario, strategy string, options phase4RunOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := phase4Controls{Events: 60, Products: 6, InitialStock: 100, Quantity: 1, Partitions: 1, MainWorkers: 1, RetryWorkers: 1, Pool: 1, MaxRetries: 5,
		Base: time.Second, Cap: 8 * time.Second, RestoreAfter: 8 * time.Second, ObserveFor: 35 * time.Second, SampleInterval: 500 * time.Millisecond, MainSeed: options.MainSeed, RetrySeed: options.RetrySeed, DBTimeout: 10 * time.Second}
	if scenario == "B" {
		c.PublishInterval = 200 * time.Millisecond
	}
	first, err := event.New(time.Now().UnixMilli()*100, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := phase4Run{RunID: "phase4." + first.EventID, Scenario: scenario, Strategy: strategy, Status: "FAIL", Repetition: options.Repetition, Controls: c,
		InitialInventory: map[int64]int64{}, FinalInventory: map[int64]int64{}, FaultEndDefinition: "first successful external SQL SELECT 1 after restart (100ms polling; query duration recorded by timestamp)", RawDirectory: options.RawDirectory}
	workers := []*child{}
	defer func() {
		for _, w := range workers {
			if !w.stopped {
				w.stop(t)
			}
			r.Workers = append(r.Workers, phase4Process{w.cmd.Process.Pid, w.cmd.ProcessState.ExitCode(), w.log.text()})
		}
		if t.Failed() {
			r.Status = "FAIL"
		}
		savePhase4(t, root, r)
	}()
	db, err := mysql.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	historical := inventoryRows(t, ctx, db)
	for i := 0; i < c.Products; i++ {
		id := first.ProductID + int64(i)
		if _, err := db.ExecContext(ctx, "INSERT INTO inventory (product_id,available_quantity,updated_at) VALUES (?,100,UTC_TIMESTAMP(6))", id); err != nil {
			t.Fatal(err)
		}
		r.InitialInventory[id] = stock(t, ctx, db, id)
	}
	for i := 0; i < c.Events; i++ {
		e, err := event.New(first.ProductID+int64(i%c.Products), 1)
		if err != nil {
			t.Fatal(err)
		}
		r.Events = append(r.Events, e)
	}
	brokers, err := config.Brokers()
	if err != nil {
		t.Fatal(err)
	}
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second, Transport: transport}
	for _, suffix := range []string{"main", "retry", "dlq"} {
		r.Topics = append(r.Topics, r.RunID+"."+suffix)
		r.Groups = append(r.Groups, r.RunID+"-"+suffix)
	}
	configs := []kafka.TopicConfig{}
	for _, topic := range r.Topics {
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
	ins := make([]inspector, 3)
	for i, topic := range r.Topics {
		await(t, "partition leader", func() bool {
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
		await(t, "coordinator", func() bool {
			resp, err := client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{Addr: kafka.TCP(brokers...), Key: r.Groups[i], KeyType: kafka.CoordinatorKeyTypeConsumer})
			if err != nil {
				t.Fatal(err)
			}
			if errors.Is(resp.Error, kafka.GroupCoordinatorNotAvailable) {
				return false
			}
			if resp.Error != nil {
				t.Fatal(resp.Error)
			}
			client.Addr = kafka.TCP(net.JoinHostPort(resp.Coordinator.Host, strconv.Itoa(resp.Coordinator.Port)))
			return true
		})
		ins[i] = inspector{t: t, ctx: ctx, brokers: brokers, client: client, group: r.Groups[i], topic: topic, partitions: []int{0}}
		r.Before = append(r.Before, ins[i].snapshot())
	}
	for i := 0; i < 2; i++ {
		args := []string{"-topic", r.Topics[0], "-retry-topic", r.Topics[1], "-dlq-topic", r.Topics[2], "-retry-strategy", strategy, "-retry-delay", c.Base.String(), "-retry-cap", c.Cap.String(), "-max-retries", strconv.Itoa(c.MaxRetries), "-retry-seed", strconv.FormatInt(c.MainSeed+int64(i), 10)}
		if i == 1 {
			args = append(args, "-retry-worker")
		}
		workers = append(workers, start(t, root, binary, []string{"KAFKA_CONSUMER_GROUP=" + r.Groups[i]}, args...))
	}
	for i := 0; i < 2; i++ {
		await(t, "stable empty worker group", func() bool { return ins[i].members() == 1 })
	}
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], r.Topics[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetRequiredAcks(-1); err != nil {
		t.Fatal(err)
	}
	// The workers are already assigned; every run begins with a stopped real DB.
	docker(t, root, "compose", "stop", "mysql")
	db.Close()
	restored := false
	defer func() {
		if !restored {
			docker(t, root, "compose", "up", "-d", "--wait", "--wait-timeout", "120", "mysql")
		}
	}()
	db, err = mysql.Pool()
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	var one int
	err = db.QueryRowContext(probeCtx, "SELECT 1").Scan(&one)
	probeCancel()
	if err == nil {
		t.Fatal("DB was usable after stop")
	}
	r.UnavailableConfirmed = true
	r.FaultStart = time.Now().UTC()
	type publishedResult struct {
		records []phase4Publish
		err     error
	}
	pubDone := make(chan publishedResult, 1)
	go func() {
		result := publishedResult{}
		for i, e := range r.Events {
			planned := r.FaultStart.Add(time.Duration(i) * c.PublishInterval)
			if d := time.Until(planned); d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					result.err = ctx.Err()
					pubDone <- result
					return
				case <-timer.C:
				}
			}
			started := time.Now().UTC()
			value, _ := json.Marshal(e)
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			_, err := conn.WriteMessages(kafka.Message{Key: []byte(e.OrderID), Value: value})
			result.records = append(result.records, phase4Publish{e.EventID, planned, started, time.Now().UTC()})
			if err != nil {
				result.err = err
				break
			}
		}
		pubDone <- result
	}()
	type restoreResult struct {
		at  time.Time
		err error
	}
	restoreDone := make(chan restoreResult, 1)
	requested := false
	returned := false
	nextSample := r.FaultStart
	for time.Now().Before(r.FaultStart.Add(c.ObserveFor)) {
		now := time.Now().UTC()
		if !requested && !now.Before(r.FaultStart.Add(c.RestoreAfter)) {
			r.RestoreRequestedAt = now
			requested = true
			go func() {
				cmd := exec.CommandContext(ctx, "docker", "compose", "start", "mysql")
				cmd.Dir = root
				_, err := cmd.CombinedOutput()
				restoreDone <- restoreResult{time.Now().UTC(), err}
			}()
		}
		if requested && !returned {
			select {
			case result := <-restoreDone:
				r.RestoreReturnedAt = result.at
				returned = true
				if result.err != nil {
					t.Fatal("restore command", result.err)
				}
			default:
			}
		}
		if requested && r.FaultEnd.IsZero() {
			probeCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			err := db.QueryRowContext(probeCtx, "SELECT 1").Scan(&one)
			cancel()
			if err == nil && one == 1 {
				r.FaultEnd = time.Now().UTC()
				restored = true
			}
		}
		if !now.Before(nextSample) {
			s := phase4Sample{At: time.Now().UTC(), Main: ins[0].snapshot(), Retry: ins[1].snapshot(), DLQ: ins[2].snapshot()}
			s.FinishedAt = time.Now().UTC()
			r.Samples = append(r.Samples, s)
			nextSample = nextSample.Add(c.SampleInterval)
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.ObservationEnd = time.Now().UTC()
	result := <-pubDone
	r.Published = result.records
	if result.err != nil {
		t.Fatal(result.err)
	}
	if !returned {
		result := <-restoreDone
		r.RestoreReturnedAt = result.at
		if result.err != nil {
			t.Fatal(result.err)
		}
	}
	if r.FaultEnd.IsZero() {
		t.Fatal("DB did not recover during observation")
	}
	for _, w := range workers {
		w.stop(t)
	}
	for i := range ins {
		r.After = append(r.After, ins[i].snapshot())
	}
	for _, pair := range []struct {
		i   int
		out *[]kafka.Message
	}{{0, &r.MainRecords}, {1, &r.RetryRecords}, {2, &r.DLQRecords}} {
		for off := int64(0); off < r.After[pair.i].End[0]; off++ {
			*pair.out = append(*pair.out, ins[pair.i].read(0, off))
		}
	}
	for id := range r.InitialInventory {
		r.FinalInventory[id] = stock(t, ctx, db, id)
	}
	afterRows := inventoryRows(t, ctx, db)
	for id := range r.InitialInventory {
		delete(afterRows, id)
	}
	if !reflect.DeepEqual(historical, afterRows) {
		t.Fatal("historical inventory changed")
	}
	r.HistoricalInventoryUnchanged = true
	if len(r.MainRecords) != c.Events {
		t.Fatal("main event count")
	}
	r.Status = "PASS"
	t.Logf("%s/%s main=%d retry=%d dlq=%d outage=%.3fs", scenario, strategy, len(r.MainRecords), len(r.RetryRecords), len(r.DLQRecords), r.FaultEnd.Sub(r.FaultStart).Seconds())
}

func savePhase4(t *testing.T, root string, r phase4Run) {
	t.Helper()
	dir := filepath.Join(root, "experiments/phase4")
	if r.RawDirectory != "" {
		dir = r.RawDirectory
	} else if override := os.Getenv("PHASE4_RAW_DIR"); override != "" {
		dir = override
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%s-%s.json", r.Strategy, r.Scenario, r.RunID))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		t.Error(err)
		return
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(r); err != nil {
		t.Error(err)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
}
