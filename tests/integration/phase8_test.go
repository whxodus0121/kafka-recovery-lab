//go:build integration && phase2 && phase3 && phase4 && phase5 && phase6 && phase7 && phase8

package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
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

type phase8Fixture struct {
	t                 *testing.T
	root, worker, cli string
	ctx               context.Context
	cancel            context.CancelFunc
	db                *sql.DB
	brokers           []string
	client            *kafka.Client
	runID             string
	topics, groups    map[string]string
	inspectors        map[string]inspector
	raw               map[string]any
}

func TestPhase8(t *testing.T) {
	f := newPhase8(t)
	defer f.close()
	f.run()
	f.raw["status"] = "PASS"
}

func newPhase8(t *testing.T) *phase8Fixture {
	root, _ := filepath.Abs("../..")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	id, _ := event.New(time.Now().UnixNano(), 1)
	runID := "phase8." + id.EventID
	topics := map[string]string{}
	for _, role := range []string{"main", "main-fail", "retry", "dlq", "recovery"} {
		topics[role] = runID + "." + role
	}
	groups := map[string]string{}
	for _, role := range []string{"main", "main-fail", "retry", "recovery"} {
		groups[role] = runID + "." + role + ".group"
	}
	transport := &kafka.Transport{}
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 30 * time.Second, Transport: transport}
	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		t.Fatal(err)
	}
	for _, createErr := range response.Errors {
		if createErr != nil {
			t.Fatal(createErr)
		}
	}
	for _, topic := range topics {
		await(t, "phase8 topic leader", func() bool {
			conn, dialErr := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
			if dialErr == nil {
				conn.Close()
				return true
			}
			if errors.Is(dialErr, kafka.NotLeaderForPartition) || errors.Is(dialErr, kafka.LeaderNotAvailable) {
				return false
			}
			t.Fatal(dialErr)
			return false
		})
	}
	f := &phase8Fixture{t: t, root: root, worker: build(t, root, "worker"), cli: build(t, root, "replay"), ctx: ctx, cancel: cancel, db: db, brokers: brokers, client: client, runID: runID, topics: topics, groups: groups, inspectors: map[string]inspector{}, raw: map[string]any{"runId": runID, "status": "FAIL", "startedAt": time.Now().UTC(), "topics": topics, "groups": groups}}
	for _, role := range []string{"main", "main-fail", "retry", "recovery"} {
		f.inspectors[role] = f.newInspector(role)
	}
	return f
}

func (f *phase8Fixture) close() {
	f.raw["finishedAt"] = time.Now().UTC()
	dir := filepath.Join(f.root, "experiments", "phase8")
	if err := os.MkdirAll(dir, 0755); err == nil {
		data, _ := json.MarshalIndent(f.raw, "", "  ")
		_ = os.WriteFile(filepath.Join(dir, "observability-"+f.runID+".json"), append(data, '\n'), 0644)
	}
	f.client.Transport.(*kafka.Transport).CloseIdleConnections()
	f.db.Close()
	f.cancel()
}

func (f *phase8Fixture) newInspector(role string) inspector {
	group, topic := f.groups[role], f.topics[role]
	client := &kafka.Client{Addr: kafka.TCP(f.brokers...), Timeout: 5 * time.Second}
	await(f.t, "phase8 coordinator", func() bool {
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

func (f *phase8Fixture) workerArgs(role, address string, rate float64) []string {
	args := []string{"-retry-topic", f.topics["retry"], "-dlq-topic", f.topics["dlq"], "-recovery-topic", f.topics["recovery"], "-retry-delay", "100ms", "-metrics-address", address}
	switch role {
	case "retry":
		args = append(args, "-retry-worker")
	case "recovery":
		args = append(args, "-recovery-worker", "-recovery-rate", strconv.FormatFloat(rate, 'f', -1, 64))
	default:
		args = append(args, "-topic", f.topics[role])
	}
	return args
}

func (f *phase8Fixture) run() {
	retryWorker := start(f.t, f.root, f.worker, []string{"KAFKA_CONSUMER_GROUP=" + f.groups["retry"]}, f.workerArgs("retry", ":22113", 0)...)
	recoveryWorker := start(f.t, f.root, f.worker, []string{"KAFKA_CONSUMER_GROUP=" + f.groups["recovery"]}, f.workerArgs("recovery", ":22114", 0)...)
	mainWorker := start(f.t, f.root, f.worker, []string{"KAFKA_CONSUMER_GROUP=" + f.groups["main"]}, f.workerArgs("main", ":22112", 0)...)
	for _, role := range []string{"main", "retry", "recovery"} {
		await(f.t, role+" worker", func() bool {
			return map[string]*child{"main": mainWorker, "retry": retryWorker, "recovery": recoveryWorker}[role].log.has("worker_started")
		})
	}
	f.awaitMetric(`sum(up{job="inventory-workers"})`, 3)

	normal := f.newProductEvent(100, 2)
	value, _ := json.Marshal(normal)
	message := kafka.Message{Key: []byte(normal.OrderID), Value: value}
	processingBefore := f.prometheusValue(`inventory_processing_duration_seconds_count{worker="main",outcome="success"}`)
	f.sample("normal", `inventory_consumer_records_total{worker="main",outcome="success"}`, func() {
		f.publish(f.topics["main"], message)
		await(f.t, "normal commit", func() bool { return f.inspectors["main"].committed()[0] == 1 })
	}, 1)
	processingAfter := f.awaitMetric(`inventory_processing_duration_seconds_count{worker="main",outcome="success"}`, 1)
	f.recordSample("normal-processing-duration", `inventory_processing_duration_seconds_count{worker="main",outcome="success"}`, processingBefore, processingAfter)
	f.sample("duplicate", `inventory_duplicate_total{worker="main"}`, func() {
		f.publish(f.topics["main"], message)
		await(f.t, "duplicate commit", func() bool { return f.inspectors["main"].committed()[0] == 2 })
	}, 1)
	f.sample("dlq", `inventory_dlq_published_total{worker="main",error_code="MALFORMED_JSON"}`, func() {
		f.publish(f.topics["main"], kafka.Message{Key: []byte("malformed"), Value: []byte("{bad")})
		await(f.t, "DLQ commit", func() bool { return f.inspectors["main"].committed()[0] == 3 && f.topicEnd("dlq") == 1 })
	}, 1)
	if stock(f.t, f.ctx, f.db, normal.ProductID) != 98 || f.markerCount(normal.EventID) != 1 {
		f.t.Fatal("normal/duplicate DB state mismatch")
	}
	mainWorker.stop(f.t)

	failureWorker := start(f.t, f.root, f.worker, []string{"KAFKA_CONSUMER_GROUP=" + f.groups["main-fail"], "MYSQL_PORT=1"}, f.workerArgs("main-fail", ":22112", 0)...)
	await(f.t, "failure worker", func() bool { return failureWorker.log.has("worker_started") })
	retryEvent := f.newProductEvent(100, 3)
	retryValue, _ := json.Marshal(retryEvent)
	retrySuccessBefore := f.prometheusValue(`inventory_consumer_records_total{worker="retry",outcome="success"}`)
	overdueBefore := f.prometheusValue(`inventory_retry_overdue_seconds_count{strategy="fixed"}`)
	f.sample("retry", `inventory_retry_published_total{worker="main",error_code="DB_CONNECTION"}`, func() {
		f.publish(f.topics["main-fail"], kafka.Message{Key: []byte(retryEvent.OrderID), Value: retryValue})
		await(f.t, "retry completed", func() bool {
			return f.inspectors["main-fail"].committed()[0] == 1 && f.inspectors["retry"].committed()[0] == 1 && stock(f.t, f.ctx, f.db, retryEvent.ProductID) == 97
		})
	}, 1)
	retrySuccessAfter := f.awaitMetric(`inventory_consumer_records_total{worker="retry",outcome="success"}`, 1)
	overdueAfter := f.awaitMetric(`inventory_retry_overdue_seconds_count{strategy="fixed"}`, 1)
	f.recordSample("retry-worker", `inventory_consumer_records_total{worker="retry",outcome="success"}`, retrySuccessBefore, retrySuccessAfter)
	f.recordSample("retry-overdue", `inventory_retry_overdue_seconds_count{strategy="fixed"}`, overdueBefore, overdueAfter)

	recoveryEvent := f.newProductEvent(100, 1)
	singleDLQOffset := f.publishDeadLetters(recoveryEvent)
	f.sample("recovery", `inventory_recovery_processed_total{outcome="success"}`, func() {
		f.runReplay("-offset", strconv.FormatInt(singleDLQOffset, 10), "", "")
		await(f.t, "single recovery", func() bool { return f.inspectors["recovery"].committed()[0] == 1 })
	}, 1)
	if stock(f.t, f.ctx, f.db, recoveryEvent.ProductID) != 99 {
		f.t.Fatal("single recovery DB mismatch")
	}
	recoveryWorker.stop(f.t)

	limitedWorker := start(f.t, f.root, f.worker, []string{"KAFKA_CONSUMER_GROUP=" + f.groups["recovery"]}, f.workerArgs("recovery", ":22114", 5)...)
	await(f.t, "limited recovery worker", func() bool { return limitedWorker.log.has("worker_started") })
	waitBefore := f.awaitMetric(`sum(inventory_recovery_rate_limit_wait_seconds_count)`, 0)
	limitedEvents := make([]event.OrderCreated, 0, 10)
	for range 10 {
		limitedEvents = append(limitedEvents, f.newProductEvent(100, 1))
	}
	bulkDLQOffset := f.publishDeadLetters(limitedEvents...)
	f.runReplay("-start-offset", strconv.FormatInt(bulkDLQOffset, 10), "-limit", "10")
	await(f.t, "limited recovery", func() bool { return f.inspectors["recovery"].committed()[0] == 11 })
	f.awaitMetric(`inventory_recovery_processed_total{outcome="success"}`, 10)
	waitCount := f.awaitAtLeast(`sum(inventory_recovery_rate_limit_wait_seconds_count)`, 8)
	f.recordSample("recovery-rate-limit", `sum(inventory_recovery_rate_limit_wait_seconds_count)`, waitBefore, waitCount)
	for _, e := range limitedEvents {
		if stock(f.t, f.ctx, f.db, e.ProductID) != 99 || f.markerCount(e.EventID) != 1 {
			f.t.Fatal("limited recovery DB mismatch")
		}
	}

	f.awaitMetric(`sum(up{job="inventory-workers"})`, 3)
	targets := f.prometheusTargets()
	cardinality := f.cardinalityAudit()
	grafana := f.grafanaAudit()
	f.raw["processes"] = map[string]any{"main": failureWorker.cmd.Process.Pid, "retry": retryWorker.cmd.Process.Pid, "recovery": limitedWorker.cmd.Process.Pid}
	f.raw["prometheusTargets"] = targets
	f.raw["cardinalityAudit"] = cardinality
	f.raw["grafana"] = grafana
	f.raw["rateLimit"] = map[string]any{"input": 10, "configuredRate": 5, "waitHistogramCount": waitCount, "recoveryCommittedOffset": 11}
	f.raw["database"] = map[string]any{"normalInventory": 98, "retryInventory": 97, "singleRecoveryInventory": 99, "limitedRecoverySuccess": 10}
	f.raw["kafka"] = map[string]any{"mainCommitted": 3, "failedMainCommitted": 1, "retryCommitted": 1, "recoveryCommitted": 11, "dlqEnd": f.topicEnd("dlq"), "recoveryEnd": f.topicEnd("recovery")}
	f.raw["lagMetric"] = map[string]any{"implemented": false, "reason": "No exact committed-group lag collector was added; broker/API polling would expand this phase and last-observed high-water mark would misstate the meaning."}
	f.raw["replayCLIMetrics"] = map[string]any{"scraped": false, "reason": "The replay CLI is short-lived; publication measurements remain in structured logs and Phase 7 evidence without Pushgateway."}
}

func (f *phase8Fixture) newProductEvent(quantity, decrement int64) event.OrderCreated {
	product := time.Now().UnixNano()
	if _, err := f.db.ExecContext(f.ctx, "INSERT INTO inventory (product_id,available_quantity,updated_at) VALUES (?,?,UTC_TIMESTAMP(6))", product, quantity); err != nil {
		f.t.Fatal(err)
	}
	e, err := event.New(product, decrement)
	if err != nil {
		f.t.Fatal(err)
	}
	return e
}

func (f *phase8Fixture) markerCount(eventID string) int {
	var count int
	if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM processed_events WHERE event_id=?", eventID).Scan(&count); err != nil {
		f.t.Fatal(err)
	}
	return count
}

func (f *phase8Fixture) publish(topic string, messages ...kafka.Message) {
	for i := range messages {
		messages[i].Topic = topic
	}
	writer := &kafka.Writer{Addr: kafka.TCP(f.brokers...), RequiredAcks: kafka.RequireAll, Async: false}
	defer writer.Close()
	if err := writer.WriteMessages(f.ctx, messages...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *phase8Fixture) publishDeadLetters(events ...event.OrderCreated) int64 {
	startOffset := f.topicEnd("dlq")
	messages := make([]kafka.Message, 0, len(events))
	for index, e := range events {
		value, _ := json.Marshal(e)
		count := 3
		dlq := inventory.DeadLetter{ID: "phase8-" + e.EventID, Key: []byte(e.OrderID), Value: value, Topic: f.topics["main"], Partition: 0, Offset: int64(index), RetryCount: &count, ErrorCode: "DB_CONNECTION", ErrorMessage: "phase8 fixture", FailedAt: time.Now().UTC()}
		payload, _ := json.Marshal(dlq)
		messages = append(messages, kafka.Message{Key: []byte(e.OrderID), Value: payload})
	}
	f.publish(f.topics["dlq"], messages...)
	return startOffset
}

func (f *phase8Fixture) runReplay(mode, offset, limitFlag, limit string) {
	args := []string{"-dlq-topic", f.topics["dlq"], "-recovery-topic", f.topics["recovery"], "-partition", "0", mode, offset}
	if limitFlag != "" {
		args = append(args, limitFlag, limit)
	}
	cmd := exec.Command(f.cli, args...)
	cmd.Dir, cmd.Env = f.root, os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		f.t.Fatalf("replay: %v %s", err, output)
	}
}

func (f *phase8Fixture) topicEnd(role string) int64 {
	conn, err := kafka.DialLeader(f.ctx, "tcp", f.brokers[0], f.topics[role], 0)
	if err != nil {
		f.t.Fatal(err)
	}
	defer conn.Close()
	end, err := conn.ReadLastOffset()
	if err != nil {
		f.t.Fatal(err)
	}
	return end
}

func (f *phase8Fixture) sample(name, query string, action func(), expected float64) {
	before := f.prometheusValue(query)
	action()
	after := f.awaitMetric(query, expected)
	f.recordSample(name, query, before, after)
}

func (f *phase8Fixture) recordSample(name, query string, before, after float64) {
	samples, _ := f.raw["metricScenarios"].([]map[string]any)
	f.raw["metricScenarios"] = append(samples, map[string]any{"scenario": name, "query": query, "before": before, "after": after, "queryTimestamp": time.Now().UTC()})
}

func (f *phase8Fixture) prometheusValue(query string) float64 {
	var response struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	f.getJSON("http://127.0.0.1:9090/api/v1/query?query="+url.QueryEscape(query), &response)
	if response.Status != "success" {
		f.t.Fatal("Prometheus query failed", query)
	}
	if len(response.Data.Result) == 0 {
		return 0
	}
	value, err := strconv.ParseFloat(fmt.Sprint(response.Data.Result[0].Value[1]), 64)
	if err != nil {
		f.t.Fatal(err)
	}
	return value
}

func (f *phase8Fixture) awaitMetric(query string, expected float64) float64 {
	var value float64
	await(f.t, "metric "+query, func() bool {
		value = f.prometheusValue(query)
		return value == expected
	})
	return value
}

func (f *phase8Fixture) awaitAtLeast(query string, expected float64) float64 {
	var value float64
	await(f.t, "metric "+query, func() bool {
		value = f.prometheusValue(query)
		return value >= expected
	})
	return value
}

func (f *phase8Fixture) getJSON(address string, target any) {
	response, err := http.Get(address)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		f.t.Fatalf("GET %s status=%d body=%s", address, response.StatusCode, body)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		f.t.Fatal(err)
	}
}

func (f *phase8Fixture) prometheusTargets() []map[string]any {
	var response struct {
		Status string `json:"status"`
		Data   struct {
			Active []struct {
				Labels     map[string]string `json:"labels"`
				Health     string            `json:"health"`
				LastScrape time.Time         `json:"lastScrape"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	f.getJSON("http://127.0.0.1:9090/api/v1/targets", &response)
	result := []map[string]any{}
	for _, target := range response.Data.Active {
		if target.Labels["job"] == "inventory-workers" {
			if target.Health != "up" {
				f.t.Fatal("worker target not UP", target.Labels)
			}
			result = append(result, map[string]any{"role": target.Labels["target_role"], "instance": target.Labels["instance"], "health": target.Health, "lastScrape": target.LastScrape})
		}
	}
	if len(result) != 3 {
		f.t.Fatal("expected three worker targets", result)
	}
	sort.Slice(result, func(i, j int) bool { return fmt.Sprint(result[i]["role"]) < fmt.Sprint(result[j]["role"]) })
	return result
}

func (f *phase8Fixture) cardinalityAudit() map[string]any {
	allowedLabels := map[string]bool{"worker": true, "outcome": true, "error_code": true, "strategy": true, "le": true}
	forbidden := []string{"eventid", "orderid", "productid", "replayid", "batchid", "offset", strings.ToLower(f.runID)}
	labelPattern := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="`)
	labels := map[string]bool{}
	requiredMetrics := []string{"inventory_consumer_records_total", "inventory_processing_duration_seconds", "inventory_retry_published_total", "inventory_dlq_published_total", "inventory_duplicate_total", "inventory_retry_overdue_seconds", "inventory_recovery_processed_total", "inventory_recovery_rate_limit_wait_seconds"}
	seenMetrics := map[string]bool{}
	for _, port := range []int{22112, 22113, 22114} {
		response, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
		if err != nil {
			f.t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		output := string(body)
		for _, metric := range requiredMetrics {
			if strings.Contains(output, metric) {
				seenMetrics[metric] = true
			}
		}
		for _, line := range strings.Split(output, "\n") {
			if strings.HasPrefix(line, "#") {
				continue
			}
			start, end := strings.Index(line, "{"), strings.Index(line, "}")
			if start < 0 || end < start {
				continue
			}
			labelSet := strings.ToLower(line[start : end+1])
			for _, value := range forbidden {
				if strings.Contains(labelSet, value) {
					f.t.Fatal("forbidden high-cardinality label in metrics", value)
				}
			}
		}
		for _, match := range labelPattern.FindAllStringSubmatch(output, -1) {
			labels[match[1]] = true
			if !allowedLabels[match[1]] {
				f.t.Fatal("unexpected metric label", match[1])
			}
		}
	}
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(seenMetrics) != len(requiredMetrics) {
		f.t.Fatal("missing required metric descriptor", seenMetrics)
	}
	return map[string]any{"status": "PASS", "metricNames": requiredMetrics, "observedLabelNames": names, "forbiddenIdentifiersAbsent": true, "boundedErrorCodes": true}
}

func (f *phase8Fixture) grafanaAudit() map[string]any {
	var health struct {
		Database string `json:"database"`
	}
	f.getJSON("http://127.0.0.1:3000/api/health", &health)
	var datasource struct{ Name, UID, URL string }
	f.getJSON("http://127.0.0.1:3000/api/datasources/uid/prometheus", &datasource)
	var dashboard struct {
		Dashboard struct {
			UID, Title string
			Panels     []struct {
				Title   string
				Targets []struct{ Expr string }
			}
		} `json:"dashboard"`
	}
	f.getJSON("http://127.0.0.1:3000/api/dashboards/uid/kafka-recovery-lab", &dashboard)
	queries := []map[string]any{}
	for _, panel := range dashboard.Dashboard.Panels {
		for _, target := range panel.Targets {
			var response struct {
				Status string `json:"status"`
			}
			f.getJSON("http://127.0.0.1:3000/api/datasources/proxy/uid/prometheus/api/v1/query?query="+url.QueryEscape(target.Expr), &response)
			if response.Status != "success" {
				f.t.Fatal("panel query failed", panel.Title)
			}
			queries = append(queries, map[string]any{"panel": panel.Title, "expression": target.Expr, "status": response.Status})
		}
	}
	if health.Database != "ok" || datasource.UID != "prometheus" || datasource.URL != "http://prometheus:9090" || dashboard.Dashboard.UID != "kafka-recovery-lab" || len(dashboard.Dashboard.Panels) != 9 || len(queries) != 10 {
		f.t.Fatal("Grafana provisioning mismatch")
	}
	return map[string]any{"health": health.Database, "datasource": map[string]any{"name": datasource.Name, "uid": datasource.UID, "url": datasource.URL}, "dashboard": map[string]any{"title": dashboard.Dashboard.Title, "uid": dashboard.Dashboard.UID, "panelCount": len(dashboard.Dashboard.Panels)}, "panelQueries": queries}
}

func TestPhase8Audit(t *testing.T) {
	root, _ := filepath.Abs("../..")
	paths, err := filepath.Glob(filepath.Join(root, "experiments", "phase8", "observability-*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatal("missing Phase 8 raw evidence", err)
	}
	sort.Slice(paths, func(i, j int) bool {
		left, _ := os.Stat(paths[i])
		right, _ := os.Stat(paths[j])
		return left.ModTime().After(right.ModTime())
	})
	var selected map[string]any
	var source string
	excluded := []string{}
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var raw map[string]any
		if json.Unmarshal(data, &raw) != nil {
			continue
		}
		if raw["status"] != "PASS" {
			relative, _ := filepath.Rel(root, path)
			excluded = append(excluded, filepath.ToSlash(relative))
			continue
		}
		if selected == nil {
			selected, source = raw, path
			selected["sourceSHA256"] = fmt.Sprintf("%x", sha256.Sum256(data))
		}
	}
	if selected == nil {
		t.Fatal("missing passing Phase 8 evidence")
	}
	relative, _ := filepath.Rel(root, source)
	selected["sourceFile"] = filepath.ToSlash(relative)
	document := map[string]any{"generatedAt": time.Now().UTC(), "status": "PASS", "observability": selected, "excludedFailedRuns": excluded, "phase4PerformanceRerun": false, "phase7PerformanceRerun": false, "phase9Implemented": false}
	data, _ := json.MarshalIndent(document, "", "  ")
	if err := os.WriteFile(filepath.Join(root, "docs", "phase-8-evidence.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestPhase8PriorEvidence(t *testing.T) {
	root, _ := filepath.Abs("../..")
	data, err := os.ReadFile(filepath.Join(root, "docs", "phase-7-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &evidence); err != nil || evidence.Status != "PASS" {
		t.Fatal("preserved Phase 7 evidence not PASS", err)
	}
}
