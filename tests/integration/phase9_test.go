//go:build integration && phase2 && phase3 && phase4 && phase5 && phase6 && phase7 && phase8 && phase9

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"kafka-recovery-lab/internal/mysql"
)

var phase9Seeds = [][2]int64{{4101, 4102}, {7301, 7302}, {9201, 9202}}

type phase9Stats struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

type phase9Phase4Result struct {
	SourceFile, SHA256, RunID, Scenario, Strategy string
	Repetition                                    int
	MainSeed, RetrySeed                           int64
	ActualOutageSeconds                           float64
	TotalRetryAttempts                            int
	Common8sRetryAttempts                         int
	OverallPeakRetryRPS                           int
	Common8sPeakRetryRPS                          int
	RecoverySecondsAfterHealthy                   *float64
	RetryOverdueP95MS                             float64
	HOLAttempts, ReservationInversions            int
	Success, DLQ, Unfinished                      int
}

type phase9Phase7Result struct {
	SourceFile, SHA256, RunID, Strategy                        string
	Repetition, InputCount                                     int
	PublishRate, RecoveryRate                                  float64
	PublicationDurationMS, CLICompletionMS                     int64
	PublicationAverageRPS                                      float64
	PublicationPeakRPS                                         int
	ProcessingDurationMS, BusinessRecoveryCompletionMS         int64
	ProcessingAverageRPS                                       float64
	ProcessingPeakRPS                                          int
	PeakLag, RecoveryEndOffset, RecoveryCommittedOffset        int64
	Success, DLQ, Unfinished, InitialInventory, FinalInventory int
	ProcessedEvents                                            int
}

type phase9InvalidRun struct {
	Family, SourceFile, SHA256, Reason string
}

func TestPhase9Summarize(t *testing.T) {
	got := phase9Summarize([]float64{9, 1, 5})
	if got != (phase9Stats{Median: 5, Min: 1, Max: 9}) {
		t.Fatalf("unexpected summary: %+v", got)
	}
}

func TestPhase9Environment(t *testing.T) {
	root, _ := filepath.Abs("../..")
	dir := filepath.Join(root, "experiments/phase9")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	ctxDB, err := mysql.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer ctxDB.Close()
	var mysqlVersion string
	if err := ctxDB.QueryRowContext(t.Context(), "SELECT VERSION()").Scan(&mysqlVersion); err != nil {
		t.Fatal(err)
	}
	run := func(name string, args ...string) string {
		output, commandErr := exec.Command(name, args...).CombinedOutput()
		if commandErr != nil {
			t.Fatalf("%s %v: %v: %s", name, args, commandErr, output)
		}
		return strings.TrimSpace(string(output))
	}
	environment := map[string]any{
		"capturedAt":      time.Now().UTC(),
		"goVersion":       runtime.Version(),
		"goOS":            runtime.GOOS,
		"goArch":          runtime.GOARCH,
		"cpuLogicalCount": runtime.NumCPU(),
		"dockerVersion":   run("docker", "version", "--format", "{{.Server.Version}}"),
		"kafkaVersion":    run("docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--version"),
		"mysqlVersion":    mysqlVersion,
		"phase4":          map[string]any{"events": 60, "products": 6, "maxRetries": 5, "base": "1s", "cap": "8s", "restoreAfter": "8s", "observation": "35s", "seedSets": phase9Seeds},
		"phase7":          map[string]any{"records": 120, "products": 12, "publicationRate": 20, "recoveryRate": 20, "peakDefinition": "maximum records in any sliding interval shorter than one second"},
	}
	data, err := json.MarshalIndent(environment, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "environment.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPhase9Phase4(t *testing.T) {
	root, _ := filepath.Abs("../..")
	binary := build(t, root, "worker")
	rawDir := filepath.Join(root, "experiments/phase9/phase4")
	for _, scenario := range []string{"A", "B"} {
		if !phase9Match("PHASE9_SCENARIO", scenario) {
			continue
		}
		for _, strategy := range []string{"fixed", "exponential", "jitter"} {
			if !phase9Match("PHASE9_STRATEGY", strategy) {
				continue
			}
			for index, seeds := range phase9Seeds {
				repetition := index + 1
				if !phase9Repetition(repetition) {
					continue
				}
				name := fmt.Sprintf("%s/%s/%d", scenario, strategy, repetition)
				t.Run(name, func(t *testing.T) {
					phase4RunScenarioWithOptions(t, root, binary, scenario, strategy, phase4RunOptions{Repetition: repetition, MainSeed: seeds[0], RetrySeed: seeds[1], RawDirectory: rawDir})
				})
			}
		}
	}
}

func TestPhase9Phase7(t *testing.T) {
	root, _ := filepath.Abs("../..")
	worker, cli := build(t, root, "worker"), build(t, root, "replay")
	rawDir := filepath.Join(root, "experiments/phase9/phase7")
	for _, strategy := range []string{"unlimited", "publication-limited", "recovery-limited-backlog"} {
		if !phase9Match("PHASE9_STRATEGY", strategy) {
			continue
		}
		for repetition := 1; repetition <= 3; repetition++ {
			if !phase9Repetition(repetition) {
				continue
			}
			t.Run(fmt.Sprintf("%s/%d", strategy, repetition), func(t *testing.T) {
				f := newPhase7WithOptions(t, root, worker, cli, strategy, repetition, rawDir)
				defer f.close()
				switch strategy {
				case "unlimited":
					f.comparison(0, 0, false)
				case "publication-limited":
					f.comparison(20, 0, false)
				case "recovery-limited-backlog":
					f.comparison(0, 20, true)
				}
				f.raw["status"] = "PASS"
			})
		}
	}
}

func phase9Match(key, value string) bool {
	selected := strings.TrimSpace(os.Getenv(key))
	return selected == "" || strings.EqualFold(selected, value)
}

func phase9Repetition(value int) bool {
	selected := strings.TrimSpace(os.Getenv("PHASE9_REPETITION"))
	if selected == "" {
		return true
	}
	n, err := strconv.Atoi(selected)
	return err == nil && n == value
}

func TestPhase9Audit(t *testing.T) {
	root, _ := filepath.Abs("../..")
	evidencePath := filepath.Join(root, "docs/phase-9-evidence.json")
	generatedAt := time.Now().UTC()
	var preserved []byte
	if os.Getenv("PHASE9_AUDIT_ONLY") == "1" {
		var existing struct {
			GeneratedAt time.Time `json:"generatedAt"`
		}
		var err error
		preserved, err = os.ReadFile(evidencePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(preserved, &existing); err != nil {
			t.Fatal(err)
		}
		generatedAt = existing.GeneratedAt
	}
	phase4Results, invalid4 := phase9AuditPhase4(t, root)
	phase7Results, invalid7 := phase9AuditPhase7(t, root)
	environmentPath := filepath.Join(root, "experiments/phase9/environment.json")
	environmentData, err := os.ReadFile(environmentPath)
	if err != nil {
		t.Fatal(err)
	}
	var environment map[string]any
	if err := json.Unmarshal(environmentData, &environment); err != nil {
		t.Fatal(err)
	}
	document := map[string]any{
		"generatedAt": generatedAt,
		"status":      "PASS",
		"definitions": map[string]string{
			"phase4CommonWindow": "first 8 seconds after FaultStart, before the restore request",
			"phase4Recovery":     "seconds from the first successful external SQL probe after restart until all events reach successful DB commit and both source groups drain; null when any event is DLQ or unfinished",
			"phase7PeakRPS":      "maximum records in any sliding interval shorter than one second; unchanged from Phase 7",
			"invalidRun":         "process crash, infrastructure initialization failure, Docker daemon interruption, harness defect, or evidence write failure; an unexpected performance result is valid",
		},
		"environment":        map[string]any{"sourceFile": "experiments/phase9/environment.json", "sha256": fmt.Sprintf("%x", sha256.Sum256(environmentData)), "values": environment},
		"phase4":             map[string]any{"runs": phase4Results, "aggregates": phase9AggregatePhase4(phase4Results)},
		"phase7":             map[string]any{"runs": phase7Results, "aggregates": phase9AggregatePhase7(phase7Results)},
		"invalidRuns":        append(invalid4, invalid7...),
		"priorResults":       phase9PriorResults(t, root),
		"regression":         map[string]any{"phase0To8IntegrationRerun": false, "phase8ObservabilityRerun": false},
		"claims":             map[string]any{"statisticalSignificance": false, "confidenceInterval": false, "productionSLA": false, "sampleSizePerCell": 3},
		"phase10Implemented": false,
	}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if preserved != nil {
		if !bytes.Equal(preserved, data) {
			t.Fatal("preserved Phase 9 evidence differs from raw audit")
		}
		return
	}
	if err := os.WriteFile(evidencePath, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func phase9AuditPhase4(t *testing.T, root string) ([]phase9Phase4Result, []phase9InvalidRun) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "experiments/phase9/phase4/*.json"))
	if err != nil {
		t.Fatal(err)
	}
	results := []phase9Phase4Result{}
	invalid := []phase9InvalidRun{}
	coverage := map[string]bool{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		relative, _ := filepath.Rel(root, path)
		hash := fmt.Sprintf("%x", sha256.Sum256(data))
		var raw phase4Run
		if err := json.Unmarshal(data, &raw); err != nil {
			invalid = append(invalid, phase9InvalidRun{"phase4", filepath.ToSlash(relative), hash, "invalid JSON evidence"})
			continue
		}
		if raw.Status != "PASS" {
			invalid = append(invalid, phase9InvalidRun{"phase4", filepath.ToSlash(relative), hash, "experiment did not complete; raw status is " + raw.Status})
			continue
		}
		key := fmt.Sprintf("%s/%s/%d", raw.Scenario, raw.Strategy, raw.Repetition)
		if coverage[key] {
			t.Fatal("multiple valid Phase 4 runs for repetition", key)
		}
		coverage[key] = true
		metrics := auditPhase4(t, raw)
		results = append(results, phase9Phase4Result{SourceFile: filepath.ToSlash(relative), SHA256: hash, RunID: raw.RunID, Scenario: raw.Scenario, Strategy: raw.Strategy, Repetition: raw.Repetition, MainSeed: raw.Controls.MainSeed, RetrySeed: raw.Controls.RetrySeed, ActualOutageSeconds: metrics.OutageSeconds, TotalRetryAttempts: metrics.Total, Common8sRetryAttempts: metrics.CommonOutageRetryAttempts, OverallPeakRetryRPS: metrics.PeakRPS, Common8sPeakRetryRPS: metrics.CommonOutagePeakRPS, RecoverySecondsAfterHealthy: metrics.RecoverySecondsAfterFaultEnd, RetryOverdueP95MS: metrics.OverdueP95MS, HOLAttempts: metrics.HOLDelayedAttempts, ReservationInversions: metrics.ScheduleInversions, Success: metrics.Success, DLQ: metrics.DLQ, Unfinished: metrics.Unfinished})
	}
	for _, scenario := range []string{"A", "B"} {
		for _, strategy := range []string{"fixed", "exponential", "jitter"} {
			for repetition := 1; repetition <= 3; repetition++ {
				if !coverage[fmt.Sprintf("%s/%s/%d", scenario, strategy, repetition)] {
					t.Fatalf("missing Phase 4 run %s/%s/%d", scenario, strategy, repetition)
				}
			}
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Scenario != results[j].Scenario {
			return results[i].Scenario < results[j].Scenario
		}
		if results[i].Strategy != results[j].Strategy {
			return results[i].Strategy < results[j].Strategy
		}
		return results[i].Repetition < results[j].Repetition
	})
	return results, invalid
}

func phase9AuditPhase7(t *testing.T, root string) ([]phase9Phase7Result, []phase9InvalidRun) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "experiments/phase9/phase7/*.json"))
	if err != nil {
		t.Fatal(err)
	}
	results := []phase9Phase7Result{}
	invalid := []phase9InvalidRun{}
	coverage := map[string]bool{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		relative, _ := filepath.Rel(root, path)
		hash := fmt.Sprintf("%x", sha256.Sum256(data))
		var raw struct {
			RunID, Strategy, Status                                                                string
			Repetition, InputCount, PublicationPeakRPS, ProcessingPeakRPS                          int
			PublishRate, RecoveryRate, PublicationRPS, ProcessingRPS                               float64
			PublicationDurationMs, CliDurationMs, ProcessingDurationMs, BusinessRecoveryDurationMs int64
			RecoveryPeakLag, RecoveryEndOffset, RecoveryCommittedOffset                            int64
			Success, DLQ, Unfinished, InitialInventory, FinalInventory, ProcessedEvents            int
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			invalid = append(invalid, phase9InvalidRun{"phase7", filepath.ToSlash(relative), hash, "invalid JSON evidence"})
			continue
		}
		if raw.Status != "PASS" {
			invalid = append(invalid, phase9InvalidRun{"phase7", filepath.ToSlash(relative), hash, "experiment did not complete; raw status is " + raw.Status})
			continue
		}
		key := fmt.Sprintf("%s/%d", raw.Strategy, raw.Repetition)
		if coverage[key] {
			t.Fatal("multiple valid Phase 7 runs for repetition", key)
		}
		coverage[key] = true
		if raw.InputCount != phase7Input || raw.Success+raw.DLQ+raw.Unfinished != raw.InputCount || raw.ProcessedEvents != raw.Success || raw.FinalInventory != raw.InitialInventory-raw.Success || raw.RecoveryEndOffset != int64(raw.InputCount) || raw.RecoveryCommittedOffset != int64(raw.InputCount) {
			t.Fatal("Phase 7 reconciliation failed", path)
		}
		results = append(results, phase9Phase7Result{SourceFile: filepath.ToSlash(relative), SHA256: hash, RunID: raw.RunID, Strategy: raw.Strategy, Repetition: raw.Repetition, InputCount: raw.InputCount, PublishRate: raw.PublishRate, RecoveryRate: raw.RecoveryRate, PublicationDurationMS: raw.PublicationDurationMs, CLICompletionMS: raw.CliDurationMs, PublicationAverageRPS: raw.PublicationRPS, PublicationPeakRPS: raw.PublicationPeakRPS, ProcessingDurationMS: raw.ProcessingDurationMs, BusinessRecoveryCompletionMS: raw.BusinessRecoveryDurationMs, ProcessingAverageRPS: raw.ProcessingRPS, ProcessingPeakRPS: raw.ProcessingPeakRPS, PeakLag: raw.RecoveryPeakLag, RecoveryEndOffset: raw.RecoveryEndOffset, RecoveryCommittedOffset: raw.RecoveryCommittedOffset, Success: raw.Success, DLQ: raw.DLQ, Unfinished: raw.Unfinished, InitialInventory: raw.InitialInventory, FinalInventory: raw.FinalInventory, ProcessedEvents: raw.ProcessedEvents})
	}
	for _, strategy := range []string{"unlimited", "publication-limited", "recovery-limited-backlog"} {
		for repetition := 1; repetition <= 3; repetition++ {
			if !coverage[fmt.Sprintf("%s/%d", strategy, repetition)] {
				t.Fatalf("missing Phase 7 run %s/%d", strategy, repetition)
			}
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Strategy != results[j].Strategy {
			return results[i].Strategy < results[j].Strategy
		}
		return results[i].Repetition < results[j].Repetition
	})
	return results, invalid
}

func phase9AggregatePhase4(results []phase9Phase4Result) []map[string]any {
	groups := map[string][]phase9Phase4Result{}
	for _, result := range results {
		groups[result.Scenario+"/"+result.Strategy] = append(groups[result.Scenario+"/"+result.Strategy], result)
	}
	output := []map[string]any{}
	for _, scenario := range []string{"A", "B"} {
		for _, strategy := range []string{"fixed", "exponential", "jitter"} {
			values := groups[scenario+"/"+strategy]
			aggregate := map[string]any{"scenario": scenario, "strategy": strategy, "runs": len(values), "actualOutageSeconds": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return v.ActualOutageSeconds }), "totalRetryAttempts": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.TotalRetryAttempts) }), "common8sRetryAttempts": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.Common8sRetryAttempts) }), "overallPeakRetryRPS": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.OverallPeakRetryRPS) }), "common8sPeakRetryRPS": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.Common8sPeakRetryRPS) }), "retryOverdueP95Ms": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return v.RetryOverdueP95MS }), "holAttempts": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.HOLAttempts) }), "reservationInversions": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.ReservationInversions) }), "success": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.Success) }), "dlq": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.DLQ) }), "unfinished": phase9StatsFor(values, func(v phase9Phase4Result) float64 { return float64(v.Unfinished) })}
			recovery := []float64{}
			for _, value := range values {
				if value.RecoverySecondsAfterHealthy != nil {
					recovery = append(recovery, *value.RecoverySecondsAfterHealthy)
				}
			}
			if len(recovery) == len(values) {
				aggregate["recoverySecondsAfterHealthy"] = phase9Summarize(recovery)
			} else {
				aggregate["recoverySecondsAfterHealthy"] = nil
			}
			output = append(output, aggregate)
		}
	}
	return output
}

func phase9AggregatePhase7(results []phase9Phase7Result) []map[string]any {
	groups := map[string][]phase9Phase7Result{}
	for _, result := range results {
		groups[result.Strategy] = append(groups[result.Strategy], result)
	}
	output := []map[string]any{}
	for _, strategy := range []string{"unlimited", "publication-limited", "recovery-limited-backlog"} {
		values := groups[strategy]
		output = append(output, map[string]any{"strategy": strategy, "runs": len(values), "publicationDurationMs": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.PublicationDurationMS) }), "publicationAverageRPS": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return v.PublicationAverageRPS }), "publicationPeakRPS": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.PublicationPeakRPS) }), "processingDurationMs": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.ProcessingDurationMS) }), "processingAverageRPS": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return v.ProcessingAverageRPS }), "processingPeakRPS": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.ProcessingPeakRPS) }), "peakLag": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.PeakLag) }), "cliCompletionMs": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.CLICompletionMS) }), "businessRecoveryCompletionMs": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.BusinessRecoveryCompletionMS) }), "success": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.Success) }), "dlq": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.DLQ) }), "unfinished": phase9StatsFor7(values, func(v phase9Phase7Result) float64 { return float64(v.Unfinished) })})
	}
	return output
}

func phase9StatsFor(values []phase9Phase4Result, extract func(phase9Phase4Result) float64) phase9Stats {
	numbers := make([]float64, len(values))
	for i, value := range values {
		numbers[i] = extract(value)
	}
	return phase9Summarize(numbers)
}

func phase9StatsFor7(values []phase9Phase7Result, extract func(phase9Phase7Result) float64) phase9Stats {
	numbers := make([]float64, len(values))
	for i, value := range values {
		numbers[i] = extract(value)
	}
	return phase9Summarize(numbers)
}

func phase9Summarize(values []float64) phase9Stats {
	if len(values) == 0 {
		return phase9Stats{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return phase9Stats{Median: sorted[len(sorted)/2], Min: sorted[0], Max: sorted[len(sorted)-1]}
}

func phase9PriorResults(t *testing.T, root string) map[string]any {
	t.Helper()
	phase4Data, err := os.ReadFile(filepath.Join(root, "docs/phase-4-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var phase4Document struct{ Metrics []phase4Metrics }
	if err := json.Unmarshal(phase4Data, &phase4Document); err != nil {
		t.Fatal(err)
	}
	phase4 := []map[string]any{}
	for _, metric := range phase4Document.Metrics {
		phase4 = append(phase4, map[string]any{"scenario": metric.Scenario, "strategy": metric.Strategy, "runId": metric.RunID, "common8sRetryAttempts": metric.CommonOutageRetryAttempts, "common8sPeakRetryRPS": metric.CommonOutagePeakRPS, "overallPeakRetryRPS": metric.PeakRPS, "recoverySecondsAfterHealthy": metric.RecoverySecondsAfterFaultEnd, "retryOverdueP95Ms": metric.OverdueP95MS, "holAttempts": metric.HOLDelayedAttempts, "success": metric.Success, "dlq": metric.DLQ, "unfinished": metric.Unfinished})
	}
	phase7Data, err := os.ReadFile(filepath.Join(root, "docs/phase-7-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var phase7Document struct {
		Comparison []map[string]any `json:"comparison"`
	}
	if err := json.Unmarshal(phase7Data, &phase7Document); err != nil {
		t.Fatal(err)
	}
	phase7 := []map[string]any{}
	for _, metric := range phase7Document.Comparison {
		phase7 = append(phase7, map[string]any{"strategy": metric["strategy"], "runId": metric["runId"], "publicationRPS": metric["publicationRPS"], "publicationPeakRPS": metric["publicationPeakRPS"], "processingRPS": metric["processingRPS"], "processingPeakRPS": metric["processingPeakRPS"], "peakLag": metric["recoveryPeakLag"], "businessRecoveryCompletionMs": metric["businessRecoveryDurationMs"], "success": metric["success"], "dlq": metric["dlq"], "unfinished": metric["unfinished"]})
	}
	return map[string]any{"phase4Source": "docs/phase-4-evidence.json", "phase4": phase4, "phase7Source": "docs/phase-7-evidence.json", "phase7": phase7}
}
