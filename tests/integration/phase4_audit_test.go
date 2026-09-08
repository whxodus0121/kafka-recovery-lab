//go:build integration && phase2 && phase3 && phase4

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-recovery-lab/internal/event"
	"kafka-recovery-lab/internal/inventory"
)

type phase4Attempt struct {
	EventID                                          string
	Offset                                           int64
	Count                                            int
	ScheduledAt, StartedAt, WaitAt, PublicationAckAt time.Time
	OverdueMS, HOLLowerBoundMS                       float64
}
type phase4Metrics struct {
	File, SHA256, RunID, Scenario, Strategy                                            string
	Total, PeakRPS, Success, DLQ, Unfinished                                           int
	ByCount                                                                            map[int]int
	RPS                                                                                []int
	Attempts                                                                           []phase4Attempt
	OverdueMeanMS, OverdueP95MS, OverdueMaxMS                                          float64
	HOLDelayedAttempts, ScheduleInversions                                             int
	HOLTotalLowerBoundMS, BlockingWaitMS                                               float64
	OutageSeconds, RestoreRequestSeconds, PublicationSpanSeconds, MaxPublishLatenessMS float64
	ProcessingEndAt, AllSuccessAt                                                      *time.Time
	ProcessingSecondsAfterFaultEnd, RecoverySecondsAfterFaultEnd                       *float64
	MainPeakLag, RetryPeakLag                                                          int64
	MainFailedEvents, RetryDuringOutage, PeakOutageRPS                                 int
	CommonOutageRetryAttempts, CommonOutagePeakRPS                                     int
	DriverDiagnosticLines                                                              int
}

func TestPhase4Audit(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "experiments/phase4/*.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		GeneratedAt  time.Time
		Status       string
		Metrics      []phase4Metrics
		FailedRuns   []string
		ExcludedRuns []map[string]string
		Regression   json.RawMessage
		Definitions  map[string]string
	}
	summary.GeneratedAt = time.Now().UTC()
	summary.Status = "PASS"
	summary.Definitions = map[string]string{
		"RPS":                 "one-second bins anchored at FaultStart; includes zero bins throughout the fixed observation window",
		"recovery":            "all events successfully committed to DB and both input group offsets drained; null if any DLQ or unfinished",
		"processingEnd":       "first sampled drained offsets after the last terminal DB commit or DLQ timestamp; null if unfinished; sampling upper bound",
		"HOL":                 "sum of overlaps between a prior offset's future-header wait and a later record already acknowledged AND due; conservative lower bound, not all overdue",
		"lag":                 "max(0,end-max(0,committed)); broker snapshots are sequential, not atomic; sample timestamps retained",
		"equalConditions":     "identical planned controls within each scenario; actual DB restart availability and publish jitter measured separately, not assumed identical",
		"outageQualification": "after the first 24.498s startup outlier, require actual unavailable duration 8..12s for comparison; retain excluded raw files and repeat affected cells; this is an exploratory control correction, not a pre-registered experiment",
		"commonOutageWindow":  "first 8 seconds from FaultStart, before any restore request: use CommonOutageRetryAttempts and CommonOutagePeakRPS for equally unavailable windows; full recovery results retain restart duration confounding",
	}
	controls := map[string]phase4Controls{}
	coverage := map[string]int{}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var r phase4Run
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		if r.Status != "PASS" {
			summary.FailedRuns = append(summary.FailedRuns, filepath.Base(path))
			continue
		}
		if outage := r.FaultEnd.Sub(r.FaultStart); outage < 8*time.Second || outage > 12*time.Second {
			summary.ExcludedRuns = append(summary.ExcludedRuns, map[string]string{"file": "experiments/phase4/" + filepath.Base(path), "sha256": fmt.Sprintf("%x", sha256.Sum256(b)), "reason": fmt.Sprintf("actual outage %.3fs outside 8..12s comparison control; execution status %s", outage.Seconds(), r.Status)})
			continue
		}
		if prior, ok := controls[r.Scenario]; ok && !reflect.DeepEqual(prior, r.Controls) {
			t.Fatal("unequal planned controls", path)
		}
		controls[r.Scenario] = r.Controls
		coverage[r.Scenario+"/"+r.Strategy]++
		var m phase4Metrics
		if !t.Run(r.Scenario+"/"+r.Strategy, func(t *testing.T) { m = auditPhase4(t, r) }) {
			continue
		}
		m.File = "experiments/phase4/" + filepath.Base(path)
		m.SHA256 = fmt.Sprintf("%x", sha256.Sum256(b))
		summary.Metrics = append(summary.Metrics, m)
		t.Logf("%s/%s peak=%d total=%d success=%d dlq=%d unfinished=%d overdueP95=%.1fms HOL=%d", r.Scenario, r.Strategy, m.PeakRPS, m.Total, m.Success, m.DLQ, m.Unfinished, m.OverdueP95MS, m.HOLDelayedAttempts)
	}
	for _, s := range []string{"A", "B"} {
		for _, p := range []string{"fixed", "exponential", "jitter"} {
			if coverage[s+"/"+p] == 0 {
				t.Error("missing successful run", s, p)
			}
		}
	}
	if b, err := os.ReadFile(filepath.Join(root, "bin/phase4-regression.json")); err == nil {
		summary.Regression = b
	} else {
		t.Error("missing regression evidence", err)
	}
	if t.Failed() {
		summary.Status = "FAIL"
	}
	b, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs/phase-4-evidence.json"), append(b, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

func auditPhase4(t *testing.T, r phase4Run) phase4Metrics {
	t.Helper()
	c := r.Controls
	m := phase4Metrics{RunID: r.RunID, Scenario: r.Scenario, Strategy: r.Strategy, ByCount: map[int]int{}, RPS: make([]int, int(c.ObserveFor/time.Second)+1), OutageSeconds: r.FaultEnd.Sub(r.FaultStart).Seconds(), RestoreRequestSeconds: r.RestoreRequestedAt.Sub(r.FaultStart).Seconds()}
	if !r.UnavailableConfirmed || !r.HistoricalInventoryUnchanged || len(r.Workers) != 2 || len(r.Published) != c.Events || len(r.MainRecords) != c.Events {
		t.Fatal("incomplete raw evidence")
	}
	// Same planned stop-to-start interval. Allow at most one sample/query interval overhead.
	if m.RestoreRequestSeconds < c.RestoreAfter.Seconds() || m.RestoreRequestSeconds > c.RestoreAfter.Seconds()+1 {
		t.Fatal("restore schedule drift")
	}
	if r.ObservationEnd.Sub(r.FaultStart) < c.ObserveFor || len(r.Samples) < int(c.ObserveFor/c.SampleInterval)-5 {
		t.Fatal("observation missing")
	}
	for i, s := range r.Before {
		if s.End[0] != 0 || s.Committed[0] != -1 {
			t.Fatal("nonempty initial coordinates", i)
		}
	}
	events := map[string]event.OrderCreated{}
	original := map[string]kafka.Message{}
	for i, e := range r.Events {
		events[e.EventID] = e
		rec := r.MainRecords[i]
		decoded, err := event.Decode(rec.Value)
		if err != nil || decoded != e || string(rec.Key) != e.OrderID || rec.Topic != r.Topics[0] || rec.Offset != int64(i) {
			t.Fatal("main record mismatch")
		}
		original[e.EventID] = rec
	}
	for i, p := range r.Published {
		if p.EventID != r.Events[i].EventID || !p.PlannedAt.Equal(r.FaultStart.Add(time.Duration(i)*c.PublishInterval)) || p.StartedAt.Before(p.PlannedAt) || p.AckAt.Before(p.StartedAt) {
			t.Fatal("publication schedule")
		}
		m.MaxPublishLatenessMS = math.Max(m.MaxPublishLatenessMS, float64(p.StartedAt.Sub(p.PlannedAt))/1e6)
	}
	m.PublicationSpanSeconds = r.Published[len(r.Published)-1].AckAt.Sub(r.Published[0].StartedAt).Seconds()
	type logEvent struct {
		Time                     time.Time
		Msg, EventID, Topic      string
		Offset                   int64
		RetryCount               int
		StartedAt, NextAttemptAt time.Time
		OverdueNs                int64
		RetryStrategy            string
		RetryBase, RetryCap      string
		RetrySeed                int64
	}
	logs := make([][]logEvent, 2)
	success := map[string]time.Time{}
	mainFailures := map[string]bool{}
	outageRPS := make([]int, len(m.RPS))
	failed := map[string][]time.Time{}
	published := map[string][]time.Time{}
	waits := map[int64]time.Time{}
	attempts := map[int64]logEvent{}
	lastTerminal := r.FaultStart
	for wi, w := range r.Workers {
		if w.ExitCode != 0 || w.PID <= 0 {
			t.Fatal("worker exit")
		}
		for _, line := range strings.Split(w.Log, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			// The MySQL driver writes its own stderr diagnostics, preserved in raw.
			if strings.HasPrefix(line, "[mysql] ") {
				m.DriverDiagnosticLines++
				continue
			}
			var l logEvent
			if err := json.Unmarshal([]byte(line), &l); err != nil {
				t.Fatal(err)
			}
			logs[wi] = append(logs[wi], l)
			switch l.Msg {
			case "worker_started":
				if l.RetryStrategy != r.Strategy || l.RetrySeed != c.MainSeed+int64(wi) {
					t.Fatal("worker config")
				}
				base, baseErr := time.ParseDuration(l.RetryBase)
				capDelay, capErr := time.ParseDuration(l.RetryCap)
				if baseErr != nil || capErr != nil || base != c.Base || capDelay != c.Cap {
					t.Fatal("worker timing config")
				}
			case "inventory_committed":
				if _, ok := events[l.EventID]; !ok {
					t.Fatal("unknown success")
				}
				if _, ok := success[l.EventID]; ok {
					t.Fatal("duplicate effect in timing experiment")
				}
				success[l.EventID] = l.Time
				if l.Time.After(lastTerminal) {
					lastTerminal = l.Time
				}
			case "inventory_failed":
				if wi == 0 {
					mainFailures[l.EventID] = true
				}
				failed[l.EventID] = append(failed[l.EventID], l.Time)
			case "failure_published":
				published[l.EventID] = append(published[l.EventID], l.Time)
			case "retry_wait":
				waits[l.Offset] = l.Time
			case "retry_attempt_started":
				if wi != 1 {
					t.Fatal("retry in main")
				}
				if _, ok := attempts[l.Offset]; ok {
					t.Fatal("duplicate attempt coordinate")
				}
				attempts[l.Offset] = l
			}
		}
	}
	seenCounts := map[string]int{}
	firstTimes := map[string]string{}
	rngs := []*rand.Rand{rand.New(rand.NewSource(c.MainSeed)), rand.New(rand.NewSource(c.RetrySeed))}
	for _, rec := range r.RetryRecords {
		e, err := event.Decode(rec.Value)
		if err != nil {
			t.Fatal(err)
		}
		origin, ok := original[e.EventID]
		if !ok {
			t.Fatal("unknown retry")
		}
		count, err := strconv.Atoi(header(rec, "retry-count"))
		if err != nil || count < 1 || count > c.MaxRetries || count != seenCounts[e.EventID]+1 {
			t.Fatal("retry count sequence")
		}
		seenCounts[e.EventID] = count
		if len(rec.Headers) != 7 || !bytes.Equal(rec.Key, origin.Key) || !bytes.Equal(rec.Value, origin.Value) || header(rec, "original-topic") != origin.Topic || header(rec, "original-partition") != "0" || header(rec, "original-offset") != strconv.FormatInt(origin.Offset, 10) || header(rec, "last-error-code") != "DB_CONNECTION" {
			t.Fatal("retry contract")
		}
		next, err := time.Parse(time.RFC3339Nano, header(rec, "next-attempt-at"))
		if err != nil {
			t.Fatal(err)
		}
		first, err := time.Parse(time.RFC3339Nano, header(rec, "first-failed-at"))
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			firstTimes[e.EventID] = header(rec, "first-failed-at")
		} else if firstTimes[e.EventID] != header(rec, "first-failed-at") {
			t.Fatal("first time changed")
		}
		upper := c.Base
		if r.Strategy != "fixed" {
			upper = c.Base * time.Duration(1<<(count-1))
			if upper > c.Cap {
				upper = c.Cap
			}
		}
		delay := upper
		if r.Strategy == "jitter" {
			wi := 1
			if count == 1 {
				wi = 0
			}
			delay = time.Duration(rngs[wi].Int63n(int64(upper) + 1))
		}
		if count > len(failed[e.EventID]) || count > len(published[e.EventID]) {
			t.Fatal("missing publication boundary")
		}
		calculatedAt := next.Add(-delay)
		failAt := failed[e.EventID][count-1]
		ackAt := published[e.EventID][count-1]
		if calculatedAt.Before(failAt) || calculatedAt.After(ackAt) || (count == 1 && !calculatedAt.Equal(first)) {
			t.Fatal("delay/seed differs from publication headers", e.EventID, count)
		}
		if l, ok := attempts[rec.Offset]; ok {
			if l.EventID != e.EventID || l.Topic != rec.Topic || l.RetryCount != count || !l.NextAttemptAt.Equal(next) || l.StartedAt.Before(next) || l.OverdueNs != l.StartedAt.Sub(next).Nanoseconds() {
				t.Fatal("early/mismatched retry")
			}
			a := phase4Attempt{EventID: e.EventID, Offset: rec.Offset, Count: count, ScheduledAt: next, StartedAt: l.StartedAt, WaitAt: waits[rec.Offset], PublicationAckAt: ackAt, OverdueMS: float64(l.OverdueNs) / 1e6}
			if a.WaitAt.IsZero() {
				t.Fatal("missing wait")
			}
			m.Attempts = append(m.Attempts, a)
			m.Total++
			m.ByCount[count]++
			bin := int(l.StartedAt.Sub(r.FaultStart) / time.Second)
			if bin < 0 || bin >= len(m.RPS) {
				t.Fatal("outside observation")
			}
			m.RPS[bin]++
			if l.StartedAt.Before(r.FaultEnd) {
				m.RetryDuringOutage++
				outageRPS[bin]++
			}
		}
	}
	if len(attempts) != m.Total {
		t.Fatal("unmatched attempt")
	}
	m.MainFailedEvents = len(mainFailures)
	if r.Scenario == "A" && m.MainFailedEvents != c.Events {
		t.Fatal("burst did not expose all events to outage")
	}
	if r.Scenario == "B" && (m.MainFailedEvents == 0 || m.MainFailedEvents == c.Events) {
		t.Fatal("sustained ingress did not span recovery")
	}
	for _, n := range outageRPS {
		m.PeakOutageRPS = max(m.PeakOutageRPS, n)
	}
	dlq := map[string]bool{}
	for _, rec := range r.DLQRecords {
		var d inventory.DeadLetter
		if err := json.Unmarshal(rec.Value, &d); err != nil {
			t.Fatal(err)
		}
		e, err := event.Decode(d.Value)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := original[e.EventID]
		if !ok || dlq[e.EventID] || !bytes.Equal(o.Value, d.Value) || !bytes.Equal(o.Key, d.Key) || d.Topic != o.Topic || d.Offset != o.Offset || d.Partition != 0 || d.RetryCount == nil || *d.RetryCount != c.MaxRetries || d.ErrorCode != "DB_CONNECTION" {
			t.Fatal("DLQ reconciliation")
		}
		if _, ok := success[e.EventID]; ok {
			t.Fatal("both success and DLQ")
		}
		dlq[e.EventID] = true
		if d.FailedAt.After(lastTerminal) {
			lastTerminal = d.FailedAt
		}
	}
	m.Success = len(success)
	m.DLQ = len(dlq)
	m.Unfinished = c.Events - m.Success - m.DLQ
	if m.Unfinished < 0 {
		t.Fatal("overcount")
	}
	expected := map[int64]int64{}
	for id, n := range r.InitialInventory {
		expected[id] = n
	}
	for id := range success {
		e := events[id]
		expected[e.ProductID] -= e.Quantity
	}
	if !reflect.DeepEqual(expected, r.FinalInventory) {
		t.Fatal("SQL side effects differ from success events")
	}
	for _, s := range r.Samples {
		lag := func(x snapshot) int64 {
			committed := x.Committed[0]
			if committed < 0 {
				committed = 0
			}
			return max(int64(0), x.End[0]-committed)
		}
		m.MainPeakLag = max(m.MainPeakLag, lag(s.Main))
		m.RetryPeakLag = max(m.RetryPeakLag, lag(s.Retry))
		if m.Unfinished == 0 && m.ProcessingEndAt == nil && !s.At.Before(lastTerminal) && s.Main.End[0] == int64(c.Events) && lag(s.Main) == 0 && lag(s.Retry) == 0 {
			at := s.FinishedAt
			m.ProcessingEndAt = &at
			seconds := at.Sub(r.FaultEnd).Seconds()
			m.ProcessingSecondsAfterFaultEnd = &seconds
			if m.Success == c.Events {
				m.AllSuccessAt = &at
				m.RecoverySecondsAfterFaultEnd = &seconds
			}
		}
	}
	if m.Unfinished == 0 && m.ProcessingEndAt == nil {
		t.Fatal("no sampled terminal drain")
	}
	for _, n := range m.RPS {
		m.PeakRPS = max(m.PeakRPS, n)
	}
	for _, n := range m.RPS[:int(c.RestoreAfter/time.Second)] {
		m.CommonOutageRetryAttempts += n
		m.CommonOutagePeakRPS = max(m.CommonOutagePeakRPS, n)
	}
	delays := []float64{}
	for i := range m.Attempts {
		a := &m.Attempts[i]
		delays = append(delays, a.OverdueMS)
		m.OverdueMeanMS += a.OverdueMS
		m.BlockingWaitMS += math.Max(0, float64(a.StartedAt.Sub(a.WaitAt))/1e6)
		if i > 0 && a.ScheduledAt.Before(m.Attempts[i-1].ScheduledAt) {
			m.ScheduleInversions++
		}
		for j := 0; j < i; j++ {
			prior := m.Attempts[j]
			start := prior.WaitAt
			if a.ScheduledAt.After(start) {
				start = a.ScheduledAt
			}
			if a.PublicationAckAt.After(start) {
				start = a.PublicationAckAt
			}
			if prior.ScheduledAt.After(start) {
				a.HOLLowerBoundMS += float64(prior.ScheduledAt.Sub(start)) / 1e6
			}
		}
		if a.HOLLowerBoundMS > 0 {
			m.HOLDelayedAttempts++
			m.HOLTotalLowerBoundMS += a.HOLLowerBoundMS
		}
	}
	if m.Total == 0 {
		t.Fatal("no retry experiment")
	}
	sort.Float64s(delays)
	m.OverdueMeanMS /= float64(m.Total)
	m.OverdueMaxMS = delays[len(delays)-1]
	m.OverdueP95MS = delays[int(math.Ceil(float64(len(delays))*.95))-1]
	return m
}
