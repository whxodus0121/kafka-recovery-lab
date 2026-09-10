package observability

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	worker            string
	registry          *prometheus.Registry
	records           *prometheus.CounterVec
	processing        *prometheus.HistogramVec
	retryPublished    *prometheus.CounterVec
	dlqPublished      *prometheus.CounterVec
	duplicates        *prometheus.CounterVec
	retryOverdue      *prometheus.HistogramVec
	recoveryProcessed *prometheus.CounterVec
	recoveryWait      prometheus.Histogram
}

var outcomes = []string{"success", "duplicate", "retry", "dlq", "error"}
var strategies = []string{"fixed", "exponential", "jitter", "unknown"}
var errorCodes = []string{
	"EVENT_ID_CONFLICT", "PRODUCT_MISSING", "INSUFFICIENT_INVENTORY",
	"DB_TIMEOUT", "DB_CONNECTION", "DB_DEADLOCK", "DB_LOCK_TIMEOUT",
	"INVALID_REPLAY_METADATA", "INVALID_RETRY_METADATA", "EVENT_CONTRACT",
	"MALFORMED_JSON", "UNSUPPORTED_SCHEMA", "KEY_MISMATCH", "UNKNOWN",
}

func New(worker string) (*Metrics, error) {
	worker, err := normalizeWorker(worker)
	if err != nil {
		return nil, err
	}
	m := &Metrics{
		worker:   worker,
		registry: prometheus.NewRegistry(),
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inventory_consumer_records_total", Help: "Inventory records by worker and final processing attempt outcome before source offset commit.",
		}, []string{"worker", "outcome"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "inventory_processing_duration_seconds", Help: "Inventory Store call duration, excluding Kafka fetch and retry scheduling delay.",
		}, []string{"worker", "outcome"}),
		retryPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inventory_retry_published_total", Help: "Successfully published Retry records by source worker and bounded error code.",
		}, []string{"worker", "error_code"}),
		dlqPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inventory_dlq_published_total", Help: "Successfully published DLQ records by source worker and bounded error code.",
		}, []string{"worker", "error_code"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inventory_duplicate_total", Help: "Idempotency duplicates returned by Inventory Store.",
		}, []string{"worker"}),
		retryOverdue: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "inventory_retry_overdue_seconds", Help: "Non-negative delay between persisted next-attempt-at and actual Retry Store attempt start.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30},
		}, []string{"strategy"}),
		recoveryProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inventory_recovery_processed_total", Help: "Recovery records for which the Inventory Store returned a result, by outcome.",
		}, []string{"outcome"}),
		recoveryWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "inventory_recovery_rate_limit_wait_seconds", Help: "Actual positive time spent waiting at the Recovery Store-entry rate limiter.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
		}),
	}
	collectors := []prometheus.Collector{m.records, m.processing, m.retryPublished, m.dlqPublished, m.duplicates, m.retryOverdue, m.recoveryProcessed, m.recoveryWait}
	for _, collector := range collectors {
		if err := m.registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register metrics: %w", err)
		}
	}
	for _, outcome := range outcomes {
		m.records.WithLabelValues(worker, outcome).Add(0)
		m.processing.WithLabelValues(worker, outcome)
		m.recoveryProcessed.WithLabelValues(outcome).Add(0)
	}
	for _, code := range errorCodes {
		m.retryPublished.WithLabelValues(worker, code).Add(0)
		m.dlqPublished.WithLabelValues(worker, code).Add(0)
	}
	for _, strategy := range strategies {
		m.retryOverdue.WithLabelValues(strategy)
	}
	m.duplicates.WithLabelValues(worker).Add(0)
	return m, nil
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveRecord(outcome string) {
	m.records.WithLabelValues(m.worker, normalizeOutcome(outcome)).Inc()
}

func (m *Metrics) ObserveProcessing(outcome string, duration time.Duration) {
	m.processing.WithLabelValues(m.worker, normalizeOutcome(outcome)).Observe(duration.Seconds())
}

func (m *Metrics) ObserveRetryPublished(errorCode string) {
	m.retryPublished.WithLabelValues(m.worker, normalizeErrorCode(errorCode)).Inc()
}

func (m *Metrics) ObserveDLQPublished(errorCode string) {
	m.dlqPublished.WithLabelValues(m.worker, normalizeErrorCode(errorCode)).Inc()
}

func (m *Metrics) ObserveDuplicate() { m.duplicates.WithLabelValues(m.worker).Inc() }

func (m *Metrics) ObserveRetryOverdue(strategy string, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	m.retryOverdue.WithLabelValues(normalizeStrategy(strategy)).Observe(duration.Seconds())
}

func (m *Metrics) ObserveRecoveryProcessed(outcome string) {
	m.recoveryProcessed.WithLabelValues(normalizeOutcome(outcome)).Inc()
}

func (m *Metrics) ObserveRecoveryRateLimitWait(duration time.Duration) {
	if duration > 0 {
		m.recoveryWait.Observe(duration.Seconds())
	}
}

func normalizeWorker(worker string) (string, error) {
	switch worker {
	case "main", "retry", "recovery":
		return worker, nil
	default:
		return "", fmt.Errorf("invalid metrics worker role %q", worker)
	}
}

func normalizeOutcome(outcome string) string {
	switch outcome {
	case "success", "duplicate", "retry", "dlq", "error":
		return outcome
	default:
		return "error"
	}
}

func normalizeStrategy(strategy string) string {
	switch strategy {
	case "fixed", "exponential", "jitter":
		return strategy
	default:
		return "unknown"
	}
}

func normalizeErrorCode(code string) string {
	for _, allowed := range errorCodes {
		if code == allowed {
			return code
		}
	}
	return "UNKNOWN"
}
