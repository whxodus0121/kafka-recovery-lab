package observability

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsExposeBoundedLabelsAndRealObservations(t *testing.T) {
	m, err := New("recovery")
	if err != nil {
		t.Fatal(err)
	}
	m.ObserveRecord("success")
	m.ObserveProcessing("success", 10*time.Millisecond)
	m.ObserveRetryPublished("raw database password")
	m.ObserveDLQPublished("MALFORMED_JSON")
	m.ObserveDuplicate()
	m.ObserveRetryOverdue("jitter", -time.Second)
	m.ObserveRecoveryProcessed("success")
	m.ObserveRecoveryRateLimitWait(20 * time.Millisecond)

	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	m.Handler().ServeHTTP(response, request)
	body, _ := io.ReadAll(response.Result().Body)
	output := string(body)
	for _, expected := range []string{
		`inventory_consumer_records_total{outcome="success",worker="recovery"} 1`,
		`inventory_retry_published_total{error_code="UNKNOWN",worker="recovery"} 1`,
		`inventory_dlq_published_total{error_code="MALFORMED_JSON",worker="recovery"} 1`,
		`inventory_duplicate_total{worker="recovery"} 1`,
		`inventory_recovery_processed_total{outcome="success"} 1`,
		`inventory_recovery_rate_limit_wait_seconds_count 1`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing metric %q", expected)
		}
	}
	if strings.Contains(output, "raw database password") {
		t.Fatal("raw error entered a metric label")
	}
	if _, err := New("dynamic-topic"); err == nil {
		t.Fatal("accepted dynamic worker label")
	}
}

func TestMetricsServerFollowsContextCancellation(t *testing.T) {
	m, _ := New("main")
	ctx, cancel := context.WithCancel(context.Background())
	server, err := Start(ctx, "127.0.0.1:0", m.Handler())
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}
