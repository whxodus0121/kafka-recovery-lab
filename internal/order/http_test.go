package order

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kafka-recovery-lab/internal/event"
)

func TestInvalidInputNeverPublishes(t *testing.T) {
	for _, input := range []string{`{}`, `null`, `{"productId":1,"quantity":0}`, `{"productId":1}`, `{"productId":-1,"quantity":1}`, `{"productId":1,"quantity":1.5}`, `{"productId":1,"quantity":1,"extra":true}`, `{"productId":1,"quantity":1} {}`, strings.Repeat(" ", 4097) + `{}`} {
		t.Run(input[:min(len(input), 60)], func(t *testing.T) {
			calls := 0
			h := Handler(func(context.Context, event.OrderCreated) error { calls++; return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)))
			r := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(input))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 400 || calls != 0 {
				t.Fatalf("status=%d publishes=%d", w.Code, calls)
			}
		})
	}
}

func TestResponseWaitsForPublishResult(t *testing.T) {
	for _, fail := range []bool{false, true} {
		w := httptest.NewRecorder()
		calls := 0
		h := Handler(func(_ context.Context, e event.OrderCreated) error {
			calls++
			if w.Body.Len() != 0 {
				t.Fatal("response written before publish result")
			}
			if err := e.Validate(); err != nil {
				t.Fatal(err)
			}
			if fail {
				return errors.New("broker acknowledgement unavailable")
			}
			return nil
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		r := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"productId":1,"quantity":3}`))
		r.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, r)
		expected := 202
		if fail {
			expected = 503
		}
		if w.Code != expected || calls != 1 {
			t.Fatalf("status=%d publishes=%d", w.Code, calls)
		}
	}
}
