package order

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"kafka-recovery-lab/internal/event"
)

func Handler(publish func(context.Context, event.OrderCreated) error, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			respond(w, http.StatusUnsupportedMediaType, map[string]string{"error": "application/json required"})
			return
		}
		var input struct {
			ProductID int64 `json:"productId"`
			Quantity  int64 `json:"quantity"`
		}
		body := http.MaxBytesReader(w, r.Body, 4096)
		defer body.Close()
		d := json.NewDecoder(body)
		d.DisallowUnknownFields()
		err = d.Decode(&input)
		if err == nil {
			if tailErr := d.Decode(new(any)); tailErr != io.EOF {
				err = errors.New("expected exactly one JSON object")
			}
		}
		if err != nil || input.ProductID <= 0 || input.Quantity <= 0 {
			respond(w, http.StatusBadRequest, map[string]string{"error": "positive integer productId and quantity required in one JSON object (max 4096 bytes)"})
			return
		}
		e, err := event.New(input.ProductID, input.Quantity)
		if err != nil {
			respond(w, http.StatusInternalServerError, map[string]string{"error": "event creation failed"})
			return
		}
		log := logger.With("eventId", e.EventID, "orderId", e.OrderID, "productId", e.ProductID, "quantity", e.Quantity)
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := publish(ctx, e); err != nil {
			log.Error("order_publish_failed", "error", err)
			// Timeout may mean that the broker stored the record but the ack was
			// lost. No HTTP or consumer idempotency is claimed in this phase.
			respond(w, http.StatusServiceUnavailable, map[string]string{
				"error": "publication not confirmed; outcome may be unknown", "eventId": e.EventID, "orderId": e.OrderID,
			})
			return
		}
		log.Info("order_published", "topic", event.OrdersTopic)
		respond(w, http.StatusAccepted, map[string]string{"status": "accepted", "eventId": e.EventID, "orderId": e.OrderID})
	})
	return mux
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
