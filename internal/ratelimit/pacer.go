package ratelimit

import (
	"context"
	"fmt"
	"time"
)

// Pacer enforces a minimum interval between starts in one sequential loop.
type Pacer struct {
	interval time.Duration
	last     time.Time
}

func New(rate float64) (*Pacer, error) {
	if rate <= 0 {
		return nil, fmt.Errorf("rate must be greater than zero")
	}
	interval := time.Duration(float64(time.Second) / rate)
	if interval <= 0 {
		return nil, fmt.Errorf("rate is too high")
	}
	return &Pacer{interval: interval}, nil
}

func (p *Pacer) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.last.IsZero() {
		p.last = time.Now()
		return nil
	}
	wait := time.Until(p.last.Add(p.interval))
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	p.last = time.Now()
	return nil
}
