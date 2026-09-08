package inventory

import (
	"fmt"
	"math/rand"
	"time"
)

// Delay remains the fixed delay and is the base for the other strategies.
// A policy belongs to one serial consumer; its RNG is never shared by workers.
func (p *FailurePolicy) delayFor(count int) time.Duration {
	upper := p.Delay
	if p.Strategy == "" || p.Strategy == "fixed" {
		return upper
	}
	for n := 1; n < count && upper < p.Cap; n++ {
		if upper > p.Cap/2 {
			upper = p.Cap
			break
		}
		upper *= 2
	}
	if upper > p.Cap {
		upper = p.Cap
	}
	if p.Strategy == "jitter" {
		if p.random == nil {
			p.random = rand.New(rand.NewSource(p.Seed))
		}
		return time.Duration(p.random.Int63n(int64(upper) + 1))
	}
	return upper
}

func (p *FailurePolicy) validateDelay() error {
	switch p.Strategy {
	case "", "fixed":
		return nil // Preserve the Phase 3 zero-value policy contract.
	case "exponential", "jitter":
		if p.Cap > 0 && p.Cap <= time.Hour {
			return nil
		}
	}
	return fmt.Errorf("invalid retry strategy or cap: fixed/exponential/jitter and cap (0,1h] required")
}
