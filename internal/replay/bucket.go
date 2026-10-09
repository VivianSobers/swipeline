// Package replay paces generated events into a sink with bounded memory
// and reports offered, admitted and completed progress.
package replay

import (
	"context"
	"errors"
	"math"
	"time"
)

// Clock is injectable so pacing can be tested without real sleeps.
type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Bucket is a token bucket. Tokens never exceed burst, so a stalled replay
// does not catch up with an unbounded burst afterwards.
type Bucket struct {
	rate, tokens, capacity float64
	last                   time.Time
	clock                  Clock
}

func NewBucket(rate float64, burst int, clock Clock) (*Bucket, error) {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 || rate > 100000 || burst < 1 || burst > 4096 || clock == nil {
		return nil, errors.New("invalid token bucket")
	}
	return &Bucket{rate, float64(burst), float64(burst), clock.Now(), clock}, nil
}

// Wait blocks until one token is available or ctx is done.
func (b *Bucket) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := b.clock.Now()
		elapsed := now.Sub(b.last).Seconds()
		if elapsed < 0 {
			elapsed = 0
		}
		b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.rate)
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			return nil
		}
		delay := time.Duration(math.Ceil((1 - b.tokens) / b.rate * float64(time.Second)))
		if delay < time.Nanosecond {
			delay = time.Nanosecond
		}
		if err := b.clock.Sleep(ctx, delay); err != nil {
			return err
		}
	}
}
