package replay

import (
	"context"
	"math"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.now = c.now.Add(d)
	return nil
}

func TestBucketRateAndCappedCatchup(t *testing.T) {
	c := &fakeClock{time.Unix(0, 0)}
	b, err := NewBucket(10, 2, c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err = b.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.now.Sub(time.Unix(0, 0)) != 100*time.Millisecond {
		t.Fatal(c.now)
	}
	c.now = c.now.Add(10 * time.Second)
	start := c.now
	for i := 0; i < 3; i++ {
		_ = b.Wait(context.Background())
	}
	if c.now.Sub(start) != 100*time.Millisecond {
		t.Fatal("unbounded catch-up")
	}
	for _, rate := range []float64{0, -1, math.NaN(), math.Inf(1), 100001} {
		if _, err := NewBucket(rate, 1, c); err == nil {
			t.Fatal("accepted invalid TPS")
		}
	}
}

func TestBucketCancellation(t *testing.T) {
	b, _ := NewBucket(0.01, 1, realClock{})
	_ = b.Wait(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Wait(ctx); err != context.Canceled {
		t.Fatal(err)
	}
}
