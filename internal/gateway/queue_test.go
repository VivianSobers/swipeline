package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/store"
)

// echo returns one row per value, tagged with the value so tests can check
// that replies reach the right submitter.
func echo(in []int) []store.Row {
	rows := make([]store.Row, len(in))
	for i, v := range in {
		rows[i].Request = auth.Request{CardSeq: int64(v)}
	}
	return rows
}

func TestQueueBatchesWorkArrivingWhileBusy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	sizes := make(chan int, 4)
	q := NewQueue(ctx, 8, func(ctx context.Context, in []int) ([]store.Row, error) {
		sizes <- len(in)
		if in[0] == 0 {
			<-release
		}
		return echo(in), nil
	})
	var wg sync.WaitGroup
	submit := func(v int) {
		defer wg.Done()
		row, err := q.Submit(ctx, v, time.Now().Add(time.Hour))
		if err != nil || row.Request.CardSeq != int64(v) {
			t.Errorf("value %d: %+v %v", v, row, err)
		}
	}
	wg.Add(1)
	go submit(0)
	if n := <-sizes; n != 1 {
		t.Fatal("first batch", n)
	}
	for v := 1; v <= 3; v++ {
		wg.Add(1)
		go submit(v)
	}
	for len(q.in) < 3 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	if n := <-sizes; n != 3 {
		t.Fatal("queued work was not batched", n)
	}
	wg.Wait()
}

func TestQueueIsolatesFailingItem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	q := NewQueue(ctx, 8, func(ctx context.Context, in []int) ([]store.Row, error) {
		if in[0] == 0 {
			close(started)
			<-release
		}
		for _, v := range in {
			if v < 0 {
				return nil, errors.New("conflict")
			}
		}
		return echo(in), nil
	})
	go q.Submit(ctx, 0, time.Now())
	<-started
	results := make(chan error, 3)
	for _, v := range []int{1, -1, 2} {
		go func() {
			row, err := q.Submit(ctx, v, time.Now().Add(time.Hour))
			if err == nil && row.Request.CardSeq != int64(v) {
				err = errors.New("misrouted reply")
			}
			if (err != nil) != (v < 0) {
				t.Errorf("value %d: %v", v, err)
			}
			results <- err
		}()
	}
	for len(q.in) < 3 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	for range 3 {
		<-results
	}
}

func TestQueueRejectsWhenFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	q := NewQueue(ctx, 1, func(ctx context.Context, in []int) ([]store.Row, error) {
		started <- struct{}{}
		<-release
		return echo(in), nil
	})
	go q.Submit(ctx, 0, time.Now())
	<-started
	go q.Submit(ctx, 1, time.Now())
	for len(q.in) != 1 {
		time.Sleep(time.Millisecond)
	}
	if _, err := q.Submit(ctx, 2, time.Now()); !errors.Is(err, ErrBusy) {
		t.Fatal("full queue accepted work:", err)
	}
}

func TestQueueStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := NewQueue(ctx, 1, func(ctx context.Context, in []int) ([]store.Row, error) { return echo(in), nil })
	if _, err := q.Submit(ctx, 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := q.Submit(ctx, 2, time.Now()); err == nil {
		t.Fatal("cancel ignored")
	}
}
