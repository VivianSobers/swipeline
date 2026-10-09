// Package gateway serves synchronous authorizations: it admits each request
// durably, waits for a timely proposal or the scoring deadline, and replies
// only with a committed final outcome.
package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/VivianSobers/swipeline/internal/store"
)

// ErrBusy means the queue is full; the client should retry the same ID.
var ErrBusy = errors.New("gateway queue full")

type answer struct {
	row store.Row
	err error
}

type work[T any] struct {
	value T
	due   time.Time
	reply chan answer
}

// Queue groups concurrent submissions into batches of up to 32 for one
// database transaction. A batch is flushed 2 ms after its first item, or at
// the earliest item's due time if sooner.
type Queue[T any] struct {
	in   chan work[T]
	done <-chan struct{}
}

// NewQueue starts a single worker that applies batches until ctx ends. If a
// batch fails, its items are retried one at a time, so one conflicting item
// cannot fail the others.
func NewQueue[T any](ctx context.Context, capacity int, apply func(context.Context, []T) ([]store.Row, error)) *Queue[T] {
	q := &Queue[T]{in: make(chan work[T], capacity), done: ctx.Done()}
	run := func(values []T) ([]store.Row, error) {
		call, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		rows, err := apply(call, values)
		if err == nil && len(rows) != len(values) {
			err = errors.New("batch result size mismatch")
		}
		return rows, err
	}
	go func() {
		for {
			var first work[T]
			select {
			case <-ctx.Done():
				return
			case first = <-q.in:
			}
			batch := []work[T]{first}
			until := time.Now().Add(2 * time.Millisecond)
			if first.due.Before(until) {
				until = first.due
			}
			timer := time.NewTimer(time.Until(until))
		collect:
			for len(batch) < 32 {
				select {
				case next := <-q.in:
					batch = append(batch, next)
					if next.due.Before(until) {
						until = next.due
						timer.Reset(time.Until(until))
					}
				case <-timer.C:
					break collect
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			timer.Stop()
			values := make([]T, len(batch))
			for i, w := range batch {
				values[i] = w.value
			}
			rows, err := run(values)
			if err != nil && len(batch) > 1 {
				for _, w := range batch {
					one, err := run([]T{w.value})
					a := answer{err: err}
					if err == nil {
						a.row = one[0]
					}
					w.reply <- a
				}
				continue
			}
			for i, w := range batch {
				a := answer{err: err}
				if err == nil {
					a.row = rows[i]
				}
				w.reply <- a
			}
		}
	}()
	return q
}

// Submit queues v, due by the given time, and waits for its row. It fails
// fast with ErrBusy when the queue is full. A caller that stops waiting does
// not cancel the work: an admission may still commit and be read back later.
func (q *Queue[T]) Submit(ctx context.Context, v T, due time.Time) (store.Row, error) {
	if err := ctx.Err(); err != nil {
		return store.Row{}, err
	}
	w := work[T]{value: v, due: due, reply: make(chan answer, 1)}
	select {
	case <-q.done:
		return store.Row{}, context.Canceled
	case q.in <- w:
	default:
		return store.Row{}, ErrBusy
	}
	select {
	case a := <-w.reply:
		return a.row, a.err
	case <-ctx.Done():
		return store.Row{}, ctx.Err()
	case <-q.done:
		return store.Row{}, context.Canceled
	}
}
