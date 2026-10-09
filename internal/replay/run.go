package replay

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
)

// Config is a generated run plus its transport choices. TPS, Workers and
// Queue never change event identities.
type Config struct {
	Generator generator.Config `json:"generator"`
	TPS       float64          `json:"tps"`
	Workers   int              `json:"workers"`
	Queue     int              `json:"queue"`
}

func (c Config) Validate() error {
	if err := c.Generator.Validate(); err != nil {
		return err
	}
	if c.Workers < 1 || c.Workers > 64 || c.Queue < 1 || c.Queue > 4096 || c.Workers > c.Queue || math.IsNaN(c.TPS) || math.IsInf(c.TPS, 0) || c.TPS <= 0 || c.TPS > 100000 {
		return errors.New("require finite TPS in (0,100000], 1..64 workers and workers..4096 queue")
	}
	return nil
}

// Snapshot is a point-in-time view of replay progress. Admitted means
// queued for generation and Completed means written to the sink; neither
// means a transaction was authorized.
type Snapshot struct {
	Offered             int64   `json:"offered"`
	Admitted            int64   `json:"admitted"`
	Completed           int64   `json:"completed"`
	Abandoned           int64   `json:"abandoned"`
	InFlight            int64   `json:"in_flight"`
	PeakInFlight        int64   `json:"peak_in_flight"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	OfferedTPS          float64 `json:"offered_tps"`
	AdmittedTPS         float64 `json:"admitted_tps"`
	AchievedTPS         float64 `json:"achieved_tps"`
	SchedulerLagSeconds float64 `json:"scheduler_lag_seconds"`
}

// Stats is safe for concurrent use.
type Stats struct {
	mu         sync.Mutex
	value      Snapshot
	start, end time.Time
}

func (s *Stats) begin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.start = time.Now()
	s.end = time.Time{}
	s.value = Snapshot{}
}

func (s *Stats) record(offered, admitted, completed int64, lag float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value.Offered += offered
	s.value.Admitted += admitted
	s.value.Completed += completed
	s.value.InFlight = s.value.Admitted - s.value.Completed
	if s.value.InFlight > s.value.PeakInFlight {
		s.value.PeakInFlight = s.value.InFlight
	}
	if lag > s.value.SchedulerLagSeconds {
		s.value.SchedulerLagSeconds = lag
	}
}

func (s *Stats) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.end = time.Now()
	s.value.Abandoned = s.value.Admitted - s.value.Completed
	s.value.InFlight = 0
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.value
	if s.start.IsZero() {
		return v
	}
	end := s.end
	if end.IsZero() {
		end = time.Now()
	}
	v.ElapsedSeconds = math.Max(end.Sub(s.start).Seconds(), 0.000001)
	v.OfferedTPS = float64(v.Offered) / v.ElapsedSeconds
	v.AdmittedTPS = float64(v.Admitted) / v.ElapsedSeconds
	v.AchievedTPS = float64(v.Completed) / v.ElapsedSeconds
	return v
}

// Sink receives events in source order. It must return promptly.
type Sink func(contract.Event) error

type result struct {
	ordinal int64
	event   contract.Event
	err     error
}

// Run generates cfg's events at cfg.TPS with cfg.Workers workers and writes
// them to sink in ordinal order. A window of cfg.Queue tokens, released only
// after the ordered sink write, bounds queued jobs, worker output and the
// reorder buffer together. The first sink or generation error cancels the
// run and is returned.
func Run(parent context.Context, cfg Config, sink Sink, stats *Stats) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if sink == nil || stats == nil {
		return errors.New("sink and stats required")
	}
	g, _ := generator.New(cfg.Generator)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stats.begin()
	defer stats.finish()
	started := time.Now()
	jobs := make(chan int64, cfg.Queue)
	results := make(chan result, cfg.Queue)
	window := make(chan struct{}, cfg.Queue)
	producerDone := make(chan error, 1)
	go func() {
		defer close(jobs)
		bucket, _ := NewBucket(cfg.TPS, 1, realClock{})
		for i := int64(0); i < cfg.Generator.Events; i++ {
			select {
			case window <- struct{}{}:
			case <-ctx.Done():
				producerDone <- ctx.Err()
				return
			}
			if err := bucket.Wait(ctx); err != nil {
				producerDone <- err
				return
			}
			lag := time.Since(started).Seconds() - float64(i)/cfg.TPS
			stats.record(1, 1, 0, math.Max(lag, 0))
			select {
			case jobs <- i:
			case <-ctx.Done():
				producerDone <- ctx.Err()
				return
			}
		}
		producerDone <- nil
	}()
	var workers sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				e, err := g.At(i, time.Now().UnixMilli())
				select {
				case results <- result{i, e, err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { workers.Wait(); close(results) }()
	pending := make(map[int64]result, cfg.Queue)
	var next int64
	var failure error
	for item := range results {
		if failure != nil {
			continue
		}
		pending[item.ordinal] = item
		for {
			current, ok := pending[next]
			if !ok {
				break
			}
			if current.err != nil {
				failure = current.err
				cancel()
				break
			}
			if err := sink(current.event); err != nil {
				failure = err
				cancel()
				break
			}
			stats.record(0, 0, 1, 0)
			delete(pending, next)
			next++
			<-window
		}
	}
	producerErr := <-producerDone
	if failure != nil {
		return failure
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	if producerErr != nil {
		return producerErr
	}
	if next != cfg.Generator.Events {
		return errors.New("incomplete preview")
	}
	return nil
}
