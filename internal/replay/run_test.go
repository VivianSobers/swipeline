package replay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
)

func config() Config {
	return Config{generator.Config{RunID: "run", Seed: 42, Day: "2026-10-08", Events: 200}, 100000, 8, 16}
}

func TestOrderAndBoundedSlowSink(t *testing.T) {
	stats := &Stats{}
	var next int64
	err := Run(context.Background(), config(), func(e contract.Event) error {
		if e.SourceSeq != next {
			t.Fatalf("wanted %d got %d", next, e.SourceSeq)
		}
		next++
		time.Sleep(time.Microsecond)
		return nil
	}, stats)
	s := stats.Snapshot()
	if err != nil || next != 200 || s.Completed != 200 || s.PeakInFlight > 16 || s.InFlight != 0 {
		t.Fatalf("%v %+v", err, s)
	}
}

func TestFailureStopsAndReportsPartialProgress(t *testing.T) {
	stats := &Stats{}
	boom := errors.New("disk full")
	n := 0
	err := Run(context.Background(), config(), func(contract.Event) error {
		n++
		if n == 4 {
			return boom
		}
		return nil
	}, stats)
	s := stats.Snapshot()
	if !errors.Is(err, boom) || s.Completed != 3 || s.InFlight != 0 || s.Abandoned == 0 {
		t.Fatalf("%v %+v", err, s)
	}
}

func TestCancelBlockedProducerAndWorkers(t *testing.T) {
	cfg := config()
	cfg.TPS = 0.01
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, func(contract.Event) error { once.Do(cancel); return nil }, &Stats{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers did not stop")
	}
}
