package authorize

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/replay"
)

// Rules is the rule version authorization runs are scored under.
const Rules = "auth-rules-v1"

// Summary counts one replay's outcomes. Every completed request has a
// durable final; stand-ins are also counted among declines.
type Summary struct {
	Offered     int64   `json:"offered"`
	Completed   int64   `json:"completed"`
	Approved    int64   `json:"approved"`
	Declined    int64   `json:"declined"`
	StandIn     int64   `json:"stand_in"`
	Peak        int64   `json:"peak_in_flight"`
	Seconds     float64 `json:"elapsed_seconds"`
	AchievedTPS float64 `json:"achieved_tps"`
}

// Run registers cfg as an authorization run and sends its events through
// the gateway. Each card hashes to one of concurrency FIFO lanes, so a card's
// requests arrive in source order; cfg.Queue bounds requests in flight. The
// first failure cancels the run; rerunning the same configuration resumes it,
// since every request keeps its identity.
func Run(parent context.Context, cfg replay.Config, c *Client, mode string, concurrency int) (Summary, error) {
	var result Summary
	if err := cfg.Validate(); err != nil {
		return result, err
	}
	if concurrency < 1 || concurrency > 128 {
		return result, errors.New("lanes must be 1..128")
	}
	r := auth.Run{ID: cfg.Generator.RunID, Identity: contract.ID(cfg.Generator.Identity(), Rules), Day: cfg.Generator.Day,
		Rule: Rules, Reference: generator.Reference, Mode: mode, TPS: cfg.TPS,
		Compression: 86400 * cfg.TPS / float64(cfg.Generator.Events), Concurrency: concurrency,
		Partitioner: "sha256-json-v1", Partitions: 4}
	if err := r.Validate(); err != nil {
		return result, err
	}
	if err := c.Register(parent, r); err != nil {
		return result, err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	started := time.Now()
	var mu sync.Mutex
	var failure error
	var inFlight int64
	tokens := make(chan struct{}, cfg.Queue)
	release := func() {
		mu.Lock()
		inFlight--
		mu.Unlock()
		<-tokens
	}
	lanes := make([]chan contract.Event, concurrency)
	var workers sync.WaitGroup
	for i := range lanes {
		lanes[i] = make(chan contract.Event, 1)
		workers.Add(1)
		go func(lane <-chan contract.Event) {
			defer workers.Done()
			for e := range lane {
				f, err := c.Send(ctx, e)
				mu.Lock()
				if err != nil {
					if failure == nil {
						failure = err
					}
					cancel()
				} else {
					result.Completed++
					if f.Decision == "APPROVE" {
						result.Approved++
					} else {
						result.Declined++
					}
					if f.Source == "STAND_IN" {
						result.StandIn++
					}
				}
				mu.Unlock()
				release()
			}
		}(lanes[i])
	}
	err := replay.Run(ctx, cfg, func(e contract.Event) error {
		b := e.Business
		b.RuleVersion = Rules
		b.TransactionID = contract.ID(e.TransactionID, Rules)
		e, err := contract.New(b, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		select {
		case tokens <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		mu.Lock()
		result.Offered++
		inFlight++
		result.Peak = max(result.Peak, inFlight)
		mu.Unlock()
		sum := sha256.Sum256(auth.Key(e.RunID, e.CardToken))
		select {
		case lanes[binary.BigEndian.Uint32(sum[:4])%uint32(concurrency)] <- e:
			return nil
		case <-ctx.Done():
			release()
			return ctx.Err()
		}
	}, &replay.Stats{})
	for _, lane := range lanes {
		close(lane)
	}
	workers.Wait()
	result.Seconds = time.Since(started).Seconds()
	result.AchievedTPS = float64(result.Completed) / result.Seconds
	if failure != nil {
		return result, failure
	}
	return result, err
}
