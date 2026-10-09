package authorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/replay"
)

func gateway(t *testing.T, fail string) (*httptest.Server, *auth.Run, map[string][]int64) {
	var mu sync.Mutex
	var run auth.Run
	order := map[string][]int64{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/runs" {
			_ = json.NewDecoder(r.Body).Decode(&run)
			w.WriteHeader(http.StatusCreated)
			return
		}
		var e contract.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil || e.Validate() != nil {
			t.Error("invalid event", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		order[e.CardToken] = append(order[e.CardToken], e.SourceSeq)
		mu.Unlock()
		if e.TransactionID == fail {
			w.WriteHeader(http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(auth.Final{Request: auth.Request{Event: e}, DecisionID: contract.ID(e.TransactionID, "decision"), Decision: "APPROVE", Source: "FLINK"})
	}))
	t.Cleanup(s.Close)
	return s, &run, order
}

func TestReplayBoundsOrderAndCount(t *testing.T) {
	s, run, order := gateway(t, "")
	cfg := replay.Config{Generator: generator.Config{RunID: "lanes", Seed: 42, Day: "2026-10-08", Events: 1000}, TPS: 100000, Workers: 4, Queue: 16}
	out, err := Run(context.Background(), cfg, &Client{Base: s.URL, HTTP: s.Client()}, "latency", 8)
	if err != nil || out.Offered != 1000 || out.Completed != 1000 || out.Approved != 1000 || out.Peak > 16 || out.Peak < 1 {
		t.Fatal(out, err)
	}
	if run.ID != "lanes" || run.Rule != Rules || run.Mode != "latency" || run.Concurrency != 8 || run.Validate() != nil {
		t.Fatalf("manifest %+v", run)
	}
	repeated := 0
	for card, seqs := range order {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("card %s out of order: %v", card, seqs)
			}
		}
		if len(seqs) > 1 {
			repeated++
		}
	}
	if repeated == 0 {
		t.Fatal("fixture has no repeated cards; order is untested")
	}
}

func TestReplayStopsOnFailure(t *testing.T) {
	cfg := replay.Config{Generator: generator.Config{RunID: "lanes", Seed: 42, Day: "2026-10-08", Events: 200}, TPS: 100000, Workers: 4, Queue: 16}
	g, err := generator.New(cfg.Generator)
	if err != nil {
		t.Fatal(err)
	}
	first, err := g.At(5, 1)
	if err != nil {
		t.Fatal(err)
	}
	s, _, _ := gateway(t, contract.ID(first.TransactionID, Rules))
	out, err := Run(context.Background(), cfg, &Client{Base: s.URL, HTTP: s.Client()}, "latency", 8)
	if err == nil || out.Completed == 200 {
		t.Fatal("failure did not stop the run", out)
	}
}
