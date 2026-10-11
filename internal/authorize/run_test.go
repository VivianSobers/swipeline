package authorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/lifecycle"
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
		if r.URL.Path == "/v1/lifecycle" {
			var evt lifecycle.Event
			_ = json.NewDecoder(r.Body).Decode(&evt)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(lifecycle.Receipt{
				EventID:    evt.EventID,
				SealEpoch:  1,
				CloseDay:   "2026-10-08",
				ReceivedAt: time.Now().UnixMilli(),
			})
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

func TestReplaySchedulesAndDrainsLifecycleEvents(t *testing.T) {
	var lifecycleMu sync.Mutex
	var lifecycleReceived []lifecycle.Event

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/runs" {
			w.WriteHeader(http.StatusCreated)
			return
		}
		if r.URL.Path == "/v1/lifecycle" {
			var evt lifecycle.Event
			if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
				t.Errorf("decode lifecycle: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			lifecycleMu.Lock()
			lifecycleReceived = append(lifecycleReceived, evt)
			lifecycleMu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(lifecycle.Receipt{
				EventID:    evt.EventID,
				SealEpoch:  1,
				CloseDay:   "2026-10-08",
				ReceivedAt: time.Now().UnixMilli(),
			})
			return
		}
		var e contract.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			t.Error("invalid event", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(auth.Final{
			Request:    auth.Request{Event: e},
			DecisionID: contract.ID(e.TransactionID, "decision"),
			Decision:   "APPROVE",
			Source:     "FLINK",
		})
	}))
	defer s.Close()

	queueDir := t.TempDir()
	cfg := replay.Config{Generator: generator.Config{RunID: "lc-replay", Seed: 42, Day: "2026-10-08", Events: 200}, TPS: 100000, Workers: 4, Queue: 16}
	c := &Client{Base: s.URL, HTTP: s.Client()}
	out, err := Run(context.Background(), cfg, c, "latency", 4, WithQueueDir(queueDir))
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if out.Approved != 200 {
		t.Fatalf("out.Approved = %d, want 200", out.Approved)
	}
	if out.Lifecycle == 0 {
		t.Fatalf("out.Lifecycle = 0, want > 0")
	}
	lifecycleMu.Lock()
	receivedCount := len(lifecycleReceived)
	lifecycleMu.Unlock()
	if int64(receivedCount) != out.Lifecycle {
		t.Fatalf("received %d lifecycle events, summary reported %d", receivedCount, out.Lifecycle)
	}
}

func TestReplayRestartRedrainsPendingQueue(t *testing.T) {
	var lifecycleMu sync.Mutex
	var lifecycleReceived []string

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/runs" {
			w.WriteHeader(http.StatusCreated)
			return
		}
		if r.URL.Path == "/v1/lifecycle" {
			var evt lifecycle.Event
			_ = json.NewDecoder(r.Body).Decode(&evt)
			lifecycleMu.Lock()
			lifecycleReceived = append(lifecycleReceived, evt.EventID)
			lifecycleMu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(lifecycle.Receipt{
				EventID:    evt.EventID,
				SealEpoch:  1,
				CloseDay:   "2026-10-08",
				ReceivedAt: time.Now().UnixMilli(),
			})
			return
		}
		var e contract.Event
		_ = json.NewDecoder(r.Body).Decode(&e)
		_ = json.NewEncoder(w).Encode(auth.Final{
			Request:    auth.Request{Event: e},
			DecisionID: contract.ID(e.TransactionID, "decision"),
			Decision:   "APPROVE",
			Source:     "FLINK",
		})
	}))
	defer s.Close()

	queueDir := t.TempDir()
	// Pre-enqueue unacknowledged items in the queue to simulate prior crash
	q, err := lifecycle.OpenQueue(queueDir)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}
	preEvt := lifecycle.Event{
		EventID:           "pre-existing-evt",
		RunID:             "restart-run",
		TransactionID:     "pre-tx",
		LifecycleSeq:      2,
		Kind:              string(lifecycle.KindClear),
		ParentEventID:     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		AmountMinor:       1000,
		Currency:          "USD",
		AuthorizationDate: "2026-10-08",
		CloseDay:          "2026-10-08",
		MerchantID:        "m1",
		IssuerBankID:      "b1",
		AcquirerBankID:    "b2",
		ReferenceVersion:  "ref-v1",
		SealEpoch:         1,
	}
	if err := q.Enqueue([]lifecycle.Event{preEvt}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cfg := replay.Config{Generator: generator.Config{RunID: "restart-run", Seed: 42, Day: "2026-10-08", Events: 10}, TPS: 100000, Workers: 2, Queue: 8}
	c := &Client{Base: s.URL, HTTP: s.Client()}
	out, err := Run(context.Background(), cfg, c, "latency", 2, WithQueueDir(queueDir))
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if out.Approved != 10 {
		t.Fatalf("out.Approved = %d, want 10", out.Approved)
	}

	// Verify that the pre-existing event was drained
	lifecycleMu.Lock()
	foundPre := false
	for _, id := range lifecycleReceived {
		if id == "pre-existing-evt" {
			foundPre = true
			break
		}
	}
	lifecycleMu.Unlock()
	if !foundPre {
		t.Fatalf("pre-existing event was not drained upon replay restart")
	}
}
