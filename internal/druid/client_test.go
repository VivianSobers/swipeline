package druid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestInstallIdempotent(t *testing.T) {
	specData, err := os.ReadFile("../../deploy/druid/supervisor.json")
	if err != nil {
		t.Fatal(err)
	}

	var postCount atomic.Int32
	var getCount atomic.Int32
	var installed atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/druid/indexer/v1/supervisor/swipeline_decisions":
			getCount.Add(1)
			if !installed.Load() {
				http.Error(w, `{"error":"No supervisor found for [swipeline_decisions]"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			var raw map[string]any
			_ = json.Unmarshal(specData, &raw)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":   "swipeline_decisions",
				"spec": raw["spec"],
			})
		case r.Method == http.MethodPost && r.URL.Path == "/druid/indexer/v1/supervisor":
			postCount.Add(1)
			installed.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"swipeline_decisions"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := &Client{
		Base: srv.URL,
		HTTP: srv.Client(),
	}

	created, err := client.Install(context.Background(), specData)
	if err != nil {
		t.Fatalf("unexpected error on first install: %v", err)
	}
	if !created {
		t.Fatalf("expected created == true on first install")
	}
	if postCount.Load() != 1 {
		t.Fatalf("expected 1 POST, got %d", postCount.Load())
	}

	created2, err := client.Install(context.Background(), specData)
	if err != nil {
		t.Fatalf("unexpected error on second install: %v", err)
	}
	if created2 {
		t.Fatalf("expected created == false on second install")
	}
	if postCount.Load() != 1 {
		t.Fatalf("expected post count to remain 1, got %d", postCount.Load())
	}
}

func TestRefuseChangedSupervisor(t *testing.T) {
	specData, err := os.ReadFile("../../deploy/druid/supervisor.json")
	if err != nil {
		t.Fatal(err)
	}

	var postCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/druid/indexer/v1/supervisor/swipeline_decisions":
			w.Header().Set("Content-Type", "application/json")
			var raw map[string]any
			_ = json.Unmarshal(specData, &raw)
			specObj := raw["spec"].(map[string]any)
			ioCfg := specObj["ioConfig"].(map[string]any)
			ioCfg["taskCount"] = 2
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":   "swipeline_decisions",
				"spec": specObj,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/druid/indexer/v1/supervisor":
			postCount.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := &Client{
		Base: srv.URL,
		HTTP: srv.Client(),
	}

	created, err := client.Install(context.Background(), specData)
	if !errors.Is(err, ErrSpecConflict) {
		t.Fatalf("expected ErrSpecConflict, got created=%v, err=%v", created, err)
	}
	if created {
		t.Fatalf("expected created == false on conflict")
	}
	if postCount.Load() != 0 {
		t.Fatalf("expected 0 POST on conflict, got %d", postCount.Load())
	}
}

func TestClientSQL(t *testing.T) {
	var receivedQuery string
	var receivedParams []map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/druid/v2/sql" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "invalid content type", http.StatusBadRequest)
			return
		}
		var doc struct {
			Query      string           `json:"query"`
			Parameters []map[string]any `json:"parameters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		receivedQuery = doc.Query
		receivedParams = doc.Parameters

		if doc.Query == "FAIL" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"plan failed","errorMessage":"column not found"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"total_count": float64(42), "distinct_count": float64(42)},
		})
	}))
	defer srv.Close()

	client := &Client{
		Base: srv.URL,
		HTTP: srv.Client(),
	}

	ctx := context.Background()
	rows, err := client.SQL(ctx, "SELECT COUNT(*) FROM swipeline_decisions WHERE run_id = ?", "test-run")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if receivedQuery != "SELECT COUNT(*) FROM swipeline_decisions WHERE run_id = ?" {
		t.Fatalf("unexpected query: %s", receivedQuery)
	}
	if len(receivedParams) != 1 || receivedParams[0]["value"] != "test-run" || receivedParams[0]["type"] != "VARCHAR" {
		t.Fatalf("unexpected parameters: %+v", receivedParams)
	}

	_, err = client.SQL(ctx, "FAIL")
	if err == nil {
		t.Fatal("expected error on HTTP 400, got nil")
	}
	if !strings.Contains(err.Error(), "column not found") {
		t.Fatalf("expected error to contain Druid error message 'column not found', got: %v", err)
	}

	counts, err := client.DecisionCounts(ctx, "test-run")
	if err != nil {
		t.Fatalf("DecisionCounts failed: %v", err)
	}
	if counts.Count != 42 || counts.Distinct != 42 {
		t.Fatalf("unexpected DecisionCounts: %+v", counts)
	}
}

func TestDecisionCountsRequiresExactDistinct(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Context map[string]any `json:"context"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if value, ok := payload.Context["useApproximateCountDistinct"]; !ok || value != false {
			t.Error("reconciliation must disable approximate distinct counts")
		}
		fmt.Fprint(w, `[{"total_count":1000,"distinct_count":1000}]`)
	}))
	defer server.Close()
	if _, err := NewClient(server.URL).DecisionCounts(context.Background(), "run-1"); err != nil {
		t.Fatal(err)
	}
}

func TestDecisionCountsRejectsMalformedAggregates(t *testing.T) {
	for _, response := range []string{`[]`, `[{}]`, `[{"total_count":null,"distinct_count":null}]`, `[{"total_count":1.5,"distinct_count":1}]`, `[{"total_count":-1,"distinct_count":-1}]`, `[{"total_count":1,"distinct_count":2}]`, `[{"total_count":"1","distinct_count":1}]`, `[{"total_count":1,"distinct_count":1},{"total_count":1,"distinct_count":1}]`} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, response) }))
			defer server.Close()
			if counts, err := NewClient(server.URL).DecisionCounts(context.Background(), "run-1"); err == nil {
				t.Fatalf("accepted malformed counts: %+v", counts)
			}
		})
	}
}

func TestDecisionCountsPreservesIntegerPrecision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"total_count":9007199254740993,"distinct_count":9007199254740993}]`)
	}))
	defer server.Close()
	counts, err := NewClient(server.URL).DecisionCounts(context.Background(), "run-1")
	if err != nil || counts.Count != 9007199254740993 || counts.Distinct != 9007199254740993 {
		t.Fatalf("lost exact count: %+v, %v", counts, err)
	}
}

func TestDecisionCountsReturnsNotReadyOnNull(t *testing.T) {
	for _, response := range []string{
		`[{"total_count":null,"distinct_count":null}]`,
		`[{"total_count":null,"distinct_count":10}]`,
		`[{"total_count":10,"distinct_count":null}]`,
	} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, response)
			}))
			defer server.Close()

			_, err := NewClient(server.URL).DecisionCounts(context.Background(), "run-1")
			if !errors.Is(err, ErrNotReady) {
				t.Fatalf("expected ErrNotReady, got: %v", err)
			}
		})
	}
}
