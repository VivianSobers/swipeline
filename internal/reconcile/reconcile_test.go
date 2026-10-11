package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
)

func TestCompare(t *testing.T) {
	ctx := context.Background()

	gw := auth.RunCounts{RunID: "run-1", Admitted: 100, Final: 100, Pending: 0, StandIn: 5}
	d := DruidCounts{Count: 100, Distinct: 100}

	res := Compare(ctx, gw, d)
	if !res.Equal || res.Gateway != 100 || res.Druid != 100 || res.Distinct != 100 {
		t.Fatalf("unexpected compare result: %+v", res)
	}

	// Pending > 0 -> not equal
	gwPending := gw
	gwPending.Pending = 1
	resPending := Compare(ctx, gwPending, d)
	if resPending.Equal {
		t.Fatalf("expected Equal=false when pending > 0: %+v", resPending)
	}

	// Duplicates in Druid -> not equal
	dDup := DruidCounts{Count: 101, Distinct: 100}
	resDup := Compare(ctx, gw, dDup)
	if resDup.Equal {
		t.Fatalf("expected Equal=false when duplicates in Druid: %+v", resDup)
	}
}

func TestReconcileEqual(t *testing.T) {
	gwSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(auth.RunCounts{
			RunID:    "run-1",
			Admitted: 100,
			Final:    100,
			Pending:  0,
			StandIn:  5,
		})
	}))
	defer gwSrv.Close()

	druidSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"total_count": float64(100), "distinct_count": float64(100)},
		})
	}))
	defer druidSrv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(), gwSrv.URL, druidSrv.URL, "run-1", 500*time.Millisecond, &buf)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	var res Result
	if err := json.Unmarshal(buf.Bytes(), &res); err != nil {
		t.Fatalf("invalid json output: %s", buf.String())
	}
	if !res.Equal || res.Gateway != 100 || res.Druid != 100 || res.Distinct != 100 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestReconcileDuplicateRows(t *testing.T) {
	gwSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(auth.RunCounts{
			RunID:    "run-1",
			Admitted: 100,
			Final:    100,
			Pending:  0,
		})
	}))
	defer gwSrv.Close()

	druidSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"total_count": float64(105), "distinct_count": float64(100)},
		})
	}))
	defer druidSrv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(), gwSrv.URL, druidSrv.URL, "run-1", 500*time.Millisecond, &buf)
	if err == nil {
		t.Fatal("expected error for duplicate rows, got nil")
	}
	if !strings.Contains(err.Error(), "DUPLICATE_ROWS") {
		t.Fatalf("expected DUPLICATE_ROWS error, got: %v", err)
	}
}

func TestReconcileDruidBehindUntilLaterPoll(t *testing.T) {
	gwSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(auth.RunCounts{
			RunID:    "run-1",
			Admitted: 100,
			Final:    100,
			Pending:  0,
		})
	}))
	defer gwSrv.Close()

	var pollCount atomic.Int32
	druidSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cnt := pollCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		count := float64(80)
		if cnt >= 3 {
			count = 100
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"total_count": count, "distinct_count": count},
		})
	}))
	defer druidSrv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(), gwSrv.URL, druidSrv.URL, "run-1", 2*time.Second, &buf)
	if err != nil {
		t.Fatalf("expected success on later poll, got error: %v", err)
	}
	var res Result
	if err := json.Unmarshal(buf.Bytes(), &res); err != nil {
		t.Fatalf("invalid json output: %s", buf.String())
	}
	if !res.Equal || res.Gateway != 100 || res.Druid != 100 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if pollCount.Load() < 3 {
		t.Fatalf("expected at least 3 polls, got %d", pollCount.Load())
	}
}

func TestReconcileNeverEqual(t *testing.T) {
	gwSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(auth.RunCounts{
			RunID:    "run-1",
			Admitted: 100,
			Final:    100,
			Pending:  0,
		})
	}))
	defer gwSrv.Close()

	druidSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"total_count": float64(80), "distinct_count": float64(80)},
		})
	}))
	defer druidSrv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(), gwSrv.URL, druidSrv.URL, "run-1", 200*time.Millisecond, &buf)
	if err == nil {
		t.Fatal("expected error when never equal, got nil")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "100") || !strings.Contains(errStr, "80") {
		t.Fatalf("expected error to contain both gateway (100) and druid (80) numbers, got: %s", errStr)
	}
}
