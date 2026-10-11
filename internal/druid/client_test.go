package druid

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
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
