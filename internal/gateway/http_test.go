package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/store"
)

func service(t *testing.T) (*Service, auth.Run, contract.Event) {
	t.Helper()
	dsn := os.Getenv("SWIPELINE_DB")
	if dsn == "" {
		t.Skip("run make db-test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DB.Close() })
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := contract.ID(t.Name(), time.Now().String())[:24]
	g, err := generator.New(generator.Config{RunID: id, Seed: 42, Day: "2026-10-08", Events: 1000})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := auth.Run{ID: id, Identity: id, Day: e.AuthorizationDate, Rule: e.RuleVersion, Reference: e.ReferenceVersion,
		Mode: "latency", TPS: 50, Compression: 720, Concurrency: 128, Partitioner: "sha256-json-v1", Partitions: 4}
	return New(ctx, db), r, e
}

func invoke(s *Service, method, path string, v any, key string) *httptest.ResponseRecorder {
	var payload []byte
	switch b := v.(type) {
	case nil:
	case []byte:
		payload = b
	default:
		payload, _ = json.Marshal(v)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(payload))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestRegisterHTTP(t *testing.T) {
	s, r, _ := service(t)
	for range 2 {
		if w := invoke(s, http.MethodPost, "/v1/runs", r, ""); w.Code != http.StatusCreated {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	changed := r
	changed.Identity = "different"
	if w := invoke(s, http.MethodPost, "/v1/runs", changed, ""); w.Code != http.StatusConflict {
		t.Fatal("changed manifest", w.Code)
	}
	invalid := r
	invalid.Partitions = 6
	if w := invoke(s, http.MethodPost, "/v1/runs", invalid, ""); w.Code != http.StatusBadRequest {
		t.Fatal("invalid manifest", w.Code)
	}
	data, _ := json.Marshal(r)
	if w := invoke(s, http.MethodPost, "/v1/runs", append(data, data...), ""); w.Code != http.StatusBadRequest {
		t.Fatal("trailing data", w.Code)
	}
}
