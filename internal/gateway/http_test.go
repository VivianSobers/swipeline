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

func TestRunManifestHTTP(t *testing.T) {
	s, r, _ := service(t)
	if w := invoke(s, http.MethodGet, "/v1/runs/"+r.ID, nil, ""); w.Code != http.StatusNotFound {
		t.Fatal("unknown run", w.Code)
	}
	if err := s.DB.Register(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	w := invoke(s, http.MethodGet, "/v1/runs/"+r.ID, nil, "")
	var got auth.Run
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil || got != r {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestStatusHTTP(t *testing.T) {
	s, r, e := service(t)
	ctx := context.Background()
	path := "/v1/authorizations/" + e.TransactionID
	if w := invoke(s, http.MethodGet, path, nil, ""); w.Code != http.StatusNotFound {
		t.Fatal("unknown", w.Code)
	}
	if err := s.DB.Register(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Admit(ctx, []store.Input{{Event: e, Received: time.Now().Add(-time.Second).UnixMilli()}}); err != nil {
		t.Fatal(err)
	}
	var pending auth.Request
	if w := invoke(s, http.MethodGet, path, nil, ""); w.Code != http.StatusAccepted || json.Unmarshal(w.Body.Bytes(), &pending) != nil || pending.Event != e {
		t.Fatal("pending", w.Code, w.Body.String())
	}
	if _, err := s.DB.Finalize(ctx, []store.Choice{{ID: e.TransactionID}}); err != nil {
		t.Fatal(err)
	}
	var final auth.Final
	if w := invoke(s, http.MethodGet, path, nil, ""); w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &final) != nil || final.Source != "STAND_IN" {
		t.Fatal("final", w.Code, w.Body.String())
	}
}
