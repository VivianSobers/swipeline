package preview

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/replay"
)

func request() Request {
	return Request{replay.Config{Generator: generator.Config{RunID: "demo", Seed: 42, Day: "2026-10-08", Events: 20}, TPS: 100000, Workers: 2, Queue: 8}, "first"}
}

func post(t *testing.T, s *Service, r Request) *httptest.ResponseRecorder {
	t.Helper()
	data, _ := json.Marshal(r)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/replays", bytes.NewReader(data)))
	return w
}

func TestPublicationConflictsAndRestart(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	r := request()
	if w := post(t, s, r); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	original, err := os.ReadFile(filepath.Join(root, "previews", "first", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(original, []byte("\n")) != 20 {
		t.Fatal("not 20 events")
	}
	if post(t, s, r).Code != 409 {
		t.Fatal("overwrote existing preview")
	}
	s, err = New(root)
	if err != nil {
		t.Fatal(err)
	}
	r.Output = "second"
	r.Config.Generator.Seed++
	if post(t, s, r).Code != 409 {
		t.Fatal("lost durable namespace identity")
	}
	again, _ := os.ReadFile(filepath.Join(root, "previews", "first", "events.jsonl"))
	if !bytes.Equal(original, again) {
		t.Fatal("changed completed data")
	}
}

func TestInvalidNamesAndCancelledRun(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	for _, name := range []string{"../escape", "/absolute", "a/b", ""} {
		r := request()
		r.Output = name
		if post(t, s, r).Code != 400 {
			t.Fatal("unsafe path accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := request()
	data, _ := json.Marshal(r)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/replays", bytes.NewReader(data)).WithContext(ctx)
	s.Handler().ServeHTTP(w, req)
	if w.Code == 200 {
		t.Fatal("cancelled run succeeded")
	}
	if _, err := os.Stat(filepath.Join(root, "previews", "first")); !os.IsNotExist(err) {
		t.Fatal("partial published")
	}
	entries, _ := os.ReadDir(filepath.Join(root, "previews"))
	if len(entries) != 0 {
		t.Fatal("temporary files leaked on ordinary cancellation")
	}
}

func TestIOFailureAndIdleMetrics(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	previews := filepath.Join(root, "previews")
	if err := os.Remove(previews); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(previews, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if post(t, s, request()).Code != 500 {
		t.Fatal("write failure hidden")
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "swipeline_replayer_up 1") {
		t.Fatal(w.Body.String())
	}
	if strings.Contains(w.Body.String(), "run_id=") {
		t.Fatal("unbounded metric labels")
	}
}

func TestMetricsAccumulateAcrossRuns(t *testing.T) {
	s, _ := New(t.TempDir())
	r := request()
	if post(t, s, r).Code != 200 {
		t.Fatal("first failed")
	}
	r.Output = "second"
	if post(t, s, r).Code != 200 {
		t.Fatal("second failed")
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "swipeline_replayer_completed_total 40\n") {
		t.Fatal(w.Body.String())
	}
	s.runMu.Lock()
	r.Output = "third"
	w = post(t, s, r)
	s.runMu.Unlock()
	if w.Code != 409 {
		t.Fatal("overlapping run accepted")
	}
}
