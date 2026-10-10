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
	"sync"
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

type countingWriter struct {
	*os.File
	mu     sync.Mutex
	writes int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.File.Write(p)
}

func (c *countingWriter) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func TestBufferedEventWrites(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	cw := &countingWriter{}
	s.openEvents = func(path string) (eventWriter, error) {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		cw.File = f
		return cw, nil
	}
	r := request()
	r.Config.Generator.Events = 300
	if w := post(t, s, r); w.Code != 200 {
		t.Fatalf("replay failed: %d %s", w.Code, w.Body.String())
	}
	writes := cw.writeCount()
	if writes >= 300 || writes > 20 {
		t.Fatalf("expected far fewer writes than events (buffered): got %d writes for 300 events", writes)
	}
	data, err := os.ReadFile(filepath.Join(root, "previews", "first", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(data, []byte("\n")) != 300 {
		t.Fatalf("got %d events in file, want 300", bytes.Count(data, []byte("\n")))
	}
}
