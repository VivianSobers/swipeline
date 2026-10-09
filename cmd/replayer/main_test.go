package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/preview"
)

func TestServeCancellationStopsReplay(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	data := t.TempDir()
	t.Setenv("SWIPELINE_URL", "http://"+addr)
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	var serveErr error
	go func() {
		defer close(serveDone)
		serveErr = run(ctx, []string{"serve", "--listen", addr, "--data", data}, io.Discard, io.Discard)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveDone:
		case <-time.After(7 * time.Second):
			t.Error("serve did not stop during cleanup")
		}
	})
	ready, stopReady := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopReady()
	for call(ready, "GET", "/healthz", nil, io.Discard) != nil {
		select {
		case <-serveDone:
			t.Fatalf("serve exited before readiness: %v", serveErr)
		case <-ready.Done():
			t.Fatal("service did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Keep the client alive independently of the service cancellation.
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	replayDone := make(chan struct{})
	var replayErr error
	go func() {
		defer close(replayDone)
		replayErr = run(requestCtx, []string{"replay", "--tps", "1", "--events", "100"}, io.Discard, io.Discard)
	}()
	t.Cleanup(func() {
		cancelRequest()
		select {
		case <-replayDone:
		case <-time.After(3 * time.Second):
			t.Error("replay request did not stop during cleanup")
		}
	})
	pending := filepath.Join(data, "previews", ".pending-*")
	deadline := time.After(3 * time.Second)
	for {
		matches, err := filepath.Glob(pending)
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) == 1 {
			info, err := os.Stat(filepath.Join(matches[0], "events.jsonl"))
			if err == nil && info.Size() > 0 {
				break
			}
		}
		select {
		case <-replayDone:
			t.Fatalf("replay exited before cancellation: %v", replayErr)
		case <-deadline:
			t.Fatal("replay did not start writing")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-serveDone:
		if serveErr != nil {
			t.Fatalf("serve returned an error on cancellation: %v", serveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return within 2 seconds of cancellation")
	}
	// Check immediately on return, before waiting for the client.
	if matches, err := filepath.Glob(pending); err != nil || len(matches) != 0 {
		t.Fatalf("pending previews remain after serve returned: %v (error: %v)", matches, err)
	}
	select {
	case <-replayDone:
		if replayErr == nil {
			t.Fatal("replay request succeeded after service cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("replay request did not finish after service shutdown")
	}
}

func TestReplayFlagsAndStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			fmt.Fprintln(w, `{"state":"idle"}`)
			return
		}
		var req preview.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if r.Method != "POST" || r.URL.Path != "/v1/replays" || req.Config.Generator.Seed != 42 || req.Config.TPS != 50 || req.Output != "first" {
			t.Errorf("bad request %+v", req)
		}
		fmt.Fprintln(w, `{"state":"complete"}`)
	}))
	defer server.Close()
	t.Setenv("SWIPELINE_URL", server.URL)
	var out, errs bytes.Buffer
	if err := run(context.Background(), []string{"replay", "--output", "first"}, &out, &errs); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\"state\":\"complete\"}\n" {
		t.Fatal(out.String())
	}
	out.Reset()
	if err := run(context.Background(), []string{"status"}, &out, &errs); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\"state\":\"idle\"}\n" {
		t.Fatal(out.String())
	}
}

func TestInvalidCLIAndUnavailableService(t *testing.T) {
	var out, errs bytes.Buffer
	for _, args := range [][]string{{}, {"authorize"}, {"replay", "--source", "csv"}, {"replay", "--tps", "NaN"}, {"replay", "--events", "0"}, {"replay", "extra"}, {"status", "extra"}} {
		if run(context.Background(), args, &out, &errs) == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "conflict", 409) }))
	t.Setenv("SWIPELINE_URL", server.URL)
	if run(context.Background(), []string{"status"}, &out, &errs) == nil {
		t.Fatal("ignored HTTP error")
	}
	server.Close()
	if run(context.Background(), []string{"health"}, &out, &errs) == nil {
		t.Fatal("ignored unavailable service")
	}
}
