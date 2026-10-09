package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VivianSobers/swipeline/internal/preview"
)

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
