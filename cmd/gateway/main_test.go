package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
)

func TestUnknownCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"bad"}, {"serve", "extra"}} {
		if run(context.Background(), args) == nil {
			t.Fatal("accepted", args)
		}
	}
}

// The real serve command, against live Postgres and Kafka without a
// streaming job: requests are admitted, relayed and answered with durable
// fail-closed stand-ins, and shutdown stops cleanly.
func TestServeAnswersWithDurableStandIn(t *testing.T) {
	dsn, brokers := os.Getenv("SWIPELINE_DB"), os.Getenv("SWIPELINE_KAFKA")
	if dsn == "" || brokers == "" {
		t.Skip("run make db-test")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("GATEWAY_LISTEN", addr)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"serve"}) }()
	base := "http://" + addr
	deadline := time.Now().Add(30 * time.Second)
	for {
		if r, err := http.Get(base + "/readyz"); err == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway never became ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
	id := contract.ID(t.Name(), time.Now().String())[:24]
	g, _ := generator.New(generator.Config{RunID: id, Seed: 42, Day: "2026-10-08", Events: 1000})
	e, _ := g.At(0, 1)
	manifest, _ := json.Marshal(auth.Run{ID: id, Identity: id, Day: e.AuthorizationDate, Rule: e.RuleVersion, Reference: e.ReferenceVersion,
		Mode: "latency", TPS: 50, Compression: 720, Concurrency: 128, Partitioner: "sha256-json-v1", Partitions: 4})
	if r, err := http.Post(base+"/v1/runs", "application/json", bytes.NewReader(manifest)); err != nil || r.StatusCode != http.StatusCreated {
		t.Fatal("register", err)
	}
	body, _ := json.Marshal(e)
	var f auth.Final
	for f.DecisionID == "" {
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/authorizations", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", e.TransactionID)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode == http.StatusOK {
			_ = json.NewDecoder(r.Body).Decode(&f)
		} else if r.StatusCode != http.StatusServiceUnavailable {
			t.Fatal("authorize", r.StatusCode)
		}
		r.Body.Close()
	}
	if f.Source != "STAND_IN" || f.Decision != "DECLINE" {
		t.Fatalf("%+v", f)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("shutdown:", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
	}
}
