package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
)

func TestPoolBound(t *testing.T) {
	dsn := os.Getenv("SWIPELINE_DB")
	if dsn == "" {
		t.Skip("run make db-test")
	}
	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	if s.DB.Stats().MaxOpenConnections != 12 {
		t.Fatal("unbounded pool")
	}
}

func database(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("SWIPELINE_DB")
	if dsn == "" {
		t.Skip("run make db-test")
	}
	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DB.Close() })
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestControlSchema(t *testing.T) {
	s := database(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal("migrations are not rerunnable:", err)
	}
	var n int
	err := s.DB.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema='public'
		AND table_name IN ('runs','authorizations','outbox','card_sequences','schema_migrations')`).Scan(&n)
	if err != nil || n != 5 {
		t.Fatalf("%d %v", n, err)
	}
}

// fixture registers a fresh run and returns it with its first event.
func fixture(t *testing.T, s *Store) (auth.Run, contract.Event) {
	t.Helper()
	id := contract.ID(t.Name(), time.Now().String())[:24]
	g, err := generator.New(generator.Config{RunID: id, Seed: 42, Day: "2026-10-08", Events: 1000})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := auth.Run{ID: id, Identity: contract.ID("fixture"), Day: e.AuthorizationDate, Rule: e.RuleVersion, Reference: e.ReferenceVersion,
		Mode: "latency", TPS: 50, Compression: 720, Concurrency: 128, Partitioner: "sha256-json-v1", Partitions: 4}
	if err = s.Register(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	return r, e
}

func TestRunConflict(t *testing.T) {
	s := database(t)
	r, _ := fixture(t, s)
	if err := s.Register(context.Background(), r); err != nil {
		t.Fatal("identical re-registration rejected:", err)
	}
	r.Identity = "changed"
	if s.Register(context.Background(), r) == nil {
		t.Fatal("changed run accepted")
	}
	r.Mode = "adaptive"
	if s.Register(context.Background(), r) == nil {
		t.Fatal("invalid run accepted")
	}
}
