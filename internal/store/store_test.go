package store

import (
	"context"
	"os"
	"testing"
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
