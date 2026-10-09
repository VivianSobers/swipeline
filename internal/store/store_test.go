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
