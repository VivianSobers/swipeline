package store

import (
	"context"
	"database/sql"
	"errors"
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

// sibling returns a new valid transaction for the same card as e.
func sibling(t *testing.T, e contract.Event, name string) contract.Event {
	t.Helper()
	b := e.Business
	b.TransactionID = contract.ID(e.TransactionID, name)
	b.SourceSeq++
	next, err := contract.New(b, e.ProducedAtMS)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestAdmissionOutboxAndRetry(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	ctx := context.Background()
	received := time.Now().UnixMilli()
	rows, err := s.Admit(ctx, []Input{{Event: e, Received: received}})
	if err != nil {
		t.Fatal(err)
	}
	got := rows[0].Request
	if rows[0].Final != nil || got.CardSeq != 1 || got.Received != received || got.Scoring != received+35 ||
		got.Response != received+50 || got.Mode != "latency" || got.CloseDay != e.AuthorizationDate || got.Event != e {
		t.Fatalf("receipt %+v", rows[0])
	}
	again, err := s.Admit(ctx, []Input{{Event: e, Received: received + 5000}})
	if err != nil {
		t.Fatal(err)
	}
	if again[0].Request != got {
		t.Fatal("retry changed receipt")
	}
	var n, partition int
	var topic string
	err = s.DB.QueryRow("SELECT count(*), min(topic), min(partition) FROM outbox WHERE event_id=$1", e.EventID).Scan(&n, &topic, &partition)
	if err != nil || n != 1 || topic != "swipeline.auth.requests.v1" || int32(partition) != auth.Partition(auth.Key(e.RunID, e.CardToken)) {
		t.Fatal(n, topic, partition, err)
	}
	next, err := s.Admit(ctx, []Input{{Event: sibling(t, e, "second"), Received: received}})
	if err != nil || next[0].Request.CardSeq != 2 {
		t.Fatal("card sequence", next, err)
	}
	changed := e.Business
	changed.AmountMinor++
	conflict, err := contract.New(changed, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Admit(ctx, []Input{{Event: conflict, Received: received}}); err == nil {
		t.Fatal("changed payload accepted")
	}
	stranger := e.Business
	stranger.RunID = "unregistered"
	stranger.TransactionID = contract.ID("unregistered")
	orphan, err := contract.New(stranger, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Admit(ctx, []Input{{Event: orphan, Received: received}}); err == nil {
		t.Fatal("unregistered run accepted")
	}
}

func TestOutboxPressureStopsAdmission(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	ctx := context.Background()
	if _, err := s.Admit(ctx, []Input{{Event: e, Received: time.Now().UnixMilli()}}); err != nil {
		t.Fatal(err)
	}
	if !s.Full(ctx, 0, 0) {
		t.Fatal("cap ignored")
	}
	if s.Full(ctx, 1<<40, 1<<40) {
		t.Fatal("false pressure")
	}
	var backlog int64
	if err := s.DB.QueryRow("SELECT (SELECT count(*) FROM outbox WHERE sent_at IS NULL) + (SELECT count(*) FROM authorizations WHERE status='PENDING')").Scan(&backlog); err != nil {
		t.Fatal(err)
	}
	s.MaxRows = backlog + 1 // one more admission needs two rows of headroom
	next := sibling(t, e, "pressured")
	if _, err := s.Admit(ctx, []Input{{Event: next, Received: time.Now().UnixMilli()}}); !errors.Is(err, ErrFull) {
		t.Fatalf("admitted under pressure: %v", err)
	}
	if _, err := s.Lookup(ctx, next.TransactionID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected admission left a row: %v", err)
	}
}
