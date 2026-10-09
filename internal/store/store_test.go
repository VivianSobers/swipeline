package store

import (
	"context"
	"database/sql"
	"encoding/json"
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

func admitAt(t *testing.T, s *Store, e contract.Event, received time.Time) auth.Request {
	t.Helper()
	rows, err := s.Admit(context.Background(), []Input{{Event: e, Received: received.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}
	return rows[0].Request
}

func approval(r auth.Request) *auth.Proposal {
	return &auth.Proposal{Request: r, ProposalID: contract.ID(r.TransactionID, "proposal"), Decision: "APPROVE", Reason: "RULES_CLEAR",
		Flags: []string{}, Score: 0, Velocity: 1, Usual: 100, Ratio: 1, Distinct: 1, Scored: r.Received}
}

func finalize(t *testing.T, s *Store, c Choice) Row {
	t.Helper()
	rows, err := s.Finalize(context.Background(), []Choice{c})
	if err != nil {
		t.Fatal(err)
	}
	return rows[0]
}

func TestLateProposalCannotWin(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	r := admitAt(t, s, e, time.Now().Add(-time.Second))
	got := finalize(t, s, Choice{ID: e.TransactionID, Proposal: approval(r)})
	f := got.Final
	if f == nil || f.Source != "STAND_IN" || f.Decision != "DECLINE" || f.Reason != "SCORING_TIMEOUT" || len(f.Flags) != 0 ||
		f.ProposalID != nil || f.Score != nil || f.Late != nil || f.DecisionID != contract.ID(e.TransactionID, "decision") ||
		f.Request != r || f.Latency < 1000 {
		t.Fatalf("%+v", got)
	}
	again := finalize(t, s, Choice{ID: e.TransactionID, Proposal: approval(r)})
	a, _ := json.Marshal(got.Final)
	b, _ := json.Marshal(again.Final)
	if string(a) != string(b) {
		t.Fatal("final changed")
	}
	var n int
	if err := s.DB.QueryRow("SELECT count(*) FROM outbox WHERE event_id=$1 AND topic='swipeline.decisions.raw.v1'", f.DecisionID).Scan(&n); err != nil || n != 1 {
		t.Fatal("decision outbox", n, err)
	}
}

func TestTimelyProposalWinsOnce(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	r := admitAt(t, s, e, time.Now().Add(time.Minute))
	if row := finalize(t, s, Choice{ID: e.TransactionID}); row.Final != nil {
		t.Fatal("timer finalized before the scoring deadline")
	}
	p := approval(r)
	got := finalize(t, s, Choice{ID: e.TransactionID, Proposal: p})
	f := got.Final
	if f == nil || f.Source != "FLINK" || f.Decision != "APPROVE" || f.Reason != "RULES_CLEAR" || f.ProposalID == nil ||
		*f.ProposalID != p.ProposalID || f.Usual == nil || *f.Usual != 100 || f.Late == nil || *f.Late || f.Latency != 0 {
		t.Fatalf("%+v", f)
	}
	decline := approval(r)
	decline.Decision, decline.ProposalID = "DECLINE", "second"
	if again := finalize(t, s, Choice{ID: e.TransactionID, Proposal: decline}); again.Final.Decision != "APPROVE" {
		t.Fatal("second proposal replaced the final")
	}
}

func TestForeignProposalRejected(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	r := admitAt(t, s, e, time.Now().Add(time.Minute))
	for name, mutate := range map[string]func(*auth.Proposal){
		"payload":  func(p *auth.Proposal) { p.PayloadHash = contract.ID("other") },
		"rule":     func(p *auth.Proposal) { p.RuleVersion = "rules-v9" },
		"decision": func(p *auth.Proposal) { p.Decision = "REVIEW" },
		"flags":    func(p *auth.Proposal) { p.Flags = nil },
		"id":       func(p *auth.Proposal) { p.ProposalID = "" },
	} {
		p := approval(r)
		mutate(p)
		if _, err := s.Finalize(context.Background(), []Choice{{ID: e.TransactionID, Proposal: p}}); err == nil {
			t.Fatalf("%s mismatch accepted", name)
		}
	}
	if row, err := s.Lookup(context.Background(), e.TransactionID); err != nil || row.Final != nil {
		t.Fatal("rejected proposal changed the row", row, err)
	}
}

func TestFinalMutationRejected(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	admitAt(t, s, e, time.Now().Add(-time.Second))
	finalize(t, s, Choice{ID: e.TransactionID})
	for _, statement := range []string{
		"UPDATE authorizations SET final_json='{}' WHERE transaction_id=$1",
		"UPDATE authorizations SET status='PENDING', final_json=NULL WHERE transaction_id=$1",
		"DELETE FROM authorizations WHERE transaction_id=$1",
	} {
		if _, err := s.DB.Exec(statement, e.TransactionID); err == nil {
			t.Fatalf("mutated final: %s", statement)
		}
	}
	other := sibling(t, e, "pending")
	admitAt(t, s, other, time.Now().Add(time.Minute))
	if _, err := s.DB.Exec("UPDATE authorizations SET seal_epoch=seal_epoch WHERE transaction_id=$1", other.TransactionID); err != nil {
		t.Fatal("pending rows must stay writable:", err)
	}
}

func TestPendingRecovery(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	ctx := context.Background()
	admitAt(t, s, e, time.Now().Add(-time.Second))
	waiting := sibling(t, e, "waiting")
	admitAt(t, s, waiting, time.Now().Add(time.Minute))
	recovered := false
	for page := 0; ; page++ {
		rows, err := s.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) > 32 || page > 10000 {
			t.Fatal("unbounded recovery page")
		}
		if len(rows) == 0 {
			break
		}
		choices := make([]Choice, len(rows))
		for i, r := range rows {
			if r.Request.TransactionID == waiting.TransactionID {
				t.Fatal("recovered an admission before its deadline")
			}
			recovered = recovered || r.Request.TransactionID == e.TransactionID
			choices[i] = Choice{ID: r.Request.TransactionID}
		}
		finals, err := s.Finalize(ctx, choices)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range finals {
			if f.Final == nil || f.Final.Source != "STAND_IN" {
				t.Fatalf("expired admission not resolved: %+v", f)
			}
		}
	}
	if !recovered {
		t.Fatal("lost detached admission")
	}
	if row, err := s.Lookup(ctx, waiting.TransactionID); err != nil || row.Final != nil {
		t.Fatal("unexpired admission finalized", err)
	}
}

func TestOnlyTheCommittingCallIsFresh(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	admitAt(t, s, e, time.Now().Add(-time.Second))
	if first := finalize(t, s, Choice{ID: e.TransactionID}); !first.Fresh {
		t.Fatal("committing call not fresh")
	}
	if again := finalize(t, s, Choice{ID: e.TransactionID}); again.Fresh || again.Final == nil {
		t.Fatal("repeat reported fresh")
	}
	waiting := sibling(t, e, "waiting")
	admitAt(t, s, waiting, time.Now().Add(time.Minute))
	if early := finalize(t, s, Choice{ID: waiting.TransactionID}); early.Fresh {
		t.Fatal("no-op reported fresh")
	}
}
