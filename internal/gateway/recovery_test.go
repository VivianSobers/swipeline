package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/store"
)

func TestDetachedRecovery(t *testing.T) {
	s, r, e := service(t)
	ctx := context.Background()
	if err := s.DB.Register(ctx, r); err != nil {
		t.Fatal(err)
	}
	// Resolve leftovers from earlier runs directly, so counters see only ours.
	for {
		rows, err := s.DB.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			if _, err = s.DB.Finalize(ctx, []store.Choice{{ID: r.Request.TransactionID}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// More than one recovery page of expired admissions, as after a crash.
	var ids []string
	for i := 0; i < 40; i += 8 {
		var batch []store.Input
		for j := i; j < i+8; j++ {
			b := e.Business
			b.TransactionID = contract.ID(e.TransactionID, string(rune('a'+j)))
			b.SourceSeq = int64(j)
			next, err := contract.New(b, 1)
			if err != nil {
				t.Fatal(err)
			}
			batch = append(batch, store.Input{Event: next, Received: time.Now().Add(-time.Second).UnixMilli()})
			ids = append(ids, next.TransactionID)
		}
		if _, err := s.DB.Admit(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		row, err := s.DB.Lookup(ctx, id)
		if err != nil || row.Final == nil || row.Final.Source != "STAND_IN" {
			t.Fatal("detached admission not recovered", id, err)
		}
	}
	// Recovery, repeated recovery and a late retry count each final once.
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Finals.Submit(ctx, store.Choice{ID: ids[0]}, time.Now()); err != nil {
		t.Fatal(err)
	}
	body := invoke(s, "GET", "/metrics", nil, "").Body.String()
	for _, line := range []string{
		`swipeline_gateway_final_decisions_total{decision_source="STAND_IN",decision="DECLINE"} 40`,
		`swipeline_gateway_final_decisions_total{decision_source="FLINK",decision="APPROVE"} 0`,
		`swipeline_gateway_standin_total{reason="SCORING_TIMEOUT"} 40`,
		`swipeline_gateway_standin_total{reason="NETWORK_UNAVAILABLE"} 0`,
	} {
		if !strings.Contains(body, line) {
			t.Fatalf("missing %q in\n%s", line, body)
		}
	}
}
