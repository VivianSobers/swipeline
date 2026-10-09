package gateway

import (
	"context"
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
	if err := Recover(ctx, s.DB); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		row, err := s.DB.Lookup(ctx, id)
		if err != nil || row.Final == nil || row.Final.Source != "STAND_IN" {
			t.Fatal("detached admission not recovered", id, err)
		}
	}
}
