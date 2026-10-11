package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/lifecycle"
)

func TestReceiveLifecycleAtomicRollback(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	ctx := context.Background()
	received := time.Now().UnixMilli()
	rows, err := s.Admit(ctx, []Input{{Event: e, Received: received}})
	if err != nil {
		t.Fatalf("Admit failed: %v", err)
	}
	req := rows[0].Request

	proposal := &auth.Proposal{
		Request:    req,
		ProposalID: contract.ID(e.TransactionID, "prop"),
		Decision:   "APPROVE",
		Reason:     "OK",
		Flags:      []string{},
		Score:      10,
	}
	if _, err := s.Finalize(ctx, []Choice{{ID: e.TransactionID, Proposal: proposal}}); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	lcEvt, err := lifecycle.New(
		e.RunID,
		e.TransactionID,
		e.SourceSeq,
		2,
		string(lifecycle.KindClear),
		e.EventID,
		received+1000,
		received+1000,
		e.AuthorizationDate,
		e.Currency,
		e.AmountMinor,
		e.MerchantID,
		e.IssuerBankID,
		e.AcquirerBankID,
		e.ReferenceVersion,
		req.Epoch,
		req.CloseDay,
	)
	if err != nil {
		t.Fatalf("lifecycle.New failed: %v", err)
	}

	// 1. Inject failure before commit
	errInjected := errors.New("injected commit failure")
	failCtx := FailBeforeCommitInject(ctx, errInjected)
	_, err = s.ReceiveLifecycle(failCtx, lcEvt)
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error %v, got: %v", errInjected, err)
	}

	// 2. Assert no partial row exists in lifecycle_receipts or outbox
	var lcCount, outboxCount int
	if err := s.DB.QueryRowContext(ctx, "SELECT count(*) FROM lifecycle_receipts WHERE event_id = $1", lcEvt.EventID).Scan(&lcCount); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT count(*) FROM outbox WHERE event_id = $1", lcEvt.EventID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if lcCount != 0 {
		t.Fatalf("partial write in lifecycle_receipts after rollback: count=%d", lcCount)
	}
	if outboxCount != 0 {
		t.Fatalf("partial write in outbox after rollback: count=%d", outboxCount)
	}

	// 3. Subsequent retry succeeds cleanly without duplicate key error
	receipt, err := s.ReceiveLifecycle(ctx, lcEvt)
	if err != nil {
		t.Fatalf("subsequent retry failed: %v", err)
	}
	if receipt.EventID != lcEvt.EventID {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}

	if err := s.DB.QueryRowContext(ctx, "SELECT count(*) FROM lifecycle_receipts WHERE event_id = $1", lcEvt.EventID).Scan(&lcCount); err != nil || lcCount != 1 {
		t.Fatalf("expected 1 receipt, got %d (err: %v)", lcCount, err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT count(*) FROM outbox WHERE event_id = $1", lcEvt.EventID).Scan(&outboxCount); err != nil || outboxCount != 1 {
		t.Fatalf("expected 1 outbox record, got %d (err: %v)", outboxCount, err)
	}
}
