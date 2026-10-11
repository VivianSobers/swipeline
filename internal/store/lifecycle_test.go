package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/lifecycle"
)

func TestLifecycleMigrationEmbedded(t *testing.T) {
	content, err := migrations.ReadFile("migrations/007_lifecycle.sql")
	if err != nil {
		t.Fatalf("failed to read 007_lifecycle.sql: %v", err)
	}
	sqlStr := string(content)
	if !strings.Contains(sqlStr, "CREATE TABLE IF NOT EXISTS lifecycle_receipts") {
		t.Errorf("missing lifecycle_receipts table definition: %s", sqlStr)
	}
	if !strings.Contains(sqlStr, "idx_lifecycle_receipts_tx") {
		t.Errorf("missing idx_lifecycle_receipts_tx index: %s", sqlStr)
	}
}

func TestIngestBatchesMigrationEmbedded(t *testing.T) {
	content, err := migrations.ReadFile("migrations/008_ingest_batches.sql")
	if err != nil {
		t.Fatalf("failed to read 008_ingest_batches.sql: %v", err)
	}
	sqlStr := string(content)
	if !strings.Contains(sqlStr, "CREATE TABLE IF NOT EXISTS ingest_batches") {
		t.Errorf("missing ingest_batches table definition: %s", sqlStr)
	}
	for _, field := range []string{
		"batch_id text PRIMARY KEY",
		"starting_offsets jsonb NOT NULL",
		"ending_offsets jsonb NOT NULL",
		"journal_instant text",
		"transaction_instant text",
		"posting_instant text",
		"state text NOT NULL",
		"created_at timestamptz NOT NULL DEFAULT clock_timestamp()",
		"completed_at timestamptz",
	} {
		if !strings.Contains(sqlStr, field) {
			t.Errorf("ingest_batches missing field %q", field)
		}
	}
}

func TestReceiveLifecycleValidation(t *testing.T) {
	s := &Store{}
	// Empty/invalid event must fail validation before touching database
	invalid := lifecycle.Event{}
	_, err := s.ReceiveLifecycle(context.Background(), invalid)
	if err == nil {
		t.Fatal("expected validation error for empty lifecycle event")
	}
}

func TestReceiveLifecycleDatabase(t *testing.T) {
	s := database(t)
	_, e := fixture(t, s)
	ctx := context.Background()
	received := time.Now().UnixMilli()
	rows, err := s.Admit(ctx, []Input{{Event: e, Received: received}})
	if err != nil {
		t.Fatalf("Admit failed: %v", err)
	}
	req := rows[0].Request

	// Finalize with APPROVE
	proposal := &auth.Proposal{
		Request:    req,
		ProposalID: contract.ID(e.TransactionID, "prop"),
		Decision:   "APPROVE",
		Reason:     "OK",
		Flags:      []string{},
		Score:      10,
	}
	finals, err := s.Finalize(ctx, []Choice{{ID: e.TransactionID, Proposal: proposal}})
	if err != nil || finals[0].Final == nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// 1. Valid lifecycle event
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

	receipt, err := s.ReceiveLifecycle(ctx, lcEvt)
	if err != nil {
		t.Fatalf("ReceiveLifecycle failed: %v", err)
	}
	if receipt.EventID != lcEvt.EventID || receipt.CloseDay != req.CloseDay || receipt.SealEpoch != req.Epoch {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}

	// 2. Duplicate idempotent return
	againReceipt, err := s.ReceiveLifecycle(ctx, lcEvt)
	if err != nil {
		t.Fatalf("ReceiveLifecycle duplicate failed: %v", err)
	}
	if againReceipt != receipt {
		t.Fatalf("duplicate receipt mismatch: got %+v, want %+v", againReceipt, receipt)
	}

	// 3. Conflict rejection (same event_id, different payload hash)
	conflicting := lcEvt
	conflicting.PayloadHash = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = s.ReceiveLifecycle(ctx, conflicting)
	if !errors.Is(err, ErrLifecycleConflict) {
		t.Fatalf("expected ErrLifecycleConflict, got: %v", err)
	}

	// 4. Unknown transaction rejection
	unknownEvt := lcEvt
	unknownEvt.TransactionID = "unknown-tx-999"
	unknownEvt.EventID = contract.ID(unknownEvt.TransactionID, unknownEvt.Kind, "2")
	unknownEvt.PayloadHash = lcEvt.PayloadHash
	_, err = s.ReceiveLifecycle(ctx, unknownEvt)
	if !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("expected ErrUnknownTransaction, got: %v", err)
	}

	// 5. Declined transaction rejection
	declinedEvent := sibling(t, e, "declined-sibling")
	rows, err = s.Admit(ctx, []Input{{Event: declinedEvent, Received: received}})
	if err != nil {
		t.Fatalf("Admit declinedEvent failed: %v", err)
	}
	declineProp := &auth.Proposal{
		Request:    rows[0].Request,
		ProposalID: contract.ID(declinedEvent.TransactionID, "prop-dec"),
		Decision:   "DECLINE",
		Reason:     "SUSPECT",
		Flags:      []string{"VELOCITY"},
	}
	_, err = s.Finalize(ctx, []Choice{{ID: declinedEvent.TransactionID, Proposal: declineProp}})
	if err != nil {
		t.Fatalf("Finalize DECLINE failed: %v", err)
	}

	declinedLCEvt, err := lifecycle.New(
		declinedEvent.RunID,
		declinedEvent.TransactionID,
		declinedEvent.SourceSeq,
		2,
		string(lifecycle.KindClear),
		declinedEvent.EventID,
		received+2000,
		received+2000,
		declinedEvent.AuthorizationDate,
		declinedEvent.Currency,
		declinedEvent.AmountMinor,
		declinedEvent.MerchantID,
		declinedEvent.IssuerBankID,
		declinedEvent.AcquirerBankID,
		declinedEvent.ReferenceVersion,
		req.Epoch,
		req.CloseDay,
	)
	if err != nil {
		t.Fatalf("lifecycle.New declined failed: %v", err)
	}
	_, err = s.ReceiveLifecycle(ctx, declinedLCEvt)
	if !errors.Is(err, ErrDeclinedLifecycle) {
		t.Fatalf("expected ErrDeclinedLifecycle, got: %v", err)
	}
}
