package inspect

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInspectClearedAndReversedTransactions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txID := r.URL.Query().Get("transaction_id")
		w.Header().Set("Content-Type", "application/json")
		switch txID {
		case "tx-cleared":
			_ = json.NewEncoder(w).Encode(Result{
				TransactionID: "tx-cleared",
				Status:        "CLEARED",
				Amounts: Amounts{
					AuthorizedAmount:  5000,
					ClearedAmount:     5000,
					ReversedAmount:    0,
					ChargedBackAmount: 0,
					Currency:          "USD",
				},
				StateHash:    "state-hash-cleared-001",
				StateVersion: 2,
				Events: []JournalEvent{
					{
						EventID:     "evt-auth-001",
						LogicalSeq:  1,
						EventType:   "AUTHORIZATION",
						EventTimeMS: 1728560000000,
						PayloadHash: "hash-auth-001",
					},
					{
						EventID:     "evt-clear-001",
						LogicalSeq:  2,
						EventType:   "CLEAR",
						EventTimeMS: 1728560010000,
						PayloadHash: "hash-clear-001",
					},
				},
				Postings: []Posting{
					{
						PostingID:          "post-clear-001",
						EventID:            "evt-clear-001",
						PostingDate:        "2026-10-10",
						SignedPrincipal:    5000,
						SignedInterchange:  75,
						FeeScheduleVersion: "v1",
						BatchID:            "batch-001",
					},
				},
			})
		case "tx-reversed":
			adj := "reversal"
			_ = json.NewEncoder(w).Encode(Result{
				TransactionID: "tx-reversed",
				Status:        "REVERSED",
				Amounts: Amounts{
					AuthorizedAmount:  5000,
					ClearedAmount:     5000,
					ReversedAmount:    5000,
					ChargedBackAmount: 0,
					Currency:          "USD",
				},
				StateHash:    "state-hash-reversed-002",
				StateVersion: 3,
				Events: []JournalEvent{
					{
						EventID:     "evt-auth-002",
						LogicalSeq:  1,
						EventType:   "AUTHORIZATION",
						EventTimeMS: 1728560000000,
						PayloadHash: "hash-auth-002",
					},
					{
						EventID:     "evt-clear-002",
						LogicalSeq:  2,
						EventType:   "CLEAR",
						EventTimeMS: 1728560010000,
						PayloadHash: "hash-clear-002",
					},
					{
						EventID:     "evt-rev-002",
						LogicalSeq:  3,
						EventType:   "REVERSAL",
						EventTimeMS: 1728560020000,
						PayloadHash: "hash-rev-002",
					},
				},
				Postings: []Posting{
					{
						PostingID:          "post-clear-002",
						EventID:            "evt-clear-002",
						PostingDate:        "2026-10-10",
						SignedPrincipal:    5000,
						SignedInterchange:  75,
						FeeScheduleVersion: "v1",
						BatchID:            "batch-001",
					},
					{
						PostingID:          "post-rev-002",
						EventID:            "evt-rev-002",
						PostingDate:        "2026-10-10",
						SignedPrincipal:    -5000,
						SignedInterchange:  -75,
						FeeScheduleVersion: "v1",
						AdjustmentReason:   &adj,
						BatchID:            "batch-001",
					},
				},
			})
		default:
			http.Error(w, "transaction not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()

	// 1. Inspect cleared transaction
	clearedRes, err := Inspect(ctx, "tx-cleared", server.URL)
	if err != nil {
		t.Fatalf("unexpected error inspecting cleared tx: %v", err)
	}
	if clearedRes.Status != "CLEARED" {
		t.Errorf("expected status CLEARED, got %s", clearedRes.Status)
	}
	if clearedRes.Amounts.ClearedAmount != 5000 {
		t.Errorf("expected cleared amount 5000, got %d", clearedRes.Amounts.ClearedAmount)
	}
	if len(clearedRes.Events) != 2 {
		t.Fatalf("expected 2 journal events for cleared tx, got %d", len(clearedRes.Events))
	}
	if clearedRes.Events[0].EventType != "AUTHORIZATION" || clearedRes.Events[1].EventType != "CLEAR" {
		t.Errorf("unexpected events: %+v", clearedRes.Events)
	}
	if len(clearedRes.Postings) != 1 {
		t.Fatalf("expected 1 posting for cleared tx, got %d", len(clearedRes.Postings))
	}
	if clearedRes.Postings[0].SignedPrincipal != 5000 || clearedRes.Postings[0].SignedInterchange != 75 {
		t.Errorf("unexpected posting amounts: %+v", clearedRes.Postings[0])
	}

	// 2. Inspect reversed transaction
	reversedRes, err := Inspect(ctx, "tx-reversed", server.URL)
	if err != nil {
		t.Fatalf("unexpected error inspecting reversed tx: %v", err)
	}
	if reversedRes.Status != "REVERSED" {
		t.Errorf("expected status REVERSED, got %s", reversedRes.Status)
	}
	if reversedRes.Amounts.ReversedAmount != 5000 {
		t.Errorf("expected reversed amount 5000, got %d", reversedRes.Amounts.ReversedAmount)
	}
	if len(reversedRes.Events) != 3 {
		t.Fatalf("expected 3 journal events for reversed tx, got %d", len(reversedRes.Events))
	}
	if reversedRes.Events[2].EventType != "REVERSAL" {
		t.Errorf("expected 3rd event to be REVERSAL, got %s", reversedRes.Events[2].EventType)
	}
	if len(reversedRes.Postings) != 2 {
		t.Fatalf("expected 2 postings for reversed tx, got %d", len(reversedRes.Postings))
	}
	if reversedRes.Postings[1].SignedPrincipal != -5000 || reversedRes.Postings[1].SignedInterchange != -75 {
		t.Errorf("unexpected reversal posting amounts: %+v", reversedRes.Postings[1])
	}

	// 3. Error cases
	if _, err := Inspect(ctx, "", server.URL); err == nil {
		t.Error("expected error for empty transaction ID")
	}

	if _, err := Inspect(ctx, "tx-unknown", server.URL); err == nil {
		t.Error("expected error for unknown transaction ID")
	}
}
