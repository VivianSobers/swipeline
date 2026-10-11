package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/VivianSobers/swipeline/internal/inspect"
)

// VerifyLifecycleClearing verifies that a transaction has reached CLEARED status,
// has recorded journal entries for authorization and clearing,
// and has a settlement posting with exact 150 bps interchange fee.
func VerifyLifecycleClearing(ctx context.Context, txID, sparkURL string) (*inspect.Result, error) {
	res, err := inspect.Inspect(ctx, txID, sparkURL)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect transaction %s: %w", txID, err)
	}

	if res.Status != "CLEARED" {
		return &res, fmt.Errorf("expected status CLEARED, got %s", res.Status)
	}

	var hasAuth, hasClear bool
	for _, evt := range res.Events {
		switch evt.EventType {
		case "AUTH_DECISION":
			hasAuth = true
		case "CLEAR":
			hasClear = true
		}
	}
	if !hasAuth {
		return &res, fmt.Errorf("journal missing AUTH_DECISION event")
	}
	if !hasClear {
		return &res, fmt.Errorf("journal missing CLEAR event")
	}

	var clearPosting *inspect.Posting
	for i := range res.Postings {
		p := &res.Postings[i]
		if p.SignedPrincipal > 0 && p.AdjustmentReason == nil {
			clearPosting = p
			break
		}
	}
	if clearPosting == nil {
		return &res, fmt.Errorf("settlement postings missing clearing posting")
	}

	expectedFee := (clearPosting.SignedPrincipal*150 + 5000) / 10000
	if clearPosting.SignedInterchange != expectedFee {
		return &res, fmt.Errorf("interchange fee mismatch: expected %d (150 bps of %d), got %d",
			expectedFee, clearPosting.SignedPrincipal, clearPosting.SignedInterchange)
	}

	return &res, nil
}

func TestVerifyLifecycleClearingUnit(t *testing.T) {
	// Mock inspect server returning wrong fee to demonstrate RED
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(inspect.Result{
			TransactionID: "tx-test-100",
			Status:        "CLEARED",
			Amounts: inspect.Amounts{
				AuthorizedAmount: 10000,
				ClearedAmount:    10000,
				Currency:         "USD",
			},
			Events: []inspect.JournalEvent{
				{EventID: "evt-auth-1", EventType: "AUTH_DECISION", LogicalSeq: 1},
				{EventID: "evt-clear-1", EventType: "CLEAR", LogicalSeq: 2},
			},
			Postings: []inspect.Posting{
				{
					PostingID:         "post-clear-1",
					EventID:           "evt-clear-1",
					SignedPrincipal:   10000,
					SignedInterchange: 150, // exact 150 bps fee
				},
			},
		})
	}))
	defer server.Close()

	res, err := VerifyLifecycleClearing(context.Background(), "tx-test-100", server.URL)
	if err != nil {
		t.Fatalf("VerifyLifecycleClearing failed: %v", err)
	}
	if res.Status != "CLEARED" {
		t.Errorf("expected CLEARED, got %s", res.Status)
	}
}

func TestInstalledLifecycleClearing(t *testing.T) {
	sparkURL := os.Getenv("SWIPELINE_SPARK")
	if sparkURL == "" {
		t.Skip("run make m4-demo on server")
	}

	txID := os.Getenv("SWIPELINE_TEST_TX_ID")
	if txID == "" {
		t.Fatal("missing SWIPELINE_TEST_TX_ID")
	}

	ctx := context.Background()
	res, err := VerifyLifecycleClearing(ctx, txID, sparkURL)
	if err != nil {
		t.Fatalf("VerifyLifecycleClearing failed for %s: %v", txID, err)
	}

	t.Logf("transaction %s verified CLEARED with hash %s and %d postings",
		res.TransactionID, res.StateHash, len(res.Postings))
}

// VerifyLifecycleOutOfOrder checks resolution of out-of-order events:
// Phase 1 (unapplied): chargeback is in journal but transaction status is not CHARGED_BACK and 0 postings.
// Phase 2 (resolved): clearing arrived, status is CHARGED_BACK with clearing and chargeback postings.
func VerifyLifecycleOutOfOrder(ctx context.Context, txID, sparkURL string, phase int) (*inspect.Result, error) {
	res, err := inspect.Inspect(ctx, txID, sparkURL)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect transaction %s: %w", txID, err)
	}

	var hasChargeback, hasClear bool
	for _, evt := range res.Events {
		switch evt.EventType {
		case "CHARGEBACK":
			hasChargeback = true
		case "CLEAR":
			hasClear = true
		}
	}

	switch phase {
	case 1:
		if !hasChargeback {
			return &res, fmt.Errorf("out-of-order chargeback event dropped from journal")
		}
		if res.Status == "CHARGED_BACK" {
			return &res, fmt.Errorf("chargeback applied prematurely before clearing")
		}
		if len(res.Postings) != 0 {
			return &res, fmt.Errorf("expected 0 postings during phase 1, got %d", len(res.Postings))
		}
	case 2:
		if !hasChargeback || !hasClear {
			return &res, fmt.Errorf("phase 2 requires both CLEAR and CHARGEBACK in journal")
		}
		if res.Status != "CHARGED_BACK" {
			return &res, fmt.Errorf("expected status CHARGED_BACK in phase 2, got %s", res.Status)
		}
		if len(res.Postings) != 2 {
			return &res, fmt.Errorf("expected exactly 2 postings (clearing and chargeback) in phase 2, got %d", len(res.Postings))
		}
	default:
		return &res, fmt.Errorf("unknown phase %d", phase)
	}

	return &res, nil
}

func TestVerifyLifecycleOutOfOrderUnit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txID := r.URL.Query().Get("transaction_id")
		w.Header().Set("Content-Type", "application/json")
		if txID == "tx-ooo-phase1" {
			_ = json.NewEncoder(w).Encode(inspect.Result{
				TransactionID: "tx-ooo-phase1",
				Status:        "AUTHORIZED",
				Events: []inspect.JournalEvent{
					{EventID: "evt-auth-1", EventType: "AUTH_DECISION", LogicalSeq: 1},
					{EventID: "evt-cb-1", EventType: "CHARGEBACK", LogicalSeq: 2},
				},
				Postings: []inspect.Posting{},
			})
			return
		}
		adj := "chargeback"
		_ = json.NewEncoder(w).Encode(inspect.Result{
			TransactionID: "tx-ooo-phase2",
			Status:        "CHARGED_BACK",
			Events: []inspect.JournalEvent{
				{EventID: "evt-auth-1", EventType: "AUTH_DECISION", LogicalSeq: 1},
				{EventID: "evt-cb-1", EventType: "CHARGEBACK", LogicalSeq: 2},
				{EventID: "evt-clear-1", EventType: "CLEAR", LogicalSeq: 3},
			},
			Postings: []inspect.Posting{
				{PostingID: "p-clear-1", EventID: "evt-clear-1", SignedPrincipal: 5000, SignedInterchange: 75},
				{PostingID: "p-cb-1", EventID: "evt-cb-1", SignedPrincipal: -5000, SignedInterchange: -75, AdjustmentReason: &adj},
			},
		})
	}))
	defer server.Close()

	// Test Phase 1
	res1, err := VerifyLifecycleOutOfOrder(context.Background(), "tx-ooo-phase1", server.URL, 1)
	if err != nil {
		t.Fatalf("Phase 1 failed: %v", err)
	}
	if res1.Status != "AUTHORIZED" {
		t.Errorf("Phase 1 expected status AUTHORIZED, got %s", res1.Status)
	}

	// Test Phase 2
	res2, err := VerifyLifecycleOutOfOrder(context.Background(), "tx-ooo-phase2", server.URL, 2)
	if err != nil {
		t.Fatalf("Phase 2 failed: %v", err)
	}
	if res2.Status != "CHARGED_BACK" {
		t.Errorf("Phase 2 expected status CHARGED_BACK, got %s", res2.Status)
	}
	if len(res2.Postings) != 2 {
		t.Errorf("Phase 2 expected 2 postings, got %d", len(res2.Postings))
	}
}

func TestInstalledLifecycleOutOfOrder(t *testing.T) {
	sparkURL := os.Getenv("SWIPELINE_SPARK")
	if sparkURL == "" {
		t.Skip("run make m4-demo on server")
	}

	txID := os.Getenv("SWIPELINE_TEST_TX_ID_OOO")
	if txID == "" {
		t.Fatal("missing SWIPELINE_TEST_TX_ID_OOO")
	}

	ctx := context.Background()
	res, err := VerifyLifecycleOutOfOrder(ctx, txID, sparkURL, 2)
	if err != nil {
		t.Fatalf("VerifyLifecycleOutOfOrder failed for %s: %v", txID, err)
	}

	t.Logf("transaction %s successfully resolved out of order with status %s",
		res.TransactionID, res.Status)
}

// VerifyLifecycleReversalsAndChargebacks verifies pre-clearing reversals (0 postings),
// post-clearing reversals (cancelling postings), and quarantined invalid events.
func VerifyLifecycleReversalsAndChargebacks(ctx context.Context, sparkURL, preClearingTxID, postClearingTxID, quarantinedTxID string) error {
	// 1. Pre-clearing reversal
	resPre, err := inspect.Inspect(ctx, preClearingTxID, sparkURL)
	if err != nil {
		return fmt.Errorf("failed to inspect pre-clearing tx %s: %w", preClearingTxID, err)
	}
	if resPre.Status != "REVERSED" {
		return fmt.Errorf("pre-clearing tx %s expected status REVERSED, got %s", preClearingTxID, resPre.Status)
	}
	if len(resPre.Postings) != 0 {
		return fmt.Errorf("pre-clearing reversal %s expected 0 postings, got %d", preClearingTxID, len(resPre.Postings))
	}

	// 2. Post-clearing reversal
	resPost, err := inspect.Inspect(ctx, postClearingTxID, sparkURL)
	if err != nil {
		return fmt.Errorf("failed to inspect post-clearing tx %s: %w", postClearingTxID, err)
	}
	if resPost.Status != "REVERSED" {
		return fmt.Errorf("post-clearing tx %s expected status REVERSED, got %s", postClearingTxID, resPost.Status)
	}
	if len(resPost.Postings) != 2 {
		return fmt.Errorf("post-clearing reversal %s expected 2 postings, got %d", postClearingTxID, len(resPost.Postings))
	}
	p1 := resPost.Postings[0]
	p2 := resPost.Postings[1]
	if p1.SignedPrincipal+p2.SignedPrincipal != 0 {
		return fmt.Errorf("post-clearing postings do not cancel: p1=%d, p2=%d", p1.SignedPrincipal, p2.SignedPrincipal)
	}
	if p1.SignedInterchange+p2.SignedInterchange != 0 {
		return fmt.Errorf("post-clearing fees do not cancel: f1=%d, f2=%d", p1.SignedInterchange, p2.SignedInterchange)
	}

	// 3. Quarantined attempts to clear an already reversed transaction
	if quarantinedTxID != "" {
		resQuar, err := inspect.Inspect(ctx, quarantinedTxID, sparkURL)
		if err != nil {
			return fmt.Errorf("failed to inspect quarantined tx %s: %w", quarantinedTxID, err)
		}
		if len(resQuar.Postings) != 0 {
			return fmt.Errorf("rejected transition created postings")
		}
		if resQuar.Status != "REVERSED" {
			return fmt.Errorf("quarantined tx %s expected status REVERSED, got %s", quarantinedTxID, resQuar.Status)
		}
	}

	return nil
}

func TestVerifyLifecycleReversalsAndChargebacksUnit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txID := r.URL.Query().Get("transaction_id")
		w.Header().Set("Content-Type", "application/json")
		switch txID {
		case "tx-pre-rev":
			_ = json.NewEncoder(w).Encode(inspect.Result{
				TransactionID: "tx-pre-rev",
				Status:        "REVERSED",
				Amounts:       inspect.Amounts{AuthorizedAmount: 5000, ReversedAmount: 5000, Currency: "USD"},
				Postings:      []inspect.Posting{}, // 0 postings for pre-clearing reversal
			})
		case "tx-post-rev":
			adj := "reversal"
			_ = json.NewEncoder(w).Encode(inspect.Result{
				TransactionID: "tx-post-rev",
				Status:        "REVERSED",
				Amounts:       inspect.Amounts{AuthorizedAmount: 5000, ClearedAmount: 5000, ReversedAmount: 5000, Currency: "USD"},
				Postings: []inspect.Posting{
					{PostingID: "p-clear", EventID: "evt-clear", SignedPrincipal: 5000, SignedInterchange: 75},
					{PostingID: "p-rev", EventID: "evt-rev", SignedPrincipal: -5000, SignedInterchange: -75, AdjustmentReason: &adj},
				},
			})
		case "tx-quar":
			_ = json.NewEncoder(w).Encode(inspect.Result{
				TransactionID: "tx-quar",
				Status:        "REVERSED",
				Amounts:       inspect.Amounts{AuthorizedAmount: 5000, ReversedAmount: 5000, Currency: "USD"},
				Postings:      []inspect.Posting{},
			})
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	err := VerifyLifecycleReversalsAndChargebacks(context.Background(), server.URL, "tx-pre-rev", "tx-post-rev", "tx-quar")
	if err != nil {
		t.Fatalf("VerifyLifecycleReversalsAndChargebacks failed: %v", err)
	}
}

func TestInstalledLifecycleReversalsAndChargebacks(t *testing.T) {
	sparkURL := os.Getenv("SWIPELINE_SPARK")
	if sparkURL == "" {
		t.Skip("run make m4-demo on server")
	}

	preTx := os.Getenv("SWIPELINE_TEST_TX_PRE_REV")
	postTx := os.Getenv("SWIPELINE_TEST_TX_POST_REV")
	quarTx := os.Getenv("SWIPELINE_TEST_TX_QUAR")
	if preTx == "" || postTx == "" || quarTx == "" {
		t.Fatal("missing SWIPELINE_TEST_TX_PRE_REV or SWIPELINE_TEST_TX_POST_REV")
	}

	ctx := context.Background()
	if err := VerifyLifecycleReversalsAndChargebacks(ctx, sparkURL, preTx, postTx, quarTx); err != nil {
		t.Fatalf("VerifyLifecycleReversalsAndChargebacks failed: %v", err)
	}
}

func TestLifecycleFeeRoundsHalfUp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(inspect.Result{TransactionID: "round", Status: "CLEARED", Events: []inspect.JournalEvent{{EventType: "AUTH_DECISION"}, {EventType: "CLEAR"}}, Postings: []inspect.Posting{{SignedPrincipal: 100, SignedInterchange: 2}}})
	}))
	defer server.Close()
	if _, err := VerifyLifecycleClearing(context.Background(), "round", server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestInstalledLifecycleMissingFixturesFail(t *testing.T) {
	for _, name := range []string{"Clearing", "OutOfOrder", "ReversalsAndChargebacks", "Stability"} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestInstalledLifecycle"+name+"$")
			for _, env := range os.Environ() {
				if !strings.HasPrefix(env, "SWIPELINE_") {
					command.Env = append(command.Env, env)
				}
			}
			command.Env = append(command.Env, "SWIPELINE_SPARK=http://127.0.0.1:1")
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "missing SWIPELINE_") {
				t.Fatalf("installed mode skipped missing inputs: %v %s", err, output)
			}
		})
	}
}
