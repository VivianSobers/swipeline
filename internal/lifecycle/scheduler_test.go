package lifecycle

import (
	"strconv"
	"testing"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
)

func mockFinal(decision string, ordinal int64) auth.Final {
	txID := contract.ID("mock-run", strconv.FormatInt(ordinal, 10))
	b := contract.Business{
		RunID:             "mock-run",
		TransactionID:     txID,
		SourceSeq:         ordinal,
		EventTimeMS:       1773300000000 + ordinal*1000,
		CardToken:         "card_" + contract.ID("card", strconv.FormatInt(ordinal%100, 10)),
		MerchantID:        "merchant-01",
		MCC:               "5411",
		IssuerBankID:      "bank-0",
		AcquirerBankID:    "bank-1",
		Currency:          "USD",
		AmountMinor:       5000,
		AuthorizationDate: "2026-10-11",
		RuleVersion:       "preview-rules-v1",
		ReferenceVersion:  "synthetic-reference-v1",
	}
	evt, _ := contract.New(b, 1773300000000+ordinal*1000)
	req := auth.Request{
		Event:    evt,
		Received: 1773300000000 + ordinal*1000,
		Scoring:  1773300000035 + ordinal*1000,
		Response: 1773300000050 + ordinal*1000,
		Mode:     "latency",
		Epoch:    1,
		CloseDay: "2026-10-11",
		CardSeq:  ordinal,
	}
	return auth.Final{
		Request:    req,
		DecisionID: contract.ID(txID, "decision"),
		Decision:   decision,
		Reason:     "MOCK_REASON",
	}
}

func TestDeclinesProduceZeroLifecycleEvents(t *testing.T) {
	s := &Scheduler{
		Seed:        42,
		Day:         "2026-10-11",
		RuleVersion: "preview-rules-v1",
		RefVersion:  "synthetic-reference-v1",
	}

	for i := int64(0); i < 50; i++ {
		decl := mockFinal("DECLINE", i)
		events := s.Schedule(decl, i)
		if len(events) != 0 {
			t.Fatalf("expected 0 events for DECLINE, got %d", len(events))
		}
	}
}

func TestApprovalsSequence2Distribution(t *testing.T) {
	s := &Scheduler{
		Seed:        42,
		Day:         "2026-10-11",
		RuleVersion: "preview-rules-v1",
		RefVersion:  "synthetic-reference-v1",
	}

	const n = 10000
	var clearCount, reverseCount int

	for i := int64(0); i < n; i++ {
		appr := mockFinal("APPROVE", i)
		events := s.Schedule(appr, i)
		if len(events) == 0 {
			t.Fatalf("expected at least 1 event for APPROVE, got 0")
		}
		seq2 := events[0]
		if seq2.LifecycleSeq != 2 {
			t.Fatalf("first event lifecycle_seq = %d, want 2", seq2.LifecycleSeq)
		}
		if seq2.ParentEventID != appr.EventID {
			t.Fatalf("seq 2 parent_event_id mismatch: got %s, want %s", seq2.ParentEventID, appr.EventID)
		}

		switch seq2.Kind {
		case string(KindClear):
			clearCount++
		case string(KindReverse):
			reverseCount++
			if len(events) != 1 {
				t.Fatalf("pre-clearing reversal must be terminal, got %d events", len(events))
			}
		default:
			t.Fatalf("unexpected seq 2 kind: %s", seq2.Kind)
		}
	}

	revPct := float64(reverseCount) / float64(n) * 100
	clearPct := float64(clearCount) / float64(n) * 100

	// 5% target for pre-clearing reversal (allow 4.0% - 6.0%)
	if revPct < 4.0 || revPct > 6.0 {
		t.Errorf("pre-clearing reverse pct = %.2f%%, want ~5%%", revPct)
	}
	// 95% target for clearing (allow 94.0% - 96.0%)
	if clearPct < 94.0 || clearPct > 96.0 {
		t.Errorf("clearing pct = %.2f%%, want ~95%%", clearPct)
	}
}

func TestClearedApprovalsSequence3(t *testing.T) {
	s := &Scheduler{
		Seed:        42,
		Day:         "2026-10-11",
		RuleVersion: "preview-rules-v1",
		RefVersion:  "synthetic-reference-v1",
	}

	const n = 10000
	var cbCount, postRevCount int

	for i := int64(0); i < n; i++ {
		appr := mockFinal("APPROVE", i)
		events := s.Schedule(appr, i)
		if len(events) >= 2 {
			seq2 := events[0]
			seq3 := events[1]
			if seq2.Kind != string(KindClear) {
				t.Fatalf("expected seq 2 to be CLEAR when seq 3 exists, got %s", seq2.Kind)
			}
			if seq3.LifecycleSeq != 3 {
				t.Fatalf("seq 3 lifecycle_seq = %d, want 3", seq3.LifecycleSeq)
			}
			if seq3.ParentEventID != seq2.EventID {
				t.Fatalf("seq 3 parent_event_id mismatch: got %s, want %s", seq3.ParentEventID, seq2.EventID)
			}
			switch seq3.Kind {
			case string(KindChargeback):
				cbCount++
			case string(KindReverse):
				postRevCount++
			default:
				t.Fatalf("unexpected seq 3 kind: %s", seq3.Kind)
			}
		}
	}

	cbPct := float64(cbCount) / float64(n) * 100
	// 1% target for chargeback (allow 0.6% - 1.5%)
	if cbPct < 0.6 || cbPct > 1.5 {
		t.Errorf("chargeback pct = %.2f%%, want ~1%%", cbPct)
	}
	if postRevCount == 0 {
		t.Error("expected at least one post-clearing reversal in sample")
	}
}

func TestParentEventIDChain(t *testing.T) {
	s := &Scheduler{
		Seed:        42,
		Day:         "2026-10-11",
		RuleVersion: "preview-rules-v1",
		RefVersion:  "synthetic-reference-v1",
	}

	foundChain := false
	for i := int64(0); i < 500; i++ {
		appr := mockFinal("APPROVE", i)
		events := s.Schedule(appr, i)
		if len(events) == 2 {
			foundChain = true
			if events[0].ParentEventID != appr.EventID {
				t.Errorf("event[0] parent %s != auth %s", events[0].ParentEventID, appr.EventID)
			}
			if events[1].ParentEventID != events[0].EventID {
				t.Errorf("event[1] parent %s != event[0] %s", events[1].ParentEventID, events[0].EventID)
			}
			break
		}
	}
	if !foundChain {
		t.Fatal("no 2-event lifecycle chain found in sample")
	}
}

func TestDeterministicPureFunction(t *testing.T) {
	s1 := &Scheduler{Seed: 42, Day: "2026-10-11", RuleVersion: "preview-rules-v1", RefVersion: "synthetic-reference-v1"}
	s2 := &Scheduler{Seed: 42, Day: "2026-10-11", RuleVersion: "preview-rules-v1", RefVersion: "synthetic-reference-v1"}
	sDiff := &Scheduler{Seed: 99, Day: "2026-10-11", RuleVersion: "preview-rules-v1", RefVersion: "synthetic-reference-v1"}

	appr := mockFinal("APPROVE", 123)

	run1 := s1.Schedule(appr, 123)
	run2 := s2.Schedule(appr, 123)

	if len(run1) != len(run2) {
		t.Fatalf("length mismatch: %d vs %d", len(run1), len(run2))
	}
	for i := range run1 {
		if run1[i].EventID != run2[i].EventID || run1[i].PayloadHash != run2[i].PayloadHash {
			t.Errorf("event %d differs between identical seeds", i)
		}
	}

	// Different seed produces different choice somewhere across sample
	differed := false
	for i := int64(0); i < 100; i++ {
		a := mockFinal("APPROVE", i)
		r1 := s1.Schedule(a, i)
		rDiff := sDiff.Schedule(a, i)
		if len(r1) != len(rDiff) || (len(r1) > 0 && len(rDiff) > 0 && r1[0].Kind != rDiff[0].Kind) {
			differed = true
			break
		}
	}
	if !differed {
		t.Error("different seeds produced identical choices across 100 trials")
	}
}
