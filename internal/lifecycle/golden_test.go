package lifecycle

import (
	"bytes"
	"testing"
)

func goldenFixture() Event {
	return Event{
		SchemaVersion:     1,
		EventID:           "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RunID:             "golden-run",
		TransactionID:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SourceSeq:         100,
		LifecycleSeq:      2,
		Kind:              "CLEAR",
		ParentEventID:     "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		EventTimeMS:       1773300000000,
		ProducedAtMS:      1773300000123,
		AuthorizationDate: "2026-10-11",
		Currency:          "USD",
		AmountMinor:       5000,
		MerchantID:        "merchant-golden",
		IssuerBankID:      "bank-golden-issuer",
		AcquirerBankID:    "bank-golden-acquirer",
		ReferenceVersion:  "synthetic-reference-v1",
		SealEpoch:         1,
		CloseDay:          "2026-10-11",
	}
}

func TestCanonicalV1Golden(t *testing.T) {
	e := goldenFixture()
	const wantCanonical = `{"acquirer_bank_id":"bank-golden-acquirer","amount_minor":5000,"authorization_date":"2026-10-11","close_day":"2026-10-11","currency":"USD","event_time_ms":1773300000000,"issuer_bank_id":"bank-golden-issuer","kind":"CLEAR","lifecycle_seq":2,"merchant_id":"merchant-golden","parent_event_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","reference_version":"synthetic-reference-v1","run_id":"golden-run","seal_epoch":1,"source_seq":100,"transaction_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	const wantHash = "bc7caa2fcc9a66940ad3e5f5d9d4c11388a8413c76048008857a215eacccf0fe"

	gotCanonical := string(Canonical(e))
	if gotCanonical != wantCanonical {
		t.Fatalf("canonical JSON mismatch:\n got:  %s\n want: %s", gotCanonical, wantCanonical)
	}

	gotHash := digest(Canonical(e))
	if gotHash != wantHash {
		t.Fatalf("canonical hash mismatch:\n got:  %s\n want: %s", gotHash, wantHash)
	}
}

func TestKeyReorderStability(t *testing.T) {
	// JSON with keys reversed/scrambled
	scrambledJSON := []byte(`{
		"payload_hash": "bc7caa2fcc9a66940ad3e5f5d9d4c11388a8413c76048008857a215eacccf0fe",
		"event_id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"close_day": "2026-10-11",
		"seal_epoch": 1,
		"reference_version": "synthetic-reference-v1",
		"acquirer_bank_id": "bank-golden-acquirer",
		"issuer_bank_id": "bank-golden-issuer",
		"merchant_id": "merchant-golden",
		"amount_minor": 5000,
		"currency": "USD",
		"authorization_date": "2026-10-11",
		"produced_at_ms": 1773300000999,
		"event_time_ms": 1773300000000,
		"parent_event_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"kind": "CLEAR",
		"lifecycle_seq": 2,
		"source_seq": 100,
		"transaction_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"run_id": "golden-run",
		"schema_version": 1
	}`)

	e, err := Decode(scrambledJSON)
	if err != nil {
		t.Fatalf("decode scrambled JSON: %v", err)
	}

	base := goldenFixture()
	if !bytes.Equal(Canonical(e), Canonical(base)) {
		t.Errorf("Canonical mismatch after key reordering")
	}
	if digest(Canonical(e)) != digest(Canonical(base)) {
		t.Errorf("Hash mismatch after key reordering")
	}
}

func TestSensitivityToFieldAlterations(t *testing.T) {
	base := goldenFixture()
	baseHash := digest(Canonical(base))

	mutations := []struct {
		name   string
		mutate func(*Event)
	}{
		{"amount_minor", func(e *Event) { e.AmountMinor++ }},
		{"acquirer_bank_id", func(e *Event) { e.AcquirerBankID = "bank-other-acquirer" }},
		{"authorization_date", func(e *Event) { e.AuthorizationDate = "2026-10-12" }},
		{"close_day", func(e *Event) { e.CloseDay = "2026-10-12" }},
		{"currency", func(e *Event) { e.Currency = "EUR" }},
		{"event_time_ms", func(e *Event) { e.EventTimeMS += 1000 }},
		{"issuer_bank_id", func(e *Event) { e.IssuerBankID = "bank-other-issuer" }},
		{"kind", func(e *Event) { e.Kind = "REVERSE" }},
		{"lifecycle_seq", func(e *Event) { e.LifecycleSeq = 3 }},
		{"merchant_id", func(e *Event) { e.MerchantID = "merchant-other" }},
		{"parent_event_id", func(e *Event) { e.ParentEventID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }},
		{"reference_version", func(e *Event) { e.ReferenceVersion = "synthetic-reference-v2" }},
		{"run_id", func(e *Event) { e.RunID = "other-run" }},
		{"seal_epoch", func(e *Event) { e.SealEpoch = 2 }},
		{"source_seq", func(e *Event) { e.SourceSeq = 101 }},
		{"transaction_id", func(e *Event) { e.TransactionID = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" }},
	}

	for _, tc := range mutations {
		t.Run("alter_"+tc.name, func(t *testing.T) {
			mutated := base
			tc.mutate(&mutated)
			mutHash := digest(Canonical(mutated))
			if mutHash == baseHash {
				t.Fatalf("altering %s did not change payload hash", tc.name)
			}
		})
	}

	// ProducedAtMS altering MUST NOT change hash or canonical JSON
	alteredProduced := base
	alteredProduced.ProducedAtMS = 9999999999999
	if !bytes.Equal(Canonical(alteredProduced), Canonical(base)) {
		t.Error("altering ProducedAtMS changed Canonical payload")
	}
	if digest(Canonical(alteredProduced)) != baseHash {
		t.Error("altering ProducedAtMS changed payload hash")
	}
}
