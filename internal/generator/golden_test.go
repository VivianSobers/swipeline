package generator

import (
	"testing"

	"github.com/VivianSobers/swipeline/internal/contract"
)

// These literals pin synthetic-v1 with envelope/canonical version 1, including
// the card-testing burst. Changes require an explicit versioning decision.
func TestIdentityV1Golden(t *testing.T) {
	g, err := New(Config{RunID: "golden", Seed: 42, Day: "2026-10-08", Events: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name          string
		ordinal       int64
		canonical     string
		transactionID string
		eventID       string
		payloadHash   string
	}{
		{
			name: "ordinal_0", ordinal: 0,
			canonical:     `{"run_id":"golden","transaction_id":"e1a7fb16937d2c9b1e6b784c733b2320fd9b977ea14fda1415ab6b38d0327faa","source_seq":0,"event_time_ms":1791417600000,"card_token":"card_c80818b949b00cb87851bac4fad487a124d544f4605e14fc8764d93876117d32","merchant_id":"merchant-001","mcc":"5812","issuer_bank_id":"bank-1","acquirer_bank_id":"bank-1","currency":"USD","amount_minor":1955,"authorization_date":"2026-10-08","rule_version":"preview-rules-v1","reference_version":"synthetic-reference-v1"}`,
			transactionID: "e1a7fb16937d2c9b1e6b784c733b2320fd9b977ea14fda1415ab6b38d0327faa",
			eventID:       "2072919ae0a0ce778802b4aeb7b98c4d544cb31688c614cd84c753abfed72ff7",
			payloadHash:   "77104cd923cc64d1876295832372a4c38c960a8a4f15442a3dc8f8efeec3f5f1",
		},
		{
			name: "ordinal_950_card_testing", ordinal: 950,
			canonical:     `{"run_id":"golden","transaction_id":"4c1b148d48d6e749bd79df7366ff010a619f4faf75b4ef4d4f723bbf96c88eb6","source_seq":950,"event_time_ms":1791417644000,"card_token":"card_fa425f5b2b609b3920c16d22dcb1494155305a57102afa29981d208b02c55c85","merchant_id":"merchant-000","mcc":"5411","issuer_bank_id":"bank-2","acquirer_bank_id":"bank-0","currency":"USD","amount_minor":100,"authorization_date":"2026-10-08","rule_version":"preview-rules-v1","reference_version":"synthetic-reference-v1"}`,
			transactionID: "4c1b148d48d6e749bd79df7366ff010a619f4faf75b4ef4d4f723bbf96c88eb6",
			eventID:       "ecb3b22e9a971dbbb5f1676182c4ca3634e52c56f6697c6a9622dc99a47ef1fa",
			payloadHash:   "3ecdb376bb6b51a24f223d25be06934d35265b619abf44578df06e7c7b80845b",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e, err := g.At(tt.ordinal, 1)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(contract.Canonical(e.Business)); got != tt.canonical {
				t.Errorf("canonical business JSON mismatch:\n got %s\nwant %s", got, tt.canonical)
			}
			if e.TransactionID != tt.transactionID {
				t.Errorf("transaction_id mismatch:\n got %s\nwant %s", e.TransactionID, tt.transactionID)
			}
			if e.EventID != tt.eventID {
				t.Errorf("event_id mismatch:\n got %s\nwant %s", e.EventID, tt.eventID)
			}
			if e.PayloadHash != tt.payloadHash {
				t.Errorf("payload_hash mismatch:\n got %s\nwant %s", e.PayloadHash, tt.payloadHash)
			}
		})
	}
}
