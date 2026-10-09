package generator

import (
	"testing"

	"github.com/VivianSobers/swipeline/internal/contract"
)

// These literals pin synthetic-v2 with envelope/canonical version 1, including
// the card-testing burst. They were computed by an independent implementation
// that also reproduces the synthetic-v1 values. Changes require an explicit
// versioning decision.
func TestIdentityV2Golden(t *testing.T) {
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
			canonical:     `{"run_id":"golden","transaction_id":"7080c5bb219ada3e457e47b1e8a12045871d868e844efe9f98fe51b691463c36","source_seq":0,"event_time_ms":1791417600000,"card_token":"card_f3dcbfdb8c96c52af0d8ebab07134d809f1a15b3dde65ca62b9aa1770649f7a5","merchant_id":"merchant-006","mcc":"5541","issuer_bank_id":"bank-3","acquirer_bank_id":"bank-2","currency":"USD","amount_minor":5891,"authorization_date":"2026-10-08","rule_version":"preview-rules-v1","reference_version":"synthetic-reference-v1"}`,
			transactionID: "7080c5bb219ada3e457e47b1e8a12045871d868e844efe9f98fe51b691463c36",
			eventID:       "bc4ac1b7f9216000f2060d4cb5e82c1d4fce4dcb7f5b2bd493d115702b3de726",
			payloadHash:   "66ca4e26d9585d4639e27695ed408eac33f9e68edb7d83c4551bdbab1e7e2bf7",
		},
		{
			name: "ordinal_950_card_testing", ordinal: 950,
			canonical:     `{"run_id":"golden","transaction_id":"1c2b749fd29599780c2e477f33b056a8905c2815f6b2fd353417f546726ef19e","source_seq":950,"event_time_ms":1791491054000,"card_token":"card_04d7c84b0bc684e75cce39346778c61c89a1478dffd9d0a9974f061e79f46c15","merchant_id":"merchant-000","mcc":"5411","issuer_bank_id":"bank-2","acquirer_bank_id":"bank-0","currency":"USD","amount_minor":100,"authorization_date":"2026-10-08","rule_version":"preview-rules-v1","reference_version":"synthetic-reference-v1"}`,
			transactionID: "1c2b749fd29599780c2e477f33b056a8905c2815f6b2fd353417f546726ef19e",
			eventID:       "960ed96905577e7847aeb550208ccd22ad5dee4a17bd5d0ba81f039ec6b183c8",
			payloadHash:   "de0ea762859501ca67180a108577ba6531f9706169b510fd87499cd17696e047",
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
