package contract

import "testing"

// These literals pin version 1; changing serialization or ID encoding requires
// an explicit versioning decision, not regenerating expectations in the test.
func TestIDV1Golden(t *testing.T) {
	const want = "0473ef2dc0d324ab659d3580c1134e9d812035905c4781fdd6d529b0c6860e13"
	if got := ID("a", "b"); got != want {
		t.Errorf("ID mismatch:\n got %s\nwant %s", got, want)
	}
}

func TestCanonicalV1Golden(t *testing.T) {
	b := Business{
		RunID: "fixed", TransactionID: "transaction", SourceSeq: 7,
		EventTimeMS: 1791417600123, CardToken: "card_fixed",
		MerchantID: "merchant-003", MCC: "5732", IssuerBankID: "bank-1",
		AcquirerBankID: "bank-2", Currency: "USD", AmountMinor: 1234,
		AuthorizationDate: "2026-10-08", RuleVersion: "preview-rules-v1",
		ReferenceVersion: "synthetic-reference-v1",
	}
	const want = `{"run_id":"fixed","transaction_id":"transaction","source_seq":7,"event_time_ms":1791417600123,"card_token":"card_fixed","merchant_id":"merchant-003","mcc":"5732","issuer_bank_id":"bank-1","acquirer_bank_id":"bank-2","currency":"USD","amount_minor":1234,"authorization_date":"2026-10-08","rule_version":"preview-rules-v1","reference_version":"synthetic-reference-v1"}`
	if got := string(Canonical(b)); got != want {
		t.Errorf("canonical business JSON mismatch:\n got %s\nwant %s", got, want)
	}
}
