package lifecycle

import (
	"encoding/json"
	"os"
	"testing"
)

func validClearJSON(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../schemas/fixtures/lifecycle_clear.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func validReverseJSON(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../schemas/fixtures/lifecycle_reverse.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func validChargebackJSON(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../schemas/fixtures/lifecycle_chargeback.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestValidDecodingEvents(t *testing.T) {
	clearEvt, err := Decode(validClearJSON(t))
	if err != nil {
		t.Fatalf("decode CLEAR: %v", err)
	}
	if clearEvt.Kind != string(KindClear) || clearEvt.LifecycleSeq != 2 {
		t.Errorf("unexpected CLEAR: kind=%s, seq=%d", clearEvt.Kind, clearEvt.LifecycleSeq)
	}
	if err := clearEvt.Validate(); err != nil {
		t.Errorf("validate CLEAR: %v", err)
	}

	revEvt, err := Decode(validReverseJSON(t))
	if err != nil {
		t.Fatalf("decode REVERSE: %v", err)
	}
	if revEvt.Kind != string(KindReverse) || revEvt.LifecycleSeq != 2 {
		t.Errorf("unexpected REVERSE: kind=%s, seq=%d", revEvt.Kind, revEvt.LifecycleSeq)
	}
	if err := revEvt.Validate(); err != nil {
		t.Errorf("validate REVERSE: %v", err)
	}

	cbEvt, err := Decode(validChargebackJSON(t))
	if err != nil {
		t.Fatalf("decode CHARGEBACK: %v", err)
	}
	if cbEvt.Kind != string(KindChargeback) || cbEvt.LifecycleSeq != 3 {
		t.Errorf("unexpected CHARGEBACK: kind=%s, seq=%d", cbEvt.Kind, cbEvt.LifecycleSeq)
	}
	if err := cbEvt.Validate(); err != nil {
		t.Errorf("validate CHARGEBACK: %v", err)
	}
}

func TestInvalidDecodingAndValidation(t *testing.T) {
	base := validClearJSON(t)

	tests := []struct {
		name    string
		mutate  func(m map[string]any)
		rawJSON string
	}{
		{
			name:    "unparseable JSON",
			rawJSON: `{"schema_version": 1, invalid}`,
		},
		{
			name: "invalid schema version",
			mutate: func(m map[string]any) {
				m["schema_version"] = 2
			},
		},
		{
			name: "empty parent event id",
			mutate: func(m map[string]any) {
				m["parent_event_id"] = ""
			},
		},
		{
			name: "invalid parent event id hex pattern",
			mutate: func(m map[string]any) {
				m["parent_event_id"] = "not-a-64-char-hex-hash"
			},
		},
		{
			name: "non-USD currency",
			mutate: func(m map[string]any) {
				m["currency"] = "EUR"
			},
		},
		{
			name: "zero amount minor",
			mutate: func(m map[string]any) {
				m["amount_minor"] = 0
			},
		},
		{
			name: "negative amount minor",
			mutate: func(m map[string]any) {
				m["amount_minor"] = -100
			},
		},
		{
			name: "lifecycle seq 1",
			mutate: func(m map[string]any) {
				m["lifecycle_seq"] = 1
			},
		},
		{
			name: "lifecycle seq 4",
			mutate: func(m map[string]any) {
				m["lifecycle_seq"] = 4
			},
		},
		{
			name: "invalid authorization date",
			mutate: func(m map[string]any) {
				m["authorization_date"] = "2026/10/11"
			},
		},
		{
			name: "invalid close day",
			mutate: func(m map[string]any) {
				m["close_day"] = "bad-date"
			},
		},
		{
			name: "missing required field",
			mutate: func(m map[string]any) {
				delete(m, "transaction_id")
			},
		},
		{
			name: "null required field",
			mutate: func(m map[string]any) {
				m["merchant_id"] = nil
			},
		},
		{
			name: "invalid kind",
			mutate: func(m map[string]any) {
				m["kind"] = "UNKNOWN"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var data []byte
			if tc.rawJSON != "" {
				data = []byte(tc.rawJSON)
			} else {
				var m map[string]any
				if err := json.Unmarshal(base, &m); err != nil {
					t.Fatal(err)
				}
				tc.mutate(m)
				var err error
				data, err = json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
			}

			if _, err := Decode(data); err == nil {
				t.Fatalf("expected Decode to reject %s, got nil error", tc.name)
			}
		})
	}
}

func TestNewLifecycleEvent(t *testing.T) {
	evt, err := New(
		"run-01",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		10,
		2,
		string(KindClear),
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		1773300010000,
		1773300010050,
		"2026-10-11",
		"USD",
		5000,
		"merchant-01",
		"bank-issuer",
		"bank-acquirer",
		"v1",
		1,
		"2026-10-11",
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if evt.SchemaVersion != 1 {
		t.Errorf("schema version = %d, want 1", evt.SchemaVersion)
	}
	if evt.EventID == "" {
		t.Error("event ID is empty")
	}
	if evt.PayloadHash == "" {
		t.Error("payload hash is empty")
	}
	if err := evt.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

	// Negative amount should fail
	_, err = New(
		"run-01",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		10,
		2,
		string(KindClear),
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		1773300010000,
		1773300010050,
		"2026-10-11",
		"USD",
		-50,
		"merchant-01",
		"bank-issuer",
		"bank-acquirer",
		"v1",
		1,
		"2026-10-11",
	)
	if err == nil {
		t.Error("expected error for negative amount")
	}
}
