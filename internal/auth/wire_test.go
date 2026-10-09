package auth

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
)

func TestStandInNulls(t *testing.T) {
	f := Final{Decision: "DECLINE", Source: "STAND_IN", Flags: []string{}}
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	_ = json.Unmarshal(b, &v)
	for _, k := range []string{"score", "proposal_id", "velocity_60s", "usual_minor", "spend_ratio", "merchant_distinct_estimate", "features_late"} {
		if x, ok := v[k]; !ok || x != nil {
			t.Fatalf("%s: %v", k, x)
		}
	}
}

func sample(t *testing.T) []byte {
	t.Helper()
	g, err := generator.New(generator.Config{RunID: "auth", Seed: 42, Day: "2026-10-08", Events: 1000})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestIdempotencyHeader(t *testing.T) {
	b := sample(t)
	e, err := contract.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(b, e.TransactionID); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", contract.ID("other"), e.EventID} {
		if _, err := Decode(b, key); err == nil {
			t.Fatalf("accepted key %q", key)
		}
	}
}

func TestTrustedMetadata(t *testing.T) {
	b := sample(t)
	e, err := contract.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"received_at_ms", "scoring_deadline_at_ms", "response_deadline_at_ms", "deadline_mode", "seal_epoch", "close_day", "accepted_card_seq"} {
		for _, value := range []string{"0", "null", `"x"`} {
			bad := bytes.Replace(b, []byte("{"), []byte(`{"`+field+`":`+value+`,`), 1)
			if _, err := Decode(bad, e.TransactionID); err == nil {
				t.Fatalf("accepted client %s=%s", field, value)
			}
		}
	}
}
