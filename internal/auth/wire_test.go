package auth

import (
	"encoding/json"
	"testing"

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
