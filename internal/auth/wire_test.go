package auth

import (
	"bytes"
	"encoding/json"
	"math"
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

func TestRunRangeAndVersions(t *testing.T) {
	e, err := contract.Decode(sample(t))
	if err != nil {
		t.Fatal(err)
	}
	good := Run{ID: e.RunID, Identity: "identity", Day: e.AuthorizationDate, Rule: e.RuleVersion, Reference: e.ReferenceVersion,
		Mode: "latency", TPS: 50, Compression: 720, Concurrency: 128, Partitioner: "sha256-json-v1", Partitions: 4}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := good.Check(e); err != nil {
		t.Fatal(err)
	}
	throughput := good
	throughput.Mode = "throughput"
	if err := throughput.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Run){
		"adaptive mode":    func(r *Run) { r.Mode = "adaptive" },
		"bad day":          func(r *Run) { r.Day = "2026-13-01" },
		"bad run id":       func(r *Run) { r.ID = "../x" },
		"empty identity":   func(r *Run) { r.Identity = "" },
		"partitioner":      func(r *Run) { r.Partitioner = "murmur2" },
		"partitions":       func(r *Run) { r.Partitions = 6 },
		"no concurrency":   func(r *Run) { r.Concurrency = 0 },
		"concurrency 129":  func(r *Run) { r.Concurrency = 129 },
		"zero tps":         func(r *Run) { r.TPS = 0 },
		"nan tps":          func(r *Run) { r.TPS = math.NaN() },
		"inf compression":  func(r *Run) { r.Compression = math.Inf(1) },
		"zero compression": func(r *Run) { r.Compression = 0 },
	} {
		r := good
		mutate(&r)
		if r.Validate() == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	for name, mutate := range map[string]func(*Run){
		"run":       func(r *Run) { r.ID = "other" },
		"day":       func(r *Run) { r.Day = "2026-10-09" },
		"rule":      func(r *Run) { r.Rule = "rules-v9" },
		"reference": func(r *Run) { r.Reference = "ref-v9" },
	} {
		r := good
		mutate(&r)
		if r.Check(e) == nil {
			t.Fatalf("%s conflict accepted", name)
		}
	}
	late := e
	late.EventTimeMS += 86400000
	if good.Check(late) == nil {
		t.Fatal("event outside business day accepted")
	}
}

func TestCanonicalKey(t *testing.T) {
	if string(Key("ab", "c")) != `["ab","c"]` {
		t.Fatalf("key encoding %s", Key("ab", "c"))
	}
	if string(Key("ab", "c")) == string(Key("a", "bc")) {
		t.Fatal("ambiguous key")
	}
	// Pinned from an independent SHA-256 implementation; every producer and
	// the streaming job must agree on these.
	for _, tc := range []struct {
		run, card string
		want      int32
	}{{"run", "card", 0}, {"a", "b", 1}, {"a", "e", 2}, {"a", "c", 3}} {
		if got := Partition(Key(tc.run, tc.card)); got != tc.want {
			t.Fatalf("%s/%s: partition %d, want %d", tc.run, tc.card, got, tc.want)
		}
	}
}
