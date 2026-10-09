package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestReplyClientOptions(t *testing.T) {
	p, err := Producer("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.OptValue("ProducerLinger") != time.Duration(0) {
		t.Fatal("linger")
	}
	c, err := Consumer("127.0.0.1:1", "swipeline-gateway-v1", func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.OptValue("FetchMinBytes") != int32(1) || c.OptValue("FetchMaxWait") != 10*time.Millisecond {
		t.Fatal("fetch")
	}
	if c.OptValue("DisableAutoCommit") != true {
		t.Fatal("offsets must be committed only after durable arbitration")
	}
}

func validProposal(t *testing.T) map[string]any {
	t.Helper()
	g, err := generator.New(generator.Config{RunID: "proposal", Seed: 42, Day: "2026-10-08", Events: 10})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	p := auth.Proposal{Request: auth.Request{Event: e, Received: 1000, Scoring: 1035, Response: 1050, Mode: "latency", CloseDay: e.AuthorizationDate, CardSeq: 1},
		ProposalID: contract.ID(e.TransactionID, "proposal"), Decision: "DECLINE", Reason: "CARD_TESTING", Flags: []string{"CARD_TESTING"},
		Score: 100, Velocity: 1, Usual: 100, Ratio: 1, Distinct: 61.2, Scored: 1010}
	data, _ := json.Marshal(p)
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	return m
}

func TestProposalValidation(t *testing.T) {
	data, _ := json.Marshal(validProposal(t))
	if _, err := Proposal(data); err != nil {
		t.Fatal("valid proposal rejected:", err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing decision":  func(m map[string]any) { delete(m, "decision") },
		"null score":        func(m map[string]any) { m["score"] = nil },
		"missing flags":     func(m map[string]any) { delete(m, "fraud_flags") },
		"missing envelope":  func(m map[string]any) { delete(m, "payload_hash") },
		"schema version":    func(m map[string]any) { m["schema_version"] = 2 },
		"foreign id":        func(m map[string]any) { m["proposal_id"] = "p1" },
		"unknown decision":  func(m map[string]any) { m["decision"] = "REVIEW" },
		"score over 180":    func(m map[string]any) { m["score"] = 181 },
		"negative velocity": func(m map[string]any) { m["velocity_60s"] = -1 },
		"usual under floor": func(m map[string]any) { m["usual_minor"] = 99 },
		"negative distinct": func(m map[string]any) { m["merchant_distinct_estimate"] = -1 },
		"unknown flag":      func(m map[string]any) { m["fraud_flags"] = []any{"GEO"} },
	} {
		m := validProposal(t)
		mutate(m)
		data, _ := json.Marshal(m)
		if _, err := Proposal(data); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := Proposal([]byte("not json")); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestSanitizedDeadLetter(t *testing.T) {
	secret := []byte(`{"card_token":"card_secret","amount_minor":1}`)
	b := DeadLetter("swipeline.auth.proposals.v1", 2, 9, "INVALID_PROPOSAL", secret)
	if bytes.Contains(b, []byte("secret")) {
		t.Fatal("payload leaked into the dead letter")
	}
	c := jsonschema.NewCompiler()
	data, err := os.ReadFile("../../schemas/dlq.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc, value any
	if err = json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if err = c.AddResource("urn:swipeline:dlq:v1", doc); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile("urn:swipeline:dlq:v1")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &value); err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(value); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(secret)
	if m := value.(map[string]any); m["payload_hash"] != hex.EncodeToString(sum[:]) || m["error_code"] != "INVALID_PROPOSAL" || m["offset"] != 9.0 {
		t.Fatal(m)
	}
}
