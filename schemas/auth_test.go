package schemas

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// compile registers every schema file under its URN and compiles id with
// format assertions on.
func compile(t *testing.T, id string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	files, err := filepath.Glob("*.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if err := c.AddResource(doc["$id"].(string), doc); err != nil {
			t.Fatal(err)
		}
	}
	s, err := c.Compile(id)
	if err != nil {
		t.Fatalf("compile %s: %v", id, err)
	}
	return s
}

func request(t *testing.T) auth.Request {
	t.Helper()
	g, err := generator.New(generator.Config{RunID: "schema-auth", Seed: 42, Day: "2026-10-08", Events: 10})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	return auth.Request{Event: e, Received: 1000, Scoring: 1035, Response: 1050, Mode: "latency", Epoch: 0, CloseDay: "2026-10-08", CardSeq: 0}
}

// wire checks that file requires exactly the JSON fields typ declares
// itself, each with a property definition.
func wire(t *testing.T, file string, typ reflect.Type) {
	t.Helper()
	var own []string
	for i := 0; i < typ.NumField(); i++ {
		if f := typ.Field(i); !f.Anonymous {
			own = append(own, f.Tag.Get("json"))
		}
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Required   []string                   `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	for _, k := range doc.Required {
		if len(doc.Properties[k]) == 0 {
			t.Fatalf("%s: no property for required %s", file, k)
		}
	}
	sort.Strings(own)
	sort.Strings(doc.Required)
	if !reflect.DeepEqual(own, doc.Required) {
		t.Fatalf("%s: required %v, wire fields %v", file, doc.Required, own)
	}
}

// check validates v unchanged and rejects every named mutation of it.
func check(t *testing.T, s *jsonschema.Schema, v any, bad map[string]func(map[string]any)) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	decode := func() map[string]any {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if err := s.Validate(decode()); err != nil {
		t.Fatalf("valid value rejected: %v", err)
	}
	for name, mutate := range bad {
		m := decode()
		mutate(m)
		if s.Validate(m) == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestAuthRequestSchema(t *testing.T) {
	wire(t, "auth-request.schema.json", reflect.TypeOf(auth.Request{}))
	check(t, compile(t, "urn:swipeline:auth-request:v1"), request(t), map[string]func(map[string]any){
		"missing received":      func(m map[string]any) { delete(m, "received_at_ms") },
		"null seal epoch":       func(m map[string]any) { m["seal_epoch"] = nil },
		"negative card seq":     func(m map[string]any) { m["accepted_card_seq"] = -1 },
		"fractional deadline":   func(m map[string]any) { m["scoring_deadline_at_ms"] = 1.5 },
		"adaptive mode":         func(m map[string]any) { m["deadline_mode"] = "adaptive" },
		"bad close day":         func(m map[string]any) { m["close_day"] = "2026-13-40" },
		"missing business card": func(m map[string]any) { delete(m, "card_token") },
		"missing envelope id":   func(m map[string]any) { delete(m, "event_id") },
	})
}
