package schemas

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRegisteredSchemasValidateGeneratedEvents(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	for _, name := range []string{"common.schema.json", "preview.schema.json"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource(doc["$id"].(string), doc); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"urn:swipeline:common:v1", "urn:swipeline:preview:v1"} {
		if _, err := compiler.Compile(id); err != nil {
			t.Fatalf("compile %s: %v", id, err)
		}
	}
	schema, err := compiler.Compile("urn:swipeline:preview:v1")
	if err != nil {
		t.Fatal(err)
	}
	g, err := generator.New(generator.Config{RunID: "schema-test", Seed: 42, Day: "2026-10-08", Events: 10})
	if err != nil {
		t.Fatal(err)
	}
	event, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"valid", nil},
		{"missing common field", func(v map[string]any) { delete(v, "event_id") }},
		{"missing business field", func(v map[string]any) { delete(v, "amount_minor") }},
		{"null common field", func(v map[string]any) { v["source_seq"] = nil }},
		{"null business field", func(v map[string]any) { v["card_token"] = nil }},
		{"fractional money", func(v map[string]any) { v["amount_minor"] = 1.5 }},
		{"wrong currency", func(v map[string]any) { v["currency"] = "EUR" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil {
				tc.mutate(value)
			}
			err := schema.Validate(value)
			if (err == nil) != (tc.mutate == nil) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestSchemasMatchRequiredWireFields(t *testing.T) {
	wanted := map[string]bool{}
	for _, typ := range []reflect.Type{reflect.TypeOf(contract.Event{}), reflect.TypeOf(contract.Business{})} {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if !f.Anonymous {
				wanted[f.Tag.Get("json")] = true
			}
		}
	}
	seen := map[string]bool{}
	for _, name := range []string{"common.schema.json", "preview.schema.json"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Required   []string                  `json:"required"`
			Properties map[string]map[string]any `json:"properties"`
		}
		if err = json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		for _, key := range doc.Required {
			if !wanted[key] || doc.Properties[key] == nil {
				t.Fatalf("invalid schema field %s", key)
			}
			seen[key] = true
			kind := doc.Properties[key]["type"]
			if kind != "integer" && kind != "string" {
				t.Fatalf("nullable or untyped %s", key)
			}
		}
	}
	if !reflect.DeepEqual(wanted, seen) {
		t.Fatalf("wire/schema mismatch: %v / %v", wanted, seen)
	}
}
