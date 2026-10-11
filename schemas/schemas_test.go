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

func TestLifecycleFixturesValidateAgainstSchema(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	for _, name := range []string{"common.schema.json", "lifecycle.schema.json"} {
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
	schema, err := compiler.Compile("urn:swipeline:lifecycle:v1")
	if err != nil {
		t.Fatal(err)
	}

	fixtures := []string{
		"fixtures/lifecycle_clear.json",
		"fixtures/lifecycle_reverse.json",
		"fixtures/lifecycle_chargeback.json",
	}

	for _, fixturePath := range fixtures {
		t.Run(fixturePath, func(t *testing.T) {
			data, err := os.ReadFile(fixturePath)
			if err != nil {
				t.Fatalf("read fixture %s: %v", fixturePath, err)
			}
			var doc map[string]any
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatalf("unmarshal fixture %s: %v", fixturePath, err)
			}
			if err := schema.Validate(doc); err != nil {
				t.Fatalf("fixture %s failed validation: %v", fixturePath, err)
			}
		})
	}

	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "missing parent_event_id",
			mutate: func(v map[string]any) {
				delete(v, "parent_event_id")
			},
		},
		{
			name: "negative amount_minor",
			mutate: func(v map[string]any) {
				v["amount_minor"] = -5000
			},
		},
		{
			name: "invalid kind",
			mutate: func(v map[string]any) {
				v["kind"] = "INVALID_KIND"
			},
		},
		{
			name: "lifecycle_seq below range",
			mutate: func(v map[string]any) {
				v["lifecycle_seq"] = 1
			},
		},
		{
			name: "lifecycle_seq above range",
			mutate: func(v map[string]any) {
				v["lifecycle_seq"] = 4
			},
		},
		{
			name: "missing authorization_date",
			mutate: func(v map[string]any) {
				delete(v, "authorization_date")
			},
		},
		{
			name: "missing currency",
			mutate: func(v map[string]any) {
				delete(v, "currency")
			},
		},
		{
			name: "non-USD currency",
			mutate: func(v map[string]any) {
				v["currency"] = "EUR"
			},
		},
		{
			name: "missing merchant_id",
			mutate: func(v map[string]any) {
				delete(v, "merchant_id")
			},
		},
		{
			name: "missing issuer_bank_id",
			mutate: func(v map[string]any) {
				delete(v, "issuer_bank_id")
			},
		},
		{
			name: "missing acquirer_bank_id",
			mutate: func(v map[string]any) {
				delete(v, "acquirer_bank_id")
			},
		},
		{
			name: "missing reference_version",
			mutate: func(v map[string]any) {
				delete(v, "reference_version")
			},
		},
		{
			name: "missing seal_epoch",
			mutate: func(v map[string]any) {
				delete(v, "seal_epoch")
			},
		},
		{
			name: "missing close_day",
			mutate: func(v map[string]any) {
				delete(v, "close_day")
			},
		},
	} {
		t.Run("invalid_"+tc.name, func(t *testing.T) {
			data, err := os.ReadFile("fixtures/lifecycle_clear.json")
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			tc.mutate(doc)
			if err := schema.Validate(doc); err == nil {
				t.Fatalf("expected validation failure for %s, got nil", tc.name)
			}
		})
	}
}

func TestLifecycleSchemaRequiredFields(t *testing.T) {
	data, err := os.ReadFile("lifecycle.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}

	expectedRequired := []string{
		"lifecycle_seq",
		"kind",
		"parent_event_id",
		"authorization_date",
		"currency",
		"amount_minor",
		"merchant_id",
		"issuer_bank_id",
		"acquirer_bank_id",
		"reference_version",
		"seal_epoch",
		"close_day",
	}

	requiredSet := make(map[string]bool)
	for _, r := range doc.Required {
		requiredSet[r] = true
	}

	for _, expected := range expectedRequired {
		if !requiredSet[expected] {
			t.Errorf("missing required field in schema: %s", expected)
		}
		if doc.Properties[expected] == nil {
			t.Errorf("missing property definition in schema: %s", expected)
		}
	}
}
