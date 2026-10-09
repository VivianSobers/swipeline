package schemas

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/VivianSobers/swipeline/internal/contract"
)

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
