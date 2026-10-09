package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Finals are assembled in SQL; both kinds must satisfy the published
// decision contract that downstream consumers validate against.
func TestStoredFinalsMatchDecisionSchema(t *testing.T) {
	s := database(t)
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	files, err := filepath.Glob("../../schemas/*.schema.json")
	if err != nil || len(files) == 0 {
		t.Fatal("no schemas", err)
	}
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err = json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if err = c.AddResource(doc["$id"].(string), doc); err != nil {
			t.Fatal(err)
		}
	}
	decision, err := c.Compile("urn:swipeline:decision:v1")
	if err != nil {
		t.Fatal(err)
	}
	_, e := fixture(t, s)
	late := admitAt(t, s, e, time.Now().Add(-time.Second))
	timely := sibling(t, e, "timely")
	r := admitAt(t, s, timely, time.Now().Add(time.Minute))
	for _, row := range []Row{finalize(t, s, Choice{ID: late.TransactionID}), finalize(t, s, Choice{ID: r.TransactionID, Proposal: approval(r)})} {
		data, err := json.Marshal(row.Final)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err = json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		if err = decision.Validate(value); err != nil {
			t.Fatalf("%s final violates decision schema: %v", row.Final.Source, err)
		}
	}
}
