package store

import (
	"strings"
	"testing"
)

func TestPostingAssignmentsMigrationEmbedded(t *testing.T) {
	content, err := migrations.ReadFile("migrations/009_posting_assignments.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"ALTER TABLE ingest_batches", "posting_assignments JSONB NOT NULL DEFAULT '{}'::jsonb"} {
		if !strings.Contains(string(content), expected) {
			t.Fatalf("posting assignment migration missing %q", expected)
		}
	}
}
