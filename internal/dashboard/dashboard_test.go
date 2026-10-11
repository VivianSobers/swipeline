package dashboard

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartsContainExecutableQueryContexts(t *testing.T) {
	expectedMetrics := map[string][]string{
		"tps": {"tps"}, "approval_rate": {"approval_rate"},
		"decline_reasons":    {"decline_count"},
		"fraud_flags_by_mcc": {"CARD_TESTING", "CARD_VELOCITY", "SPEND_ANOMALY"},
		"latency":            {"bucket_count", "mean_latency_ms", "max_latency_ms"},
		"freshness":          {"max_time", "ingestion_lag_seconds", "total_count", "standin_share"},
	}
	if len(ChartDefinitions) != len(expectedMetrics) {
		t.Fatalf("expected six charts, got %d", len(ChartDefinitions))
	}
	for _, def := range ChartDefinitions {
		t.Run(def.Name, func(t *testing.T) {
			got := FormatChartYAML(def, "SELECT broken_raw_sql")
			var context struct {
				Datasource struct {
					ID   string `json:"id"`
					Type string `json:"type"`
				} `json:"datasource"`
				Queries []struct {
					Metrics []struct {
						Label      string `json:"label"`
						Expression string `json:"sqlExpression"`
					} `json:"metrics"`
					SQL string `json:"sql"`
				} `json:"queries"`
				Format     string `json:"result_format"`
				ResultType string `json:"result_type"`
			}
			_, tail, ok := strings.Cut(got, "query_context: |\n  ")
			if !ok {
				t.Fatal("missing query context")
			}
			contextJSON, _, _ := strings.Cut(tail, "\n")
			if err := json.Unmarshal([]byte(contextJSON), &context); err != nil {
				t.Fatal(err)
			}
			if context.Datasource.ID != DatasetUUID || context.Datasource.Type != "table" || context.Format != "json" || context.ResultType != "full" || len(context.Queries) != 1 {
				t.Fatalf("invalid query context: %+v", context)
			}
			query := context.Queries[0]
			want, known := expectedMetrics[def.Name]
			if !known || query.SQL != "" || len(query.Metrics) != len(want) {
				t.Fatalf("invalid query for %s: %+v", def.Name, query)
			}
			for i, metric := range query.Metrics {
				if metric.Label != want[i] || strings.TrimSpace(metric.Expression) == "" {
					t.Fatalf("invalid metric: %+v", metric)
				}
			}
			if strings.Contains(got, "broken_raw_sql") {
				t.Fatal("unsupported raw SQL leaked into chart")
			}
		})
	}
}

func TestChartMetadataIsQuotedForYAML(t *testing.T) {
	def := ChartDefinitions[0]
	def.SliceName = "A: chart #1"
	def.Description = "Value: details\nsecond line"
	got := FormatChartYAML(def, "")
	for key, want := range map[string]string{"slice_name": def.SliceName, "description": def.Description} {
		var decoded string
		_, tail, ok := strings.Cut(got, key+": ")
		if !ok {
			t.Fatalf("missing %s", key)
		}
		value, _, _ := strings.Cut(tail, "\n")
		if err := json.Unmarshal([]byte(value), &decoded); err != nil || decoded != want {
			t.Fatalf("%s is not a safely quoted YAML scalar: %q (%v)", key, value, err)
		}
	}
}

func TestBundlePreservesImporterRoot(t *testing.T) {
	source := filepath.Join(t.TempDir(), "payments-operations")
	if err := os.MkdirAll(filepath.Join(source, "charts"), 0755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"metadata.yaml": "version: 1.0.0\ntype: Dashboard\n", "charts/a.yaml": "slice_name: test\n"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(t.TempDir(), "bundle.zip")
	if err := BuildBundleZip(source, output); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := BuildBundleZip(source, output); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("bundle changed on repeat build")
	}
	archive, err := zip.OpenReader(output)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	for _, entry := range archive.File {
		if !strings.HasPrefix(entry.Name, "payments-operations/") {
			t.Fatalf("Superset strips first path component; missing common root: %s", entry.Name)
		}
	}
}
