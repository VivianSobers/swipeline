package dashboard

import (
	"encoding/json"
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
