package tests

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/VivianSobers/swipeline/internal/dashboard"
)

var allowedColumns = map[string]bool{
	"__time":                     true,
	"run_id":                     true,
	"transaction_id":             true,
	"decision_id":                true,
	"merchant_id":                true,
	"mcc":                        true,
	"issuer_bank_id":             true,
	"acquirer_bank_id":           true,
	"currency":                   true,
	"decision":                   true,
	"reason":                     true,
	"decision_source":            true,
	"rule_version":               true,
	"reference_version":          true,
	"fraud_flags":                true,
	"features_late":              true,
	"amount_minor":               true,
	"event_time_ms":              true,
	"decision_latency_ms":        true,
	"score":                      true,
	"velocity_60s":               true,
	"spend_ratio":                true,
	"merchant_distinct_estimate": true,
}

func TestDashboardQueries(t *testing.T) {
	queryFiles := []string{
		"tps.sql",
		"approval_rate.sql",
		"decline_reasons.sql",
		"fraud_flags_by_mcc.sql",
		"latency.sql",
		"freshness.sql",
	}

	baseDir := "dashboards/queries"
	if _, err := os.Stat(baseDir); err != nil {
		baseDir = "../dashboards/queries"
	}

	placeholderRe := regexp.MustCompile(`:([a-zA-Z0-9_]+)`)
	identRe := regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\b|"([^"]+)"`)

	for _, file := range queryFiles {
		t.Run(file, func(t *testing.T) {
			path := filepath.Join(baseDir, file)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("failed to read %s: %v", file, err)
			}
			sql := string(data)

			// Placeholders check
			matches := placeholderRe.FindAllStringSubmatch(sql, -1)
			if len(matches) == 0 {
				t.Errorf("%s has no placeholders", file)
			}
			hasRunID := false
			for _, m := range matches {
				if m[1] == "run_id" {
					hasRunID = true
				}
			}
			if !hasRunID {
				t.Errorf("%s missing :run_id placeholder", file)
			}
			if !strings.Contains(sql, "run_id = :run_id") && !strings.Contains(sql, "\"run_id\" = :run_id") {
				t.Errorf("%s does not filter by run_id = :run_id", file)
			}

			// Ensure forbidden column card_token is never referenced
			if strings.Contains(strings.ToLower(sql), "card_token") {
				t.Errorf("%s references prohibited column card_token", file)
			}

			// Specific file assertions
			switch file {
			case "tps.sql":
				if !strings.Contains(sql, "TIME_FLOOR(__time") {
					t.Errorf("tps.sql missing TIME_FLOOR(__time)")
				}
				if !strings.Contains(sql, "PT10S") {
					t.Errorf("tps.sql missing PT10S")
				}
				if !strings.Contains(sql, "/ 10") {
					t.Errorf("tps.sql missing division by 10")
				}
			case "approval_rate.sql":
				if !strings.Contains(sql, "APPROVE") {
					t.Errorf("approval_rate.sql missing APPROVE")
				}
				if !strings.Contains(sql, "NULL") {
					t.Errorf("approval_rate.sql missing NULL handling for zero count")
				}
			case "decline_reasons.sql":
				if !strings.Contains(sql, "reason") {
					t.Errorf("decline_reasons.sql missing reason")
				}
				if !strings.Contains(sql, "decision_source") {
					t.Errorf("decline_reasons.sql missing decision_source")
				}
			case "fraud_flags_by_mcc.sql":
				if strings.Contains(strings.ToUpper(sql), "UNNEST") {
					t.Errorf("fraud_flags_by_mcc.sql must not use UNNEST")
				}
				for _, flag := range []string{"CARD_TESTING", "CARD_VELOCITY", "SPEND_ANOMALY"} {
					if !strings.Contains(sql, "MV_CONTAINS(fraud_flags, '"+flag+"')") &&
						!strings.Contains(sql, "MV_CONTAINS(\"fraud_flags\", '"+flag+"')") {
						t.Errorf("fraud_flags_by_mcc.sql missing MV_CONTAINS for %s", flag)
					}
				}
				if !strings.Contains(sql, "mcc") {
					t.Errorf("fraud_flags_by_mcc.sql missing mcc")
				}
			case "latency.sql":
				for _, bucket := range []string{"0-5", "5-10", "10-20", "20-35", "35-50", "50+"} {
					if !strings.Contains(sql, bucket) {
						t.Errorf("latency.sql missing bucket %s", bucket)
					}
				}
				if !strings.Contains(strings.ToUpper(sql), "AVG") {
					t.Errorf("latency.sql missing AVG/mean")
				}
				if !strings.Contains(strings.ToUpper(sql), "MAX") {
					t.Errorf("latency.sql missing MAX")
				}
			case "freshness.sql":
				if !strings.Contains(sql, "MAX(__time)") {
					t.Errorf("freshness.sql missing MAX(__time)")
				}
				if !strings.Contains(sql, "CURRENT_TIMESTAMP") {
					t.Errorf("freshness.sql missing CURRENT_TIMESTAMP")
				}
				if !strings.Contains(sql, "STAND_IN") {
					t.Errorf("freshness.sql missing STAND_IN share")
				}
			}
		})
	}
	_ = identRe
}

func TestDruidDatabaseAndDataset(t *testing.T) {
	dbPath := "dashboards/payments-operations/databases/druid.yaml"
	if _, err := os.Stat(dbPath); err != nil {
		dbPath = "../dashboards/payments-operations/databases/druid.yaml"
	}
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", dbPath, err)
	}
	dbContent := string(dbData)

	if !strings.Contains(dbContent, "sqlalchemy_uri: druid://druid-broker:8082/druid/v2/sql/") {
		t.Errorf("db missing or incorrect sqlalchemy_uri")
	}
	if !strings.Contains(dbContent, "expose_in_sqllab: true") {
		t.Errorf("db missing expose_in_sqllab: true")
	}
	if !strings.Contains(dbContent, "allow_dml: false") {
		t.Errorf("db missing allow_dml: false")
	}

	dsPath := "dashboards/payments-operations/datasets/druid/swipeline_decisions.yaml"
	if _, err := os.Stat(dsPath); err != nil {
		dsPath = "../dashboards/payments-operations/datasets/druid/swipeline_decisions.yaml"
	}
	dsData, err := os.ReadFile(dsPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", dsPath, err)
	}
	dsContent := string(dsData)

	if !strings.Contains(dsContent, "table_name: swipeline_decisions") {
		t.Errorf("dataset missing table_name: swipeline_decisions")
	}
	if strings.Contains(strings.ToLower(dsContent), "card_token") {
		t.Errorf("dataset contains prohibited card_token column")
	}

	for col, _ := range allowedColumns {
		if !strings.Contains(dsContent, "column_name: "+col) {
			t.Errorf("dataset missing column %s", col)
		}
	}
}

func TestThroughputAndApprovalCharts(t *testing.T) {
	chartsDir := "dashboards/payments-operations/charts"
	if _, err := os.Stat(chartsDir); err != nil {
		chartsDir = "../dashboards/payments-operations/charts"
	}
	queriesDir := "dashboards/queries"
	if _, err := os.Stat(queriesDir); err != nil {
		queriesDir = "../dashboards/queries"
	}

	for _, name := range []string{"tps", "approval_rate"} {
		chartFile := filepath.Join(chartsDir, name+".yaml")
		data, err := os.ReadFile(chartFile)
		if err != nil {
			t.Fatalf("failed to read %s: %v", chartFile, err)
		}
		sqlData, err := os.ReadFile(filepath.Join(queriesDir, name+".sql"))
		if err != nil {
			t.Fatalf("failed to read sql %s: %v", name, err)
		}
		var def dashboard.ChartDef
		found := false
		for _, d := range dashboard.ChartDefinitions {
			if d.Name == name {
				def = d
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unknown chart %s", name)
		}

		want := dashboard.FormatChartYAML(def, string(sqlData))
		got := string(data)
		if got != want {
			t.Errorf("chart %s committed YAML does not match generator output:\ngot:\n%s\nwant:\n%s", name, got, want)
		}
	}
}
