package dashboard

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type ChartDef struct {
	Name        string
	SliceName   string
	VizType     string
	UUID        string
	Description string
}

var ChartDefinitions = []ChartDef{
	{
		Name:        "tps",
		SliceName:   "Throughput (TPS)",
		VizType:     "echarts_timeseries_line",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000011",
		Description: "Throughput in transactions per second (10s buckets)",
	},
	{
		Name:        "approval_rate",
		SliceName:   "Approval Rate",
		VizType:     "big_number_total",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000012",
		Description: "Approved final decisions divided by all final decisions (including stand-ins)",
	},
	{
		Name:        "decline_reasons",
		SliceName:   "Decline Reasons",
		VizType:     "table",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000013",
		Description: "Count of declined decisions grouped by primary decline reason and source",
	},
	{
		Name:        "fraud_flags_by_mcc",
		SliceName:   "Fraud Flags by MCC",
		VizType:     "table",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000014",
		Description: "Fraud flags by MCC. Fraud flags can overlap across transactions.",
	},
	{
		Name:        "latency",
		SliceName:   "Decision Latency",
		VizType:     "table",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000015",
		Description: "Decision latency distribution. Druid latency measures admission to final choice and excludes the commit, with Prometheus as the authority for the 50/100 ms targets.",
	},
	{
		Name:        "freshness",
		SliceName:   "Freshness and Integrity",
		VizType:     "table",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000016",
		Description: "Ingestion freshness and stand-in metrics. merchant_distinct_estimate: approximate distinct cards in recent 60 seconds.",
	},
}

const DatasetUUID = "e4b3c2a1-0000-4000-8000-000000000002"

// ChartQuery returns Superset's structured query object. Raw SQL is not a
// supported chart-data query field; SQL expressions belong in columns/metrics.
func ChartQuery(name string) map[string]any {
	metric := func(label, expression string) map[string]any {
		return map[string]any{"expressionType": "SQL", "sqlExpression": expression, "label": label, "hasCustomLabel": true, "optionName": "metric_" + label}
	}
	column := func(label, expression string) map[string]any {
		return map[string]any{"expressionType": "SQL", "sqlExpression": expression, "label": label}
	}
	q := map[string]any{"columns": []any{}, "metrics": []any{}, "filters": []any{}, "time_range": "No filter", "row_limit": 10000}
	switch name {
	case "tps":
		q["columns"] = []any{column("time_bucket", "TIME_FLOOR(__time, 'PT10S')")}
		q["metrics"] = []any{metric("tps", "COUNT(*) / 10.0")}
		q["orderby"] = []any{[]any{column("time_bucket", "TIME_FLOOR(__time, 'PT10S')"), true}}
	case "approval_rate":
		q["metrics"] = []any{metric("approval_rate", `CASE WHEN COUNT(*) = 0 THEN NULL ELSE CAST(SUM(CASE WHEN "decision" = 'APPROVE' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) END`)}
	case "decline_reasons":
		q["columns"] = []any{"reason", "decision_source"}
		q["metrics"] = []any{metric("decline_count", "COUNT(*)")}
		q["filters"] = []any{map[string]any{"col": "decision", "op": "==", "val": "DECLINE"}}
	case "fraud_flags_by_mcc":
		q["columns"] = []any{"mcc"}
		metrics := []any{}
		for _, flag := range []string{"CARD_TESTING", "CARD_VELOCITY", "SPEND_ANOMALY"} {
			metrics = append(metrics, metric(flag, "SUM(CASE WHEN MV_CONTAINS(fraud_flags, '"+flag+"') THEN 1 ELSE 0 END)"))
		}
		q["metrics"] = metrics
	case "latency":
		q["columns"] = []any{column("latency_bucket", `CASE WHEN decision_latency_ms < 5 THEN '0-5' WHEN decision_latency_ms < 10 THEN '5-10' WHEN decision_latency_ms < 20 THEN '10-20' WHEN decision_latency_ms < 35 THEN '20-35' WHEN decision_latency_ms < 50 THEN '35-50' ELSE '50+' END`)}
		q["metrics"] = []any{metric("bucket_count", "COUNT(*)"), metric("mean_latency_ms", "AVG(decision_latency_ms)"), metric("max_latency_ms", "MAX(decision_latency_ms)")}
	case "freshness":
		q["metrics"] = []any{metric("max_time", "MAX(__time)"), metric("ingestion_lag_seconds", "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - EXTRACT(EPOCH FROM MAX(__time))"), metric("total_count", "COUNT(*)"), metric("standin_share", `CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE CAST(SUM(CASE WHEN decision_source = 'STAND_IN' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) END`)}
	}
	return q
}

func FormatChartYAML(def ChartDef, _ string) string {
	q := ChartQuery(def.Name)
	// Form controls also need real metrics/columns so opening or filtering a chart
	// regenerates the same query in the browser.
	params := map[string]any{"datasource": DatasetUUID + "__table", "viz_type": def.VizType, "query_mode": "aggregate", "time_range": "No filter", "row_limit": 10000, "adhoc_filters": []any{}, "metrics": q["metrics"], "groupby": q["columns"], "all_columns": []any{}}
	filters := []any{}
	for _, item := range q["filters"].([]any) {
		f := item.(map[string]any)
		filters = append(filters, map[string]any{"expressionType": "SIMPLE", "clause": "WHERE", "subject": f["col"], "operator": f["op"], "comparator": f["val"]})
	}
	params["adhoc_filters"] = filters
	if def.Name == "approval_rate" {
		params["metric"] = q["metrics"].([]any)[0]
	}
	if def.Name == "tps" {
		params["x_axis"] = q["columns"].([]any)[0]
		params["groupby"] = []any{}
		params["time_series_limit"] = 0
	}
	context := map[string]any{"datasource": map[string]any{"id": DatasetUUID, "type": "table"}, "queries": []any{q}, "result_format": "json", "result_type": "full"}
	paramsJSON, _ := json.Marshal(params)
	contextJSON, _ := json.Marshal(context)
	sliceNameJSON, _ := json.Marshal(def.SliceName)
	descriptionJSON, _ := json.Marshal(def.Description)
	return fmt.Sprintf(`slice_name: %s
viz_type: %s
params: %s
query_context: |
  %s
cache_timeout: null
uuid: %s
version: 1.0.0
dataset_uuid: %s
description: %s
`, sliceNameJSON, def.VizType, paramsJSON, contextJSON, def.UUID, DatasetUUID, descriptionJSON)
}

func GenerateCharts(queriesDir, chartsDir string, chartNames ...string) error {
	namesMap := make(map[string]bool)
	for _, n := range chartNames {
		namesMap[n] = true
	}

	for _, def := range ChartDefinitions {
		if len(namesMap) > 0 && !namesMap[def.Name] {
			continue
		}
		sqlFile := filepath.Join(queriesDir, def.Name+".sql")
		sqlData, err := os.ReadFile(sqlFile)
		if err != nil {
			return fmt.Errorf("read sql file %s: %w", sqlFile, err)
		}

		yamlContent := FormatChartYAML(def, string(sqlData))
		outFile := filepath.Join(chartsDir, def.Name+".yaml")
		if err := os.MkdirAll(chartsDir, 0755); err != nil {
			return err
		}
		if err := os.WriteFile(outFile, []byte(yamlContent), 0644); err != nil {
			return fmt.Errorf("write chart yaml %s: %w", outFile, err)
		}
	}
	return nil
}

func BuildBundleZip(srcDir, zipPath string) error {
	outFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	w := zip.NewWriter(outFile)
	defer w.Close()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		f, err := w.Create(rel)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = f.Write(data)
		return err
	})
}
