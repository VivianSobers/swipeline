package dashboard

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		VizType:     "echarts_timeseries_bar",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000013",
		Description: "Count of declined decisions grouped by primary decline reason and source",
	},
	{
		Name:        "fraud_flags_by_mcc",
		SliceName:   "Fraud Flags by MCC",
		VizType:     "echarts_timeseries_bar",
		UUID:        "e4b3c2a1-0000-4000-8000-000000000014",
		Description: "Fraud flags by MCC. Fraud flags can overlap across transactions.",
	},
	{
		Name:        "latency",
		SliceName:   "Decision Latency",
		VizType:     "echarts_timeseries_bar",
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

func FormatChartYAML(def ChartDef, sqlQuery string) string {
	sqlClean := strings.TrimSpace(sqlQuery)
	lines := strings.Split(sqlClean, "\n")
	for i, line := range lines {
		lines[i] = "    " + line
	}
	indentedSQL := strings.Join(lines, "\n")

	return fmt.Sprintf(`slice_name: %s
viz_type: %s
params:
  adhoc_filters: []
  datasource: %s__table
  viz_type: %s
  query_mode: sql
  sql: |
%s
query_context: null
cache_timeout: null
uuid: %s
version: 1.0.0
dataset_uuid: %s
description: %s
`, def.SliceName, def.VizType, DatasetUUID, def.VizType, indentedSQL, def.UUID, DatasetUUID, def.Description)
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
