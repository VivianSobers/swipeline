package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/VivianSobers/swipeline/internal/dashboard"
)

func TestMainGenerateCharts(t *testing.T) {
	tmpDir := t.TempDir()
	queriesDir := filepath.Join(tmpDir, "queries")
	chartsDir := filepath.Join(tmpDir, "charts")
	if err := os.MkdirAll(queriesDir, 0755); err != nil {
		t.Fatal(err)
	}

	for _, def := range dashboard.ChartDefinitions {
		if err := os.WriteFile(filepath.Join(queriesDir, def.Name+".sql"), []byte("SELECT 1"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	if err := dashboard.GenerateCharts(queriesDir, chartsDir, "tps"); err != nil {
		t.Fatalf("GenerateCharts failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(chartsDir, "tps.yaml")); err != nil {
		t.Errorf("tps.yaml not created")
	}
}
