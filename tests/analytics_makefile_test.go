package tests

import (
	"os"
	"strings"
	"testing"
)

func TestAnalyticsMakefileDruidSmoke(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		data, err = os.ReadFile("../Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)

	if !strings.Contains(content, "m3-druid-smoke:") {
		t.Fatal("Makefile missing m3-druid-smoke target")
	}
	if !strings.Contains(content, ".PHONY: m3-druid-smoke") {
		t.Fatal("Makefile missing .PHONY: m3-druid-smoke")
	}

	// Find the m3-druid-smoke target block
	idx := strings.Index(content, "m3-druid-smoke:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}

	for _, want := range []string{
		"COMPOSE_PROFILES=full",
		"down -v",
		"--wait",
		"druid-router:8888",
		"SELECT 1",
		"logs",
		":ro,z",
		"postgres",
		"kafka",
		"druid-zk",
		"druid-coordinator-overlord",
		"druid-broker",
		"druid-historical",
		"druid-middlemanager",
		"druid-router",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("m3-druid-smoke missing %q", want)
		}
	}
}
