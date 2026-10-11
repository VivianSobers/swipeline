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

func TestAnalyticsMakefileSupersetCheck(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		data, err = os.ReadFile("../Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)

	if !strings.Contains(content, "superset-check:") {
		t.Fatal("Makefile missing superset-check target")
	}
	if !strings.Contains(content, ".PHONY: superset-check") {
		t.Fatal("Makefile missing .PHONY: superset-check")
	}

	idx := strings.Index(content, "superset-check:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}

	for _, want := range []string{
		"deploy/superset.Dockerfile",
		"swipeline-superset:m3",
		"pydruid",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("superset-check missing %q", want)
		}
	}
}

func TestAnalyticsMakefileSupersetPassword(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		data, err = os.ReadFile("../Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)

	if !strings.Contains(content, "superset-password:") {
		t.Fatal("Makefile missing superset-password target")
	}
	if !strings.Contains(content, ".PHONY: superset-password") {
		t.Fatal("Makefile missing .PHONY: superset-password")
	}

	idx := strings.Index(content, "superset-password:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}

	for _, want := range []string{
		"docker compose exec -T superset cat /secrets/admin-password",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("superset-password missing %q", want)
		}
	}
}

func TestAnalyticsMakefilePreflightOrder(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		data, err = os.ReadFile("../Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)

	// Check m3-druid-smoke
	idxSmoke := strings.Index(content, "m3-druid-smoke:")
	if idxSmoke == -1 {
		t.Fatal("Makefile missing m3-druid-smoke target")
	}
	blockSmoke := content[idxSmoke:]
	if next := strings.Index(blockSmoke[1:], "\n.PHONY:"); next != -1 {
		blockSmoke = blockSmoke[:next+1]
	}
	smokeLines := strings.Split(strings.TrimSpace(blockSmoke), "\n")
	if len(smokeLines) < 2 || !strings.Contains(smokeLines[1], "preflight.sh full") {
		t.Fatalf("m3-druid-smoke first step must call preflight.sh full, got: %s", smokeLines[1])
	}

	// Check m3-demo
	idxDemo := strings.Index(content, "m3-demo:")
	if idxDemo == -1 {
		t.Fatal("Makefile missing m3-demo target")
	}
	blockDemo := content[idxDemo:]
	if next := strings.Index(blockDemo[1:], "\n.PHONY:"); next != -1 {
		blockDemo = blockDemo[:next+1]
	}
	demoLines := strings.Split(strings.TrimSpace(blockDemo), "\n")
	if len(demoLines) < 2 || !strings.Contains(demoLines[1], "preflight.sh full") {
		t.Fatalf("m3-demo first step must call preflight.sh full, got: %s", demoLines[1])
	}
}
