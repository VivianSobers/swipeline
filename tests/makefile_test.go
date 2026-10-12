package tests

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Every bind mount the Makefile passes to docker run must carry the shared
// SELinux label so checks work on hosts where Docker enforces SELinux.
func TestMakefileBindMountsAreSELinuxShared(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	mounts := regexp.MustCompile(`-v "([^"]+)"`).FindAllStringSubmatch(string(data), -1)
	if len(mounts) == 0 {
		t.Fatal("no bind mounts found")
	}
	labelled := regexp.MustCompile(`:(ro,)?z$`)
	for _, m := range mounts {
		if !labelled.MatchString(m[1]) {
			t.Errorf("bind mount without z label: %s", m[1])
		}
	}
}

func TestSeaweedFSMakefileSmoke(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		data, err = os.ReadFile("Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)
	if !strings.Contains(content, ".PHONY: m4-seaweedfs-smoke") || !strings.Contains(content, "m4-seaweedfs-smoke:") {
		t.Fatal("Makefile missing m4-seaweedfs-smoke target")
	}
	idx := strings.Index(content, "m4-seaweedfs-smoke:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}
	for _, want := range []string{
		"seaweedfs",
		"down -v",
		"TestSeaweedFSS3Compatibility",
		":ro,z",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("m4-seaweedfs-smoke missing %q", want)
		}
	}
}

func TestM4DemoMakefileTargets(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		data, err = os.ReadFile("Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)
	if !strings.Contains(content, ".PHONY: m4-storage") || !strings.Contains(content, "m4-storage:") {
		t.Fatal("Makefile missing m4-storage target")
	}
	if !strings.Contains(content, ".PHONY: m4-demo") || !strings.Contains(content, "m4-demo:") {
		t.Fatal("Makefile missing m4-demo target")
	}
	idx := strings.Index(content, "m4-demo:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}
	for _, want := range []string{
		"tests/preflight.sh core",
		"tests/m4-demo.sh",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("m4-demo missing %q", want)
		}
	}
}

func TestBatchTestMakefileTarget(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		data, err = os.ReadFile("Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)
	if !strings.Contains(content, ".PHONY: batch-test") || !strings.Contains(content, "batch-test:") {
		t.Fatal("Makefile missing batch-test target")
	}
	idx := strings.Index(content, "batch-test:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}
	for _, want := range []string{
		"swipeline-spark:m4",
		"python -m unittest discover -s batch/tests -t .",
		":ro,z",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("batch-test missing %q", want)
		}
	}

	ciData, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		ciData, err = os.ReadFile(".github/workflows/ci.yml")
		if err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(string(ciData), "make batch-test") {
		t.Error("CI check job missing make batch-test")
	}
}

func TestWorkflowLintMakefileTarget(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		data, err = os.ReadFile("Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)
	if !strings.Contains(content, ".PHONY:") || !strings.Contains(content, "workflow-lint") {
		t.Fatal("Makefile missing workflow-lint target in .PHONY")
	}
	idx := strings.Index(content, "workflow-lint:")
	if idx == -1 {
		t.Fatal("Makefile missing workflow-lint target")
	}
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}

	// Actionlint image must be pinned to an exact version tag, never latest
	imageRe := regexp.MustCompile(`rhysd/actionlint:([a-zA-Z0-9._-]+)`)
	matches := imageRe.FindStringSubmatch(content)
	if len(matches) < 2 {
		t.Fatal("Makefile missing rhysd/actionlint image tag")
	}
	tag := matches[1]
	if tag == "latest" {
		t.Errorf("actionlint image must be pinned to an exact release version tag, got latest")
	}

	// Must bind-mount repository read-only with :z
	for _, want := range []string{
		":ro,z",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("workflow-lint target missing %q", want)
		}
	}

	ciData, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		ciData, err = os.ReadFile(".github/workflows/ci.yml")
		if err != nil {
			t.Fatal(err)
		}
	}
	ciContent := string(ciData)
	if !strings.Contains(ciContent, "make workflow-lint") {
		t.Error("CI workflow missing make workflow-lint step")
	}
	lintIdx := strings.Index(ciContent, "make workflow-lint")
	checkIdx := strings.Index(ciContent, "make check")
	if lintIdx == -1 || checkIdx == -1 || lintIdx > checkIdx {
		t.Error("make workflow-lint must appear before make check in CI check job")
	}
}
