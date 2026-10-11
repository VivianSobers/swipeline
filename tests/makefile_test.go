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
