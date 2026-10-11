package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreflightMemoryCheck(t *testing.T) {
	script := "tests/preflight.sh"
	if _, err := os.Stat(script); err != nil {
		script = "../tests/preflight.sh"
		if _, err := os.Stat(script); err != nil {
			t.Fatal("cannot find preflight.sh")
		}
	}

	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Fake docker command
	fakeDocker := filepath.Join(binDir, "docker")
	dockerContent := `#!/bin/sh
echo '{"services":{"s1":{"mem_limit":"1048576000"},"s2":{"mem_limit":"1048576000"}}}'
`
	if err := os.WriteFile(fakeDocker, []byte(dockerContent), 0755); err != nil {
		t.Fatal(err)
	}

	// Fake meminfo: 3000 MiB available (3072000 kB)
	// Required is 2000 MiB (from s1+s2) + 1536 MiB = 3536 MiB
	meminfoPath := filepath.Join(tmpDir, "meminfo")
	shortMeminfo := `MemTotal:       16000000 kB
MemFree:         1000000 kB
MemAvailable:    3072000 kB
`
	if err := os.WriteFile(meminfoPath, []byte(shortMeminfo), 0644); err != nil {
		t.Fatal(err)
	}

	// 1. Should fail when memory is short
	cmd := exec.Command("sh", script, "full")
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"SWIPELINE_MEMINFO="+meminfoPath,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected preflight.sh to fail when memory is short, but succeeded: %s", string(out))
	}
	outStr := strings.TrimSpace(string(out))
	if !strings.Contains(outStr, "required=3536 available=3000") {
		t.Fatalf("expected output to contain 'required=3536 available=3000', got: %s", outStr)
	}

	// 2. Should pass when memory is sufficient: 5000 MiB available (5120000 kB)
	sufficientMeminfo := `MemTotal:       16000000 kB
MemFree:         4000000 kB
MemAvailable:    5120000 kB
`
	if err := os.WriteFile(meminfoPath, []byte(sufficientMeminfo), 0644); err != nil {
		t.Fatal(err)
	}

	cmdPass := exec.Command("sh", script, "full")
	cmdPass.Env = append(os.Environ(),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"SWIPELINE_MEMINFO="+meminfoPath,
	)
	outPass, errPass := cmdPass.CombinedOutput()
	if errPass != nil {
		t.Fatalf("expected preflight.sh to pass when memory is sufficient, but failed: %v: %s", errPass, string(outPass))
	}
	outPassStr := strings.TrimSpace(string(outPass))
	if !strings.Contains(outPassStr, "required=3536 available=5000") {
		t.Fatalf("expected output to contain 'required=3536 available=5000', got: %s", outPassStr)
	}
}
