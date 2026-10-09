package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFlinkStartRestoresNewestCompleteCheckpoint(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state with spaces")
	checkpoint := func(job, name string, complete bool, age time.Duration) string {
		t.Helper()
		path := filepath.Join(root, "checkpoints", job, name)
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatal(err)
		}
		if complete {
			meta := filepath.Join(path, "_metadata")
			if err := os.WriteFile(meta, []byte("checkpoint"), 0644); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now().Add(age)
			if err := os.Chtimes(meta, stamp, stamp); err != nil {
				t.Fatal(err)
			}
		}
		return path
	}
	checkpoint("old-job", "chk-999", true, -time.Hour)
	want := checkpoint("selected-job", "chk-2", true, -time.Minute)
	checkpoint("selected-job", "chk-1", true, -2*time.Minute)
	checkpoint("incomplete-job", "chk-1000", false, 0)
	checkpoint("selected-job", "chk-3", false, 0)
	cmd := exec.Command("sh", "../deploy/flink-start.sh", "standalone-job", "--job-classname", "io.swipeline.Authorization")
	cmd.Env = append(os.Environ(), "SWIPELINE_FLINK_STATE="+root, "SWIPELINE_FLINK_ENTRYPOINT=echo")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("start: %v: %s", err, out)
	}
	expected := "standalone-job --job-classname io.swipeline.Authorization --fromSavepoint " + want
	if strings.TrimSpace(string(out)) != expected {
		t.Fatalf("arguments: %q want %q", out, expected)
	}
	for _, job := range []string{"old-job", "incomplete-job"} {
		if _, err := os.Stat(filepath.Join(root, "checkpoints", job)); !os.IsNotExist(err) {
			t.Fatalf("%s was not removed: %v", job, err)
		}
	}
	for _, path := range []string{want, filepath.Join(root, "checkpoints", "selected-job", "chk-1")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("selected job must be retained: %v", err)
		}
	}
}

func TestFlinkStartWithoutCheckpoint(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "incomplete"}[incomplete], func(t *testing.T) {
			root := t.TempDir()
			if incomplete {
				if err := os.MkdirAll(filepath.Join(root, "checkpoints", "job", "chk-9"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("sh", "../deploy/flink-start.sh", "standalone-job", "--job-classname", "io.swipeline.Authorization")
			cmd.Env = append(os.Environ(), "SWIPELINE_FLINK_STATE="+root, "SWIPELINE_FLINK_ENTRYPOINT=echo")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("start: %v: %s", err, out)
			}
			if strings.TrimSpace(string(out)) != "standalone-job --job-classname io.swipeline.Authorization" {
				t.Fatalf("fresh start arguments: %s", out)
			}
		})
	}
}
