package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFailureLogsOrderAndFullGateway(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "docker_invocations.log")

	stubDocker := filepath.Join(tempDir, "docker")
	stubContent := `#!/bin/sh
echo "$@" >> "` + logFile + `"
case "$*" in
	*"up "*)
		echo "simulated startup failure" >&2
		exit 1
		;;
	*"down "*)
		exit 0
		;;
	*"config"*"--services"*)
		printf 'gateway\npostgres\nkafka\nflink-jobmanager\nflink-taskmanager\nprometheus\nreplayer\n'
		exit 0
		;;
	*"logs"*)
		case "$*" in
			*"gateway"*)
				for i in $(seq 1 250); do
					echo "gateway-full-log-line-$i"
				done
				;;
			*"postgres"*)
				echo "postgres-full-log-line-1"
				;;
			*"--tail 200"*)
				echo "old-unfiltered-tail-200-log"
				;;
			*"--tail 100"*)
				for last; do :; done
				echo "tail-100-log-for-$last"
				;;
			*)
				echo "unknown-logs: $*"
				;;
		esac
		exit 0
		;;
	*)
		exit 0
		;;
esac
`
	if err := os.WriteFile(stubDocker, []byte(stubContent), 0755); err != nil {
		t.Fatal(err)
	}

	runScript := func(t *testing.T, script string, args ...string) (string, []string) {
		t.Helper()
		_ = os.Remove(logFile)
		cmdArgs := append([]string{script}, args...)
		cmd := exec.Command("sh", cmdArgs...)
		cmd.Env = append(os.Environ(), "PATH="+tempDir+":"+os.Getenv("PATH"))
		out, _ := cmd.CombinedOutput()
		output := string(out)

		var invocations []string
		if data, err := os.ReadFile(logFile); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				if line != "" {
					invocations = append(invocations, line)
				}
			}
		}
		return output, invocations
	}

	assertOrderAndFullLog := func(t *testing.T, output string, invocations []string) {
		t.Helper()
		if strings.Contains(output, "old-unfiltered-tail-200-log") {
			t.Fatalf("failure cleanup used old --tail 200 log printing")
		}

		if !strings.Contains(output, "gateway-full-log-line-1\n") || !strings.Contains(output, "gateway-full-log-line-250\n") {
			t.Fatalf("failure cleanup missing complete gateway log:\n%s", output)
		}
		if !strings.Contains(output, "postgres-full-log-line-1") {
			t.Fatalf("failure cleanup missing postgres log:\n%s", output)
		}

		gwPos := strings.Index(output, "gateway-full-log-line-1")
		pgPos := strings.Index(output, "postgres-full-log-line-1")
		if gwPos == -1 || pgPos == -1 || gwPos >= pgPos {
			t.Fatalf("expected gateway logs before postgres in output: gwPos=%d, pgPos=%d", gwPos, pgPos)
		}

		var logCalls []string
		for _, inv := range invocations {
			if strings.Contains(inv, "logs") {
				logCalls = append(logCalls, inv)
			}
		}
		if len(logCalls) < 3 {
			t.Fatalf("expected at least 3 log invocations (gateway, postgres, other services), got %d: %v", len(logCalls), logCalls)
		}

		if !strings.Contains(logCalls[0], "gateway") || strings.Contains(logCalls[0], "--tail") {
			t.Fatalf("first logs call must be complete gateway logs, got: %s", logCalls[0])
		}
		if !strings.Contains(logCalls[1], "postgres") || strings.Contains(logCalls[1], "--tail") {
			t.Fatalf("second logs call must be complete postgres logs, got: %s", logCalls[1])
		}
		for i, call := range logCalls[2:] {
			if !strings.Contains(call, "--tail 100") {
				t.Fatalf("logs call %d must have --tail 100, got: %s", i+2, call)
			}
			if strings.Contains(call, "gateway") || strings.Contains(call, "postgres") {
				t.Fatalf("logs call %d should not be gateway or postgres: %s", i+2, call)
			}
		}
	}

	t.Run("m2-demo", func(t *testing.T) {
		output, invocations := runScript(t, "m2-demo.sh")
		assertOrderAndFullLog(t, output, invocations)
	})

	t.Run("m2-latency", func(t *testing.T) {
		output, invocations := runScript(t, "m2-latency.sh")
		assertOrderAndFullLog(t, output, invocations)
	})

	t.Run("dump-logs-func", func(t *testing.T) {
		output, invocations := runScript(t, "dump-logs.sh", "test-project")
		assertOrderAndFullLog(t, output, invocations)
	})
}
