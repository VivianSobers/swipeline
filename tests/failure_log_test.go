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

// Exercise the real shell flow while replacing container execution and Go tests.
func TestAuthorizationFailureLogsBeforeRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		auth, recovery, canonical string
		wantDumps                 int
		wantFailure               bool
	}{
		{"authorization", "1", "0", "0", 1, true},
		{"recovery", "0", "1", "0", 1, true},
		{"both", "1", "1", "0", 2, true},
		{"canonical", "0", "0", "1", 1, true},
		{"authorization-and-canonical", "1", "0", "1", 2, true},
		{"success", "0", "0", "0", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stubs := map[string]string{
				"docker": `#!/bin/sh
case "$1" in
 run)
  while [ "$1" != "sh" ]; do shift; done
  exec "$@"
  ;;
 compose)
  case "$*" in
   *"config --services"*) printf 'gateway\npostgres\nflink-jobmanager\n' ;;
   *"logs --no-color gateway"*) echo gateway-dump ;;
  esac
  ;;
esac
`,
				"go": `#!/bin/sh
echo "test invocation: $*"
case "$*" in
 *TestInstalledAuthorization*) echo authorization; exit "$AUTH_STATUS" ;;
 *TestLateProposalAfterStandIn*TestGatewayRestartRecovers*) echo recovery; exit "$RECOVERY_STATUS" ;;
 *TestCanonical*) echo canonical; exit "$CANONICAL_STATUS" ;;
 *) exit 99 ;;
esac
`,
			}
			for name, content := range stubs {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("sh", "m2-demo.sh")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"),
				"AUTH_STATUS="+tc.auth, "RECOVERY_STATUS="+tc.recovery, "CANONICAL_STATUS="+tc.canonical)
			out, err := cmd.CombinedOutput()
			output := string(out)
			if (err != nil) != tc.wantFailure {
				t.Errorf("exit error=%v, want failure=%t\n%s", err, tc.wantFailure, output)
			}
			auth := strings.Index(output, "\nauthorization\n")
			recovery := strings.Index(output, "\nrecovery\n")
			if auth < 0 || recovery <= auth {
				t.Errorf("authorization must run before a separate recovery invocation:\n%s", output)
			}
			if tc.auth == "1" {
				dump := strings.Index(output, "gateway-dump")
				if dump <= auth || dump >= recovery {
					t.Errorf("authorization logs must precede recovery:\n%s", output)
				}
			}
			if got := strings.Count(output, "gateway-dump"); got != tc.wantDumps {
				t.Errorf("log dumps=%d, want %d:\n%s", got, tc.wantDumps, output)
			}
			if tc.recovery == "0" && !strings.Contains(output, "\ncanonical\n") {
				t.Errorf("canonical test run missing:\n%s", output)
			}
		})
	}
}
