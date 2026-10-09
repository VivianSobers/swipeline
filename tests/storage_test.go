package tests

import (
	"os"
	"strings"
	"testing"
)

func lines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "--") && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

// The gateway owns an isolated database that no other role can connect to.
func TestGatewayDatabaseInit(t *testing.T) {
	want := []string{
		"CREATE USER gateway WITH PASSWORD 'gateway-local';",
		"CREATE DATABASE gateway OWNER gateway;",
		"REVOKE ALL ON DATABASE gateway FROM PUBLIC;",
		"GRANT CONNECT ON DATABASE gateway TO gateway;",
	}
	got := lines(t, "../deploy/postgres-init.sql")
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("init script:\n%s", strings.Join(got, "\n"))
	}
}
