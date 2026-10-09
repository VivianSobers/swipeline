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

// A single combined broker/controller that never creates topics on demand,
// so a typo in a producer cannot fork the fixed four-partition topology.
func TestKafkaBrokerConfig(t *testing.T) {
	props := map[string]string{}
	for _, line := range lines(t, "../deploy/kafka.properties") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("malformed line %q", line)
		}
		if _, dup := props[key]; dup {
			t.Fatalf("duplicate %s", key)
		}
		props[key] = value
	}
	for key, want := range map[string]string{
		"process.roles":                    "broker,controller",
		"controller.quorum.voters":         "1@kafka:9093",
		"advertised.listeners":             "PLAINTEXT://kafka:9092",
		"log.dirs":                         "/var/lib/kafka/data",
		"auto.create.topics.enable":        "false",
		"num.partitions":                   "4",
		"default.replication.factor":       "1",
		"offsets.topic.replication.factor": "1",
		"transaction.state.log.min.isr":    "1",
		"log.retention.hours":              "24",
	} {
		if props[key] != want {
			t.Errorf("%s=%q, want %q", key, props[key], want)
		}
	}
}
