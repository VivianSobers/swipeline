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

// Six application topics, fixed from first install: 24-hour retention, and
// 72 hours for dead letters.
func TestTopicTopology(t *testing.T) {
	got := map[string]string{}
	for _, line := range lines(t, "../deploy/topics.sh") {
		if f := strings.Fields(line); len(f) == 3 && f[0] == "topic" {
			got[f[1]] = f[2]
		}
	}
	want := map[string]string{
		"auth.requests": "86400000", "auth.proposals": "86400000", "decisions.raw": "86400000",
		"decisions": "86400000", "lifecycle": "86400000", "dlq": "259200000",
	}
	if len(got) != len(want) {
		t.Fatalf("topics: %v", got)
	}
	for name, retention := range want {
		if got[name] != retention {
			t.Errorf("%s retention %q, want %s", name, got[name], retention)
		}
	}
}

func TestPrometheusScrapesGateway(t *testing.T) {
	jobs := map[string]string{}
	var job string
	for _, line := range lines(t, "../deploy/prometheus.yml") {
		if name, ok := strings.CutPrefix(line, "- job_name: "); ok {
			job = name
		}
		if targets, ok := strings.CutPrefix(line, "- targets: "); ok {
			jobs[job] = targets
		}
	}
	if jobs["gateway"] != "[gateway:8081]" {
		t.Fatalf("scrape jobs: %v", jobs)
	}
}

// flatten reads the nested-map YAML subset used by deploy/flink-config.yaml
// into dotted keys.
func flatten(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	var stack []string
	for _, raw := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(raw) == "" || strings.HasPrefix(strings.TrimSpace(raw), "#") {
			continue
		}
		depth := (len(raw) - len(strings.TrimLeft(raw, " "))) / 2
		key, value, _ := strings.Cut(strings.TrimSpace(raw), ":")
		stack = append(stack[:depth], key)
		if value = strings.TrimSpace(value); value != "" {
			out[strings.Join(stack, ".")] = value
		}
	}
	return out
}

// Flink runs with zero network buffering (each record is sent at once, so a
// sparse request is not held for a batch) and leaves headroom under each
// container limit for memory the JVM does not account for exactly.
func TestFlinkProcessConfig(t *testing.T) {
	got := flatten(t, "../deploy/flink-config.yaml")
	for key, want := range map[string]string{
		"execution.buffer-timeout.enabled":     "true",
		"execution.buffer-timeout.interval":    "0 ms",
		"execution.checkpointing.interval":     "2 s",
		"jobmanager.memory.process.size":       "448m",
		"jobmanager.memory.jvm-metaspace.size": "128m",
		"jobmanager.memory.jvm-overhead.min":   "64m",
		"taskmanager.memory.process.size":      "1408m",
		"taskmanager.numberOfTaskSlots":        "3",
		"state.backend.type":                   "rocksdb",
		"state.backend.incremental":            "true",
		"state.checkpoints.dir":                "file:///state/checkpoints",
		"pipeline.max-parallelism":             "128",
	} {
		if got[key] != want {
			t.Errorf("%s=%q, want %q", key, got[key], want)
		}
	}
}

// The image's default configuration carries the JVM module options Flink
// needs on Java 17; replacing the file must keep them.
func TestFlinkKeepsJavaModuleOptions(t *testing.T) {
	opts := flatten(t, "../deploy/flink-config.yaml")["env.java.opts.all"]
	for _, flag := range []string{"--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED"} {
		if !strings.Contains(opts, flag) {
			t.Errorf("missing %s", flag)
		}
	}
}
