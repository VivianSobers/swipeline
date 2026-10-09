package tests

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type service struct {
	Image       string   `json:"image"`
	MemLimit    int64    `json:"mem_limit,string"`
	Profiles    []string `json:"profiles"`
	Privileged  bool     `json:"privileged"`
	Configs     []any    `json:"configs"`
	Tmpfs       []string `json:"tmpfs"`
	Healthcheck struct {
		Test        []string `json:"test"`
		Interval    string   `json:"interval"`
		StartPeriod string   `json:"start_period"`
	} `json:"healthcheck"`
	DependsOn map[string]struct {
		Condition string `json:"condition"`
	} `json:"depends_on"`
	Ports []struct {
		HostIP string `json:"host_ip"`
	} `json:"ports"`
	Volumes []struct {
		Type     string `json:"type"`
		Source   string `json:"source"`
		Target   string `json:"target"`
		ReadOnly bool   `json:"read_only"`
		Bind     struct {
			SELinux string `json:"selinux"`
		} `json:"bind"`
	} `json:"volumes"`
}

func rendered(t *testing.T) map[string]service {
	t.Helper()
	path := os.Getenv("SWIPELINE_COMPOSE_JSON")
	if path == "" {
		t.Skip("run make deploy-check")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]service `json:"services"`
	}
	if err = json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Services
}

func TestRenderedComposeBudgetsAndScope(t *testing.T) {
	services := rendered(t)
	expected := map[string]struct {
		image    string
		mib      int64
		hostPort bool
	}{
		"replayer":          {"swipeline-replayer:m1", 256, true},
		"prometheus":        {"prom/prometheus:v3.15.0", 256, true},
		"postgres":          {"postgres:17.11-bookworm", 512, false},
		"kafka":             {"apache/kafka:4.3.1", 768, false},
		"gateway":           {"swipeline-gateway:m2", 256, true},
		"flink-jobmanager":  {"swipeline-streaming:m2", 512, true},
		"flink-taskmanager": {"swipeline-streaming:m2", 1536, false},
	}
	if len(services) != len(expected) {
		t.Fatalf("unexpected services: %v", services)
	}
	for name, want := range expected {
		s, ok := services[name]
		if !ok {
			t.Fatal("missing", name)
		}
		if s.Image != want.image {
			t.Fatalf("%s pin changed: %s", name, s.Image)
		}
		if s.MemLimit != want.mib<<20 || s.Privileged {
			t.Fatal("budget/privilege regression", name)
		}
		if len(s.Profiles) != 2 || s.Profiles[0] != "core" || s.Profiles[1] != "full" {
			t.Fatal("profile regression", name, s.Profiles)
		}
		if !want.hostPort && len(s.Ports) != 0 {
			t.Fatal("internal service published to host", name)
		}
		for _, port := range s.Ports {
			if port.HostIP != "127.0.0.1" {
				t.Fatal("public port", name)
			}
		}
	}
}

// Fedora hosts can run Docker with SELinux enforcement, where an unlabelled
// bind mount of a file from the checkout is denied. The shared z label is a
// no-op on hosts without SELinux, and configs entries cannot carry it.
func TestRenderedBindMountsAreReadOnlyAndSELinuxShared(t *testing.T) {
	binds := 0
	for name, s := range rendered(t) {
		if len(s.Configs) != 0 {
			t.Fatal("configs entries cannot carry an SELinux label", name)
		}
		for _, v := range s.Volumes {
			if v.Type != "bind" {
				continue
			}
			binds++
			if !v.ReadOnly || v.Bind.SELinux != "z" {
				t.Fatalf("%s: mount must be a read-only bind with z label: %+v", name, v)
			}
		}
	}
	if binds == 0 {
		t.Fatal("no configuration is mounted")
	}
}

// Kafka is healthy only once the six topics exist and validate, so services
// that depend on it never start against a broker without its topology. The
// check itself starts no JVM: it reads a marker the bootstrap leaves behind.
func TestRenderedKafkaHealthRequiresTopics(t *testing.T) {
	test := rendered(t)["kafka"].Healthcheck.Test
	if len(test) != 2 || test[0] != "CMD-SHELL" || !strings.Contains(test[1], "test -f /tmp/topics-ready") || strings.Contains(test[1], "kafka-topics.sh") {
		t.Fatalf("healthcheck: %q", test)
	}
}

func TestRenderedKafkaMountsTopicBootstrap(t *testing.T) {
	for _, v := range rendered(t)["kafka"].Volumes {
		if v.Target == "/opt/swipeline/topics.sh" && strings.HasSuffix(v.Source, "/deploy/topics.sh") {
			return
		}
	}
	t.Fatal("topic bootstrap is not mounted")
}

// The broker loads compression codecs (snappy, zstd) as native libraries it
// extracts to /tmp; Docker mounts tmpfs noexec unless told otherwise.
func TestRenderedKafkaTmpAllowsNativeCodecs(t *testing.T) {
	for _, mount := range rendered(t)["kafka"].Tmpfs {
		if strings.HasPrefix(mount, "/tmp:") && strings.Contains(mount, ",exec") {
			return
		}
	}
	t.Fatal("kafka /tmp must allow exec for native codecs")
}

// Run the rendered health command against REST responses, including a running
// job whose tasks have not finished deploying.
func TestRenderedAuthorizationReadiness(t *testing.T) {
	services := rendered(t)
	h := services["flink-jobmanager"].Healthcheck
	if len(h.Test) != 2 || h.Test[0] != "CMD-SHELL" {
		t.Fatalf("missing authorization healthcheck: %q", h.Test)
	}
	if h.Interval != "2s" || h.StartPeriod != "2m0s" {
		t.Fatalf("health timing: %+v", h)
	}
	if services["flink-taskmanager"].DependsOn["flink-jobmanager"].Condition != "service_started" {
		t.Fatal("taskmanager must start before the job is healthy")
	}
	dir := t.TempDir()
	curl := "#!/bin/sh\n[ \"$*\" = \"-fsS --max-time 2 http://127.0.0.1:8081/jobs/overview\" ] || exit 2\n[ \"$REST_FAIL\" != 1 ] || exit 1\nprintf '%s\\n' \"$REST_BODY\"\n"
	if err := os.WriteFile(filepath.Join(dir, "curl"), []byte(curl), 0755); err != nil {
		t.Fatal(err)
	}
	ready := `{"jid":"abc","name":"authorization","state":"RUNNING","tasks":{"total":6,"created":0,"scheduled":0,"deploying":0,"initializing":0,"reconciling":0,"running":6}}`
	cases := []struct {
		name, body    string
		fail, healthy bool
	}{
		{"ready", `{"jobs":[` + ready + `]}`, false, true},
		{"empty", `{"jobs":[]}`, false, false},
		{"unavailable", "", true, false},
		{"wrong job", `{"jobs":[` + strings.ReplaceAll(ready, "authorization", "other") + `]}`, false, false},
		{"starting", `{"jobs":[` + strings.ReplaceAll(ready, "RUNNING", "INITIALIZING") + `]}`, false, false},
		{"unrelated running", `{"jobs":[` + strings.ReplaceAll(ready, "RUNNING", "RESTARTING") + "," + strings.ReplaceAll(ready, "authorization", "other") + `]}`, false, false},
	}
	for _, state := range []string{"created", "scheduled", "deploying", "initializing", "reconciling"} {
		cases = append(cases, struct {
			name, body    string
			fail, healthy bool
		}{state, `{"jobs":[` + strings.ReplaceAll(ready, fmt.Sprintf("%q:0", state), fmt.Sprintf("%q:1", state)) + `]}`, false, false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Compose preserves dollar escaping in config output and unescapes it at container creation.
			cmd := exec.Command("sh", "-c", strings.ReplaceAll(h.Test[1], "$$", "$"))
			fail := "0"
			if tc.fail {
				fail = "1"
			}
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "REST_BODY="+tc.body, "REST_FAIL="+fail)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.healthy {
				t.Fatalf("healthy=%v want %v: %s %v", err == nil, tc.healthy, out, err)
			}
		})
	}
}
