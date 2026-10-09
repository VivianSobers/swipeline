package tests

import (
	"encoding/json"
	"os"
	"testing"
)

type service struct {
	Image      string   `json:"image"`
	MemLimit   int64    `json:"mem_limit,string"`
	Profiles   []string `json:"profiles"`
	Privileged bool     `json:"privileged"`
	Configs    []any    `json:"configs"`
	Ports      []struct {
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
		"replayer":   {"swipeline-replayer:m1", 256, true},
		"prometheus": {"prom/prometheus:v3.15.0", 256, true},
		"postgres":   {"postgres:17.11-bookworm", 512, false},
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
