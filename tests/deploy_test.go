package tests

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRenderedComposeBudgetsAndScope(t *testing.T) {
	path := os.Getenv("SWIPELINE_COMPOSE_JSON")
	if path == "" {
		t.Skip("run make deploy-check")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Image    string   `json:"image"`
			MemLimit int64    `json:"mem_limit,string"`
			Profiles []string `json:"profiles"`
			Ports    []struct {
				HostIP string `json:"host_ip"`
			} `json:"ports"`
			Privileged bool `json:"privileged"`
		} `json:"services"`
	}
	if err = json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Services) != 2 {
		t.Fatalf("unused services: %v", doc.Services)
	}
	for _, name := range []string{"replayer", "prometheus"} {
		service, ok := doc.Services[name]
		if !ok {
			t.Fatal("missing", name)
		}
		if service.MemLimit != 256*1024*1024 || service.Privileged {
			t.Fatal("budget/privilege regression", name)
		}
		if len(service.Profiles) != 1 || service.Profiles[0] != "core" {
			t.Fatal("profile regression", name)
		}
		for _, port := range service.Ports {
			if port.HostIP != "127.0.0.1" {
				t.Fatal("public port", name)
			}
		}
	}
	if doc.Services["prometheus"].Image != "prom/prometheus:v3.15.0" {
		t.Fatal("pin changed")
	}
}

// Fedora hosts can run Docker with SELinux enforcement, where an unlabelled
// bind mount of a file from the checkout is denied. The shared z label is a
// no-op on hosts without SELinux.
func TestRenderedPrometheusConfigIsSELinuxShared(t *testing.T) {
	path := os.Getenv("SWIPELINE_COMPOSE_JSON")
	if path == "" {
		t.Skip("run make deploy-check")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Configs []any `json:"configs"`
			Volumes []struct {
				Type     string `json:"type"`
				Target   string `json:"target"`
				ReadOnly bool   `json:"read_only"`
				Bind     struct {
					SELinux string `json:"selinux"`
				} `json:"bind"`
			} `json:"volumes"`
		} `json:"services"`
	}
	if err = json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	prometheus := doc.Services["prometheus"]
	if len(prometheus.Configs) != 0 {
		t.Fatal("configs entries cannot carry an SELinux label")
	}
	for _, v := range prometheus.Volumes {
		if v.Target == "/etc/prometheus/prometheus.yml" {
			if v.Type != "bind" || !v.ReadOnly || v.Bind.SELinux != "z" {
				t.Fatalf("config mount must be a read-only bind with z label: %+v", v)
			}
			return
		}
	}
	t.Fatal("prometheus configuration is not mounted")
}
