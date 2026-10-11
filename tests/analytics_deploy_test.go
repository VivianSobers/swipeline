package tests

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestRenderedPostgresInitCreatesAnalyticsDatabases(t *testing.T) {
	path := "deploy/postgres-analytics-init.sql"
	if _, err := os.Stat(path); err != nil {
		path = "../deploy/postgres-analytics-init.sql"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)

	for _, want := range []string{
		"CREATE USER druid WITH PASSWORD 'druid-local';",
		"CREATE DATABASE druid OWNER druid;",
		"REVOKE ALL ON DATABASE druid FROM PUBLIC;",
		"GRANT CONNECT ON DATABASE druid TO druid;",
		"CREATE USER superset WITH PASSWORD 'superset-local';",
		"CREATE DATABASE superset OWNER superset;",
		"REVOKE ALL ON DATABASE superset FROM PUBLIC;",
		"GRANT CONNECT ON DATABASE superset TO superset;",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("postgres-analytics-init.sql missing: %q", want)
		}
	}

	for _, forbidden := range []string{
		"GRANT ALL ON DATABASE gateway TO druid",
		"GRANT CONNECT ON DATABASE gateway TO druid",
		"GRANT ALL ON DATABASE gateway TO superset",
		"GRANT CONNECT ON DATABASE gateway TO superset",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("postgres-analytics-init.sql grants gateway privileges to analytics users: %q", forbidden)
		}
	}

	services := rendered(t)
	pg, ok := services["postgres"]
	if !ok {
		t.Fatal("missing postgres service")
	}
	mounted := false
	for _, v := range pg.Volumes {
		if v.Target == "/docker-entrypoint-initdb.d/002-analytics.sql" && strings.HasSuffix(v.Source, "/deploy/postgres-analytics-init.sql") {
			mounted = true
			if !v.ReadOnly || v.Bind.SELinux != "z" {
				t.Fatalf("postgres analytics init mount must be read-only and selinux z: %+v", v)
			}
			break
		}
	}
	if !mounted {
		t.Fatal("postgres does not mount /docker-entrypoint-initdb.d/002-analytics.sql")
	}
}

func renderedFull(t *testing.T) map[string]service {
	t.Helper()
	path := os.Getenv("SWIPELINE_FULL_COMPOSE_JSON")
	if path == "" {
		return rendered(t)
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

func TestRenderedAnalyticsProfileTopology(t *testing.T) {
	profile := os.Getenv("COMPOSE_PROFILES")

	druidServices := map[string]struct {
		image    string
		mib      int64
		hostPort bool
	}{
		"druid-zk":                   {"zookeeper:3.9.6", 256, false},
		"druid-coordinator-overlord": {"apache/druid:38.0.0", 512, false},
		"druid-broker":               {"apache/druid:38.0.0", 512, false},
		"druid-router":               {"apache/druid:38.0.0", 256, true},
		"druid-historical":           {"apache/druid:38.0.0", 1024, false},
		"druid-middlemanager":        {"apache/druid:38.0.0", 1792, false},
	}

	if profile != "full" {
		services := rendered(t)
		for name := range druidServices {
			if _, ok := services[name]; ok {
				t.Fatalf("service %s rendered in default profile", name)
			}
		}
		return
	}

	services := renderedFull(t)

	expectedMissing := map[string]bool{
		"druid-coordinator-overlord": true,
		"druid-broker":               true,
		"druid-router":               true,
		"druid-historical":           true,
		"druid-middlemanager":        true,
	}

	for name, want := range druidServices {
		s, ok := services[name]
		if !ok {
			if expectedMissing[name] {
				continue
			}
			t.Fatalf("missing service in full profile: %s", name)
		}
		if expectedMissing[name] {
			t.Fatalf("service %s exists but was marked expectedMissing", name)
		}
		if s.Image != want.image {
			t.Fatalf("%s pin changed: %s (want %s)", name, s.Image, want.image)
		}
		if s.MemLimit != want.mib<<20 || s.Privileged {
			t.Fatalf("%s budget/privilege regression: limit=%d privileged=%v", name, s.MemLimit, s.Privileged)
		}
		if len(s.Profiles) != 1 || s.Profiles[0] != "full" {
			t.Fatalf("%s profile regression: %v", name, s.Profiles)
		}
		if !want.hostPort && len(s.Ports) != 0 {
			t.Fatalf("internal service published to host: %s", name)
		}
		for _, port := range s.Ports {
			if port.HostIP != "127.0.0.1" {
				t.Fatalf("%s public port must bind 127.0.0.1: %+v", name, port)
			}
		}
	}
}
