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

	expectedMissing := map[string]bool{}

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

func TestCanonicalSupervisorSpec(t *testing.T) {
	path := "deploy/druid/supervisor.json"
	if _, err := os.Stat(path); err != nil {
		path = "../deploy/druid/supervisor.json"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var doc struct {
		Type string `json:"type"`
		Spec struct {
			DataSchema struct {
				DataSource    string `json:"dataSource"`
				TimestampSpec struct {
					Column string `json:"column"`
					Format string `json:"format"`
				} `json:"timestampSpec"`
				DimensionsSpec struct {
					Dimensions []any `json:"dimensions"`
				} `json:"dimensionsSpec"`
				GranularitySpec struct {
					Rollup bool `json:"rollup"`
				} `json:"granularitySpec"`
			} `json:"dataSchema"`
			IoConfig struct {
				Topic              string            `json:"topic"`
				UseEarliestOffset  bool              `json:"useEarliestOffset"`
				TaskCount          int               `json:"taskCount"`
				Replicas           int               `json:"replicas"`
				ConsumerProperties map[string]string `json:"consumerProperties"`
			} `json:"ioConfig"`
			TuningConfig struct {
				MaxParseExceptions int `json:"maxParseExceptions"`
			} `json:"tuningConfig"`
		} `json:"spec"`
	}

	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("failed to parse supervisor.json: %v", err)
	}

	if doc.Spec.IoConfig.Topic != "swipeline.decisions.v1" {
		t.Errorf("expected topic swipeline.decisions.v1, got %q", doc.Spec.IoConfig.Topic)
	}
	if doc.Spec.IoConfig.ConsumerProperties["isolation.level"] != "read_committed" {
		t.Errorf("expected isolation.level read_committed, got %q", doc.Spec.IoConfig.ConsumerProperties["isolation.level"])
	}
	if !doc.Spec.IoConfig.UseEarliestOffset {
		t.Errorf("expected useEarliestOffset true")
	}
	if doc.Spec.IoConfig.TaskCount != 1 {
		t.Errorf("expected taskCount 1, got %d", doc.Spec.IoConfig.TaskCount)
	}
	if doc.Spec.IoConfig.Replicas != 1 {
		t.Errorf("expected replicas 1, got %d", doc.Spec.IoConfig.Replicas)
	}
	if doc.Spec.DataSchema.GranularitySpec.Rollup {
		t.Errorf("expected rollup false")
	}
	if doc.Spec.TuningConfig.MaxParseExceptions != 0 {
		t.Errorf("expected maxParseExceptions 0, got %d", doc.Spec.TuningConfig.MaxParseExceptions)
	}
	if doc.Spec.DataSchema.TimestampSpec.Column != "finalized_at_ms" || doc.Spec.DataSchema.TimestampSpec.Format != "millis" {
		t.Errorf("expected timestamp finalized_at_ms millis, got %+v", doc.Spec.DataSchema.TimestampSpec)
	}

	dims := doc.Spec.DataSchema.DimensionsSpec.Dimensions
	for _, d := range dims {
		switch v := d.(type) {
		case string:
			if v == "card_token" {
				t.Errorf("prohibited dimension card_token found")
			}
		case map[string]any:
			if v["name"] == "card_token" {
				t.Errorf("prohibited dimension card_token found")
			}
		}
	}

	expectedDimensions := []any{
		"run_id", "transaction_id", "decision_id", "merchant_id", "mcc",
		"issuer_bank_id", "acquirer_bank_id", "currency", "decision", "reason",
		"decision_source", "rule_version", "reference_version", "fraud_flags",
		"features_late",
		map[string]any{"name": "amount_minor", "type": "long"},
		map[string]any{"name": "event_time_ms", "type": "long"},
		map[string]any{"name": "decision_latency_ms", "type": "double"},
		map[string]any{"name": "score", "type": "long"},
		map[string]any{"name": "velocity_60s", "type": "long"},
		map[string]any{"name": "spend_ratio", "type": "double"},
		map[string]any{"name": "merchant_distinct_estimate", "type": "double"},
	}

	if len(dims) != len(expectedDimensions) {
		t.Fatalf("expected %d dimensions, got %d", len(expectedDimensions), len(dims))
	}
	for i, want := range expectedDimensions {
		got := dims[i]
		switch w := want.(type) {
		case string:
			if got != w {
				t.Errorf("dimension %d: want %v, got %v", i, w, got)
			}
		case map[string]any:
			gm, ok := got.(map[string]any)
			if !ok {
				t.Errorf("dimension %d: want %v, got %v", i, w, got)
				continue
			}
			if gm["name"] != w["name"] || gm["type"] != w["type"] {
				t.Errorf("dimension %d: want %v, got %v", i, w, gm)
			}
		}
	}
}

func TestRenderedAnalyticsBootstrap(t *testing.T) {
	profile := os.Getenv("COMPOSE_PROFILES")
	if profile != "full" {
		services := rendered(t)
		if _, ok := services["analytics-bootstrap"]; ok {
			t.Fatal("analytics-bootstrap rendered in default profile")
		}
		return
	}

	services := renderedFull(t)
	s, ok := services["analytics-bootstrap"]
	if !ok {
		t.Fatal("missing analytics-bootstrap in full profile")
	}

	if s.Image != "swipeline-replayer:m1" {
		t.Fatalf("analytics-bootstrap pin changed: %s", s.Image)
	}
	if s.MemLimit != 128<<20 {
		t.Fatalf("analytics-bootstrap memory limit: got %d, want %d", s.MemLimit, 128<<20)
	}
	if len(s.Profiles) != 1 || s.Profiles[0] != "full" {
		t.Fatalf("analytics-bootstrap profile regression: %v", s.Profiles)
	}

	for dep, want := range map[string]string{
		"druid-router":               "service_healthy",
		"druid-coordinator-overlord": "service_healthy",
		"kafka":                      "service_healthy",
	} {
		cond, ok := s.DependsOn[dep]
		if !ok || cond.Condition != want {
			t.Fatalf("analytics-bootstrap missing dependency %s with condition %s: %+v", dep, want, cond)
		}
	}

	mounted := false
	for _, v := range s.Volumes {
		if v.Target == "/etc/swipeline/supervisor.json" && strings.HasSuffix(v.Source, "/deploy/druid/supervisor.json") {
			mounted = true
			if !v.ReadOnly || v.Bind.SELinux != "z" {
				t.Fatalf("supervisor.json mount must be read-only with selinux z: %+v", v)
			}
			break
		}
	}
	if !mounted {
		t.Fatal("analytics-bootstrap does not mount /etc/swipeline/supervisor.json")
	}
}

func TestRenderedSupersetService(t *testing.T) {
	profile := os.Getenv("COMPOSE_PROFILES")
	if profile != "full" {
		services := rendered(t)
		if _, ok := services["superset"]; ok {
			t.Fatal("superset rendered in default profile")
		}
		return
	}

	services := renderedFull(t)
	s, ok := services["superset"]
	if !ok {
		t.Fatal("missing superset service in full profile")
	}

	if s.Image != "swipeline-superset:m3" {
		t.Fatalf("superset pin changed: %s", s.Image)
	}
	if s.MemLimit != 768<<20 {
		t.Fatalf("superset memory limit: got %d, want %d", s.MemLimit, 768<<20)
	}
	if len(s.Profiles) != 1 || s.Profiles[0] != "full" {
		t.Fatalf("superset profile regression: %v", s.Profiles)
	}
	if len(s.Ports) != 1 || s.Ports[0].HostIP != "127.0.0.1" {
		t.Fatalf("superset public port must bind 127.0.0.1: %+v", s.Ports)
	}

	for dep, want := range map[string]string{
		"postgres":            "service_healthy",
		"analytics-bootstrap": "service_completed_successfully",
	} {
		cond, ok := s.DependsOn[dep]
		if !ok || cond.Condition != want {
			t.Fatalf("superset missing dependency %s with condition %s: %+v", dep, want, cond)
		}
	}

	hasSecrets := false
	for _, v := range s.Volumes {
		if v.Target == "/secrets" && (v.Source == "superset-secrets" || strings.HasSuffix(v.Source, "superset-secrets")) {
			hasSecrets = true
			break
		}
	}
	if !hasSecrets {
		t.Fatalf("superset does not mount superset-secrets to /secrets: %+v", s.Volumes)
	}

	if len(s.Healthcheck.Test) == 0 {
		t.Fatal("superset missing healthcheck")
	}
	healthCmd := strings.Join(s.Healthcheck.Test, " ")
	if !strings.Contains(healthCmd, "http://127.0.0.1:8088/health") {
		t.Fatalf("superset healthcheck test: %s", healthCmd)
	}
}

func TestCIWorkflowFullProfile(t *testing.T) {
	path := ".github/workflows/ci.yml"
	if _, err := os.Stat(path); err != nil {
		path = "../.github/workflows/ci.yml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)

	for _, want := range []string{
		"COMPOSE_PROFILES=full make deploy-check",
		"superset",
		"runner capacity",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("ci.yml missing %q", want)
		}
	}
}

func TestReadmeWatchTheNetwork(t *testing.T) {
	path := "README.md"
	if _, err := os.Stat(path); err != nil {
		path = "../README.md"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)

	for _, want := range []string{
		"## Watch the network",
		"preflight",
		"COMPOSE_PROFILES=full",
		"make superset-password",
		"http://localhost:8088",
		"payments-operations",
		"swipeline reconcile",
		"the four-core reference server",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("README.md missing %q", want)
		}
	}
}
