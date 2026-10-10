package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/replay"
)

func installedClient(t *testing.T) *authorize.Client {
	t.Helper()
	base := os.Getenv("SWIPELINE_TEST_GATEWAY")
	if base == "" {
		t.Skip("run make m2-demo")
	}
	return &authorize.Client{Base: base, HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func installedConfig(id string, count int64, tps float64) replay.Config {
	return replay.Config{Generator: generator.Config{RunID: id, Seed: 42, Day: "2026-10-08", Events: count}, TPS: tps, Workers: 1, Queue: 128}
}

func installedFinals(t *testing.T, c *authorize.Client, cfg replay.Config) []auth.Final {
	t.Helper()
	g, err := generator.New(cfg.Generator)
	if err != nil {
		t.Fatal(err)
	}
	result := make([]auth.Final, cfg.Generator.Events)
	for i := range result {
		e, err := g.At(int64(i), 1)
		if err != nil {
			t.Fatal(err)
		}
		id := contract.ID(e.TransactionID, authorize.Rules)
		response, err := c.HTTP.Get(c.Base + "/v1/authorizations/" + id)
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewDecoder(response.Body).Decode(&result[i])
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || result[i].TransactionID != id || result[i].DecisionID == "" {
			t.Fatalf("ordinal %d has no final: HTTP %d %v", i, response.StatusCode, err)
		}
	}
	return result
}

func enforceBudgets() bool { return os.Getenv("SWIPELINE_ENFORCE_BUDGETS") != "0" }

func TestInstalledAuthorization(t *testing.T) {
	t.Logf("budgets enforced=%t cpus=%d", enforceBudgets(), runtime.NumCPU())
	c := installedClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	warm := installedConfig("warm-"+fmt.Sprint(time.Now().UnixNano()), 300, 50)
	if _, err := authorize.Run(ctx, warm, c, "latency", 128); err != nil {
		t.Fatal(err)
	}
	cfg := installedConfig("installed-42", 1000, 50)
	summary, err := authorize.Run(ctx, cfg, c, "latency", 128)
	if err != nil {
		t.Fatal(err)
	}
	finals := installedFinals(t, c, cfg)
	assertInstalledDecisions(t, summary, finals)
	if _, err = authorize.Run(ctx, cfg, c, "latency", 128); err != nil {
		t.Fatal(err)
	}
	if second := installedFinals(t, c, cfg); !reflect.DeepEqual(finals, second) {
		t.Fatal("replay changed stored final outcomes")
	}
	t.Logf("completed=%d approvals=%d declines=%d stand-ins=%d share=%.3f%%", summary.Completed, summary.Approved, summary.Declined, summary.StandIn, 100*float64(summary.StandIn)/float64(summary.Completed))
}

func assertInstalledDecisions(t *testing.T, summary authorize.Summary, finals []auth.Final) {
	t.Helper()
	if summary.Completed != 1000 || summary.Offered != 1000 || len(finals) != 1000 {
		t.Fatalf("incomplete replay: %+v", summary)
	}
	if enforceBudgets() && summary.StandIn >= 10 {
		t.Fatalf("stand-in budget exceeded: %d/1000", summary.StandIn)
	}
	var approved, declined, burst int
	for ordinal, f := range finals {
		if f.Source == "STAND_IN" {
			continue
		}
		if f.Source != "FLINK" {
			t.Fatalf("ordinal %d: unexpected source %q", ordinal, f.Source)
		}
		if f.Decision == "APPROVE" {
			approved++
		} else if f.Decision == "DECLINE" {
			declined++
		} else {
			t.Fatalf("invalid decision: %q", f.Decision)
		}
		// synthetic-v2 repeats card 0 at 880..900, then adds one card per
		// ordinal. For installed-42, the first 50 cards occupy 50 HLL registers:
		// 4096*ln(4096/4046)=50.3077 at ordinal 949. All burst amounts are 100,
		// and the 19.8 s burst remains within Merchant's six 10 s buckets.
		// Rules requires estimate >=50 and >=80% small attempts.
		if ordinal >= 949 && ordinal < 980 {
			burst++
			if f.Decision != "DECLINE" || f.Reason != "CARD_TESTING" {
				t.Errorf("burst ordinal %d: %s %s (distinct=%v)", ordinal, f.Decision, f.Reason, f.Distinct)
			}
		}
	}
	if approved == 0 || declined == 0 || burst == 0 {
		t.Fatalf("missing scored outcomes: approvals=%d declines=%d burst=%d", approved, declined, burst)
	}
}

// Run the assertion in a child test process so its fatal rejection is observable.
func TestInstalledDecisionsRejectOnePercent(t *testing.T) {
	t.Setenv("SWIPELINE_ENFORCE_BUDGETS", "1")
	if os.Getenv("SWIPELINE_TEST_ONE_PERCENT") == "1" {
		finals := make([]auth.Final, 1000)
		for i := range finals {
			finals[i] = auth.Final{Source: "FLINK", Decision: "APPROVE"}
			if i >= 949 && i < 980 {
				finals[i].Decision = "DECLINE"
				finals[i].Reason = "CARD_TESTING"
			}
			if i < 10 {
				finals[i].Source = "STAND_IN"
			}
		}
		assertInstalledDecisions(t, authorize.Summary{Completed: 1000, Offered: 1000, StandIn: 10}, finals)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestInstalledDecisionsRejectOnePercent$")
	cmd.Env = append(os.Environ(), "SWIPELINE_TEST_ONE_PERCENT=1")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "stand-in budget exceeded: 10/1000") {
		t.Fatalf("one-percent share was not rejected: %v\n%s", err, output)
	}
}

func TestInstalledDecisionsBudgetSwitch(t *testing.T) {
	if mode := os.Getenv("SWIPELINE_TEST_BUDGET_CASE"); mode != "" {
		finals := make([]auth.Final, 1000)
		for i := range finals {
			finals[i] = auth.Final{Source: "FLINK", Decision: "APPROVE"}
			if i >= 949 && i < 980 {
				finals[i].Decision = "DECLINE"
				finals[i].Reason = "CARD_TESTING"
			}
			if i < 82 {
				finals[i].Source = "STAND_IN"
				finals[i].Decision = "DECLINE"
			}
		}
		if mode == "defect" {
			finals[960].Decision = "APPROVE"
		}
		assertInstalledDecisions(t, authorize.Summary{Completed: 1000, Offered: 1000, StandIn: 82}, finals)
		return
	}
	for _, tc := range []struct{ name, enforce, want string }{
		{"default", "", "stand-in budget exceeded: 82/1000"},
		{"enforced", "1", "stand-in budget exceeded: 82/1000"},
		{"report", "0", ""},
		{"defect", "0", "burst ordinal 960"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWIPELINE_ENFORCE_BUDGETS", tc.enforce)
			t.Setenv("SWIPELINE_TEST_BUDGET_CASE", tc.name)
			cmd := exec.Command(os.Args[0], "-test.run=^TestInstalledDecisionsBudgetSwitch$")
			output, err := cmd.CombinedOutput()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("report mode rejected valid behavior: %v\n%s", err, output)
				}
			} else if err == nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("expected %q rejection: %v\n%s", tc.want, err, output)
			}
		})
	}
}
