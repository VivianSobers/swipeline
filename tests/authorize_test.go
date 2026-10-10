package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
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

func TestInstalledAuthorization(t *testing.T) {
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
	if summary.StandIn > 10 {
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
