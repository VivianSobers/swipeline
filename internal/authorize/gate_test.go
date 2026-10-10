package authorize

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func gateScrape(fast, within, slow, stand, base int) string {
	return fmt.Sprintf(`swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.025"} %d
swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.05"} %d
swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.1"} %d
swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="+Inf"} %d
swipeline_gateway_final_decisions_total{decision_source="FLINK",decision="APPROVE"} %d
swipeline_gateway_final_decisions_total{decision_source="STAND_IN",decision="DECLINE"} %d
`, base+fast, base+within, base+slow, base+1000, base+1000-stand, base+stand)
}

func TestGateBudgetDeltas(t *testing.T) {
	before := gateScrape(0, 0, 0, 0, 500)
	// The initial final counters must represent the baseline only.
	before = strings.Replace(before, "} 1500", "} 500", -1)
	for _, tc := range []struct {
		name                      string
		fast, within, slow, stand int
		pass                      bool
		p99                       float64
	}{
		{"within budget", 980, 1000, 1000, 5, true, 0.0375},
		{"slow", 0, 0, 990, 0, false, 0.1},
		{"stand ins", 980, 1000, 1000, 50, false, 0.0375},
		{"one percent", 980, 1000, 1000, 10, false, 0.0375},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Gate(before, gateScrape(tc.fast, tc.within, tc.slow, tc.stand, 500), "latency", 50*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			if got.Pass != tc.pass || got.Samples != 1000 || math.Abs(got.P99.Seconds()-tc.p99) > 1e-8 || math.Abs(got.StandInShare-float64(tc.stand)/1000) > 1e-8 {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestGateRejectsUnusableMeasurements(t *testing.T) {
	good := gateScrape(980, 1000, 1000, 5, 500)
	for _, tc := range []struct{ name, before, after string }{
		{"missing", "", ""},
		{"no traffic", good, good},
		{"reset", good, gateScrape(980, 1000, 1000, 5, 0)},
		{"malformed", "", strings.ReplaceAll(good, "} 1480", "} NaN")},
		{"missing infinity", "", strings.ReplaceAll(good, "le=\"+Inf\"", "le=\"1\"")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if v, err := Gate(tc.before, tc.after, "latency", 50*time.Millisecond); err == nil || v.Pass {
				t.Fatalf("accepted invalid measurement: %+v %v", v, err)
			}
		})
	}
}
