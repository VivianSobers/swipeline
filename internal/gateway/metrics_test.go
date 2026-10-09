package gateway

import (
	"strings"
	"testing"
)

func TestHistogramExposition(t *testing.T) {
	h := newHistogram(0.05, 0.1)
	for _, v := range []float64{0.0001, 0.05, 0.07, 0.1, 2} {
		h.observe(v)
	}
	var b strings.Builder
	h.write(&b, "swipeline_gateway_budget_duration_seconds", `deadline_mode="latency"`)
	want := `swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.05"} 2
swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.1"} 4
swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="+Inf"} 5
swipeline_gateway_budget_duration_seconds_sum{deadline_mode="latency"} 2.2201
swipeline_gateway_budget_duration_seconds_count{deadline_mode="latency"} 5
`
	if b.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", b.String(), want)
	}
}

func TestEmptyHistogramExposesZeroes(t *testing.T) {
	var b strings.Builder
	newHistogram(0.05).write(&b, "m", `x="y"`)
	want := "m_bucket{x=\"y\",le=\"0.05\"} 0\nm_bucket{x=\"y\",le=\"+Inf\"} 0\nm_sum{x=\"y\"} 0\nm_count{x=\"y\"} 0\n"
	if b.String() != want {
		t.Fatalf("got:\n%s", b.String())
	}
}
