package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
)

// targets are the fixed response budgets, from entry to reply, per mode.
var targets = map[string]time.Duration{"latency": 50 * time.Millisecond, "throughput": 100 * time.Millisecond}

var modes = []string{"latency", "throughput"}

var (
	sources        = []string{"FLINK", "STAND_IN"}
	decisions      = []string{"APPROVE", "DECLINE"}
	standInReasons = []string{"SCORING_TIMEOUT", "SCORING_UNAVAILABLE", "NETWORK_UNAVAILABLE"}
)

// counters holds per-process decision counts with bounded label values.
type counters struct {
	mu       sync.Mutex
	finals   map[[2]string]uint64 // source, decision
	standIns map[string]uint64    // reason
}

func (c *counters) add(f *auth.Final) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finals == nil {
		c.finals, c.standIns = map[[2]string]uint64{}, map[string]uint64{}
	}
	c.finals[[2]string{f.Source, f.Decision}]++
	if f.Source == "STAND_IN" {
		c.standIns[f.Reason]++
	}
}

func (c *counters) write(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintln(w, "# HELP swipeline_gateway_final_decisions_total Final outcomes committed by this process.")
	fmt.Fprintln(w, "# TYPE swipeline_gateway_final_decisions_total counter")
	for _, source := range sources {
		for _, decision := range decisions {
			fmt.Fprintf(w, "swipeline_gateway_final_decisions_total{decision_source=%q,decision=%q} %d\n", source, decision, c.finals[[2]string{source, decision}])
		}
	}
	fmt.Fprintln(w, "# HELP swipeline_gateway_standin_total Fail-closed stand-in declines committed by this process.")
	fmt.Fprintln(w, "# TYPE swipeline_gateway_standin_total counter")
	for _, reason := range standInReasons {
		fmt.Fprintf(w, "swipeline_gateway_standin_total{reason=%q} %d\n", reason, c.standIns[reason])
	}
}

// histogram is a fixed-bucket Prometheus histogram, safe for concurrent use.
// Bucket bounds are inclusive upper limits.
type histogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []uint64 // per bucket, not cumulative
	sum    float64
	count  uint64
}

func newHistogram(bounds ...float64) *histogram {
	return &histogram{bounds: bounds, counts: make([]uint64, len(bounds))}
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += v
	h.count++
}

// write emits the histogram's samples in text exposition format; labels is
// a rendered label list such as deadline_mode="latency".
func (h *histogram) write(w io.Writer, name, labels string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var cumulative uint64
	for i, b := range h.bounds {
		cumulative += h.counts[i]
		fmt.Fprintf(w, "%s_bucket{%s,le=%q} %d\n", name, labels, strconv.FormatFloat(b, 'g', -1, 64), cumulative)
	}
	fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n", name, labels, h.count)
	fmt.Fprintf(w, "%s_sum{%s} %s\n", name, labels, strconv.FormatFloat(h.sum, 'g', -1, 64))
	fmt.Fprintf(w, "%s_count{%s} %d\n", name, labels, h.count)
}

func (s *Service) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP swipeline_gateway_budget_duration_seconds Authorization entry to reply, including budget misses.")
	fmt.Fprintln(w, "# TYPE swipeline_gateway_budget_duration_seconds histogram")
	for _, mode := range modes {
		s.budget[mode].write(w, "swipeline_gateway_budget_duration_seconds", fmt.Sprintf("deadline_mode=%q", mode))
	}
	fmt.Fprintln(w, "# HELP swipeline_gateway_latency_target_seconds Fixed response budget per deadline mode.")
	fmt.Fprintln(w, "# TYPE swipeline_gateway_latency_target_seconds gauge")
	for _, mode := range modes {
		fmt.Fprintf(w, "swipeline_gateway_latency_target_seconds{deadline_mode=%q} %g\n", mode, targets[mode].Seconds())
	}
	s.counts.write(w)
	fmt.Fprintln(w, "# HELP swipeline_gateway_reply_retries_total Reply operations retried after an error.")
	fmt.Fprintln(w, "# TYPE swipeline_gateway_reply_retries_total counter")
	fmt.Fprintf(w, "swipeline_gateway_reply_retries_total %d\n", s.ReplyRetries.Load())
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	b, err := s.DB.Backlog(ctx)
	up := 0
	if err == nil {
		up = 1
	}
	fmt.Fprintln(w, "# HELP swipeline_gateway_database_up Whether the backlog query succeeded.")
	fmt.Fprintf(w, "# TYPE swipeline_gateway_database_up gauge\nswipeline_gateway_database_up %d\n", up)
	if err != nil {
		return
	}
	fmt.Fprintln(w, "# HELP swipeline_gateway_pending_authorizations Admissions without a final outcome.")
	fmt.Fprintf(w, "# TYPE swipeline_gateway_pending_authorizations gauge\nswipeline_gateway_pending_authorizations %d\n", b.Pending)
	fmt.Fprintln(w, "# HELP swipeline_gateway_outbox_unsent Outbox rows not yet acknowledged by Kafka.")
	fmt.Fprintf(w, "# TYPE swipeline_gateway_outbox_unsent gauge\nswipeline_gateway_outbox_unsent %d\n", b.Unsent)
	fmt.Fprintln(w, "# HELP swipeline_gateway_outbox_oldest_age_seconds Age of the oldest unsent outbox row.")
	fmt.Fprintf(w, "# TYPE swipeline_gateway_outbox_oldest_age_seconds gauge\nswipeline_gateway_outbox_oldest_age_seconds %g\n", b.OldestUnsent)
}
