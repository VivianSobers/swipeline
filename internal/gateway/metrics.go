package gateway

import (
	"fmt"
	"io"
	"strconv"
	"sync"
)

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
