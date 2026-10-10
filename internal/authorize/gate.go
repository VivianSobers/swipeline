package authorize

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Verdict measures one scrape interval. P99 uses linear interpolation between
// cumulative histogram boundaries, matching the Prometheus quantile model.
type Verdict struct {
	Pass         bool
	P99          time.Duration
	StandInShare float64
	Samples      int64
}

type gateMetrics struct {
	buckets map[float64]float64
	finals  map[string]float64
}

var metricLabel = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="([^"\n]*)"`)

func parseGateMetrics(text, mode string) (gateMetrics, error) {
	m := gateMetrics{map[float64]float64{}, map[string]float64{}}
	scan := bufio.NewScanner(strings.NewReader(text))
	for scan.Scan() {
		line := scan.Text()
		bucket := strings.HasPrefix(line, "swipeline_gateway_budget_duration_seconds_bucket{")
		final := strings.HasPrefix(line, "swipeline_gateway_final_decisions_total{")
		if !bucket && !final {
			continue
		}
		close := strings.IndexByte(line, '}')
		if close < 0 {
			return m, errors.New("unterminated metric labels")
		}
		labels := map[string]string{}
		for _, pair := range metricLabel.FindAllStringSubmatch(line[:close], -1) {
			labels[pair[1]] = pair[2]
		}
		if bucket && labels["deadline_mode"] != mode {
			continue
		}
		fields := strings.Fields(line[close+1:])
		if len(fields) != 1 {
			return m, errors.New("invalid metric value")
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return m, errors.New("invalid metric count")
		}
		if bucket {
			bound, err := strconv.ParseFloat(labels["le"], 64)
			if err != nil || math.IsNaN(bound) || bound <= 0 {
				return m, errors.New("invalid histogram boundary")
			}
			if _, ok := m.buckets[bound]; ok {
				return m, errors.New("duplicate histogram boundary")
			}
			m.buckets[bound] = value
		} else {
			source, decision := labels["decision_source"], labels["decision"]
			if (source != "FLINK" && source != "STAND_IN") || (decision != "APPROVE" && decision != "DECLINE") {
				return m, errors.New("invalid decision labels")
			}
			key := source + "/" + decision
			if _, ok := m.finals[key]; ok {
				return m, errors.New("duplicate decision counter")
			}
			m.finals[key] = value
		}
	}
	return m, scan.Err()
}

// Gate evaluates new observations only. Missing, reset or inconsistent metrics
// are errors, never evidence that a latency budget passed.
func Gate(before, after, mode string, target time.Duration) (Verdict, error) {
	var v Verdict
	if (mode != "latency" && mode != "throughput") || target <= 0 {
		return v, errors.New("invalid latency gate configuration")
	}
	a, err := parseGateMetrics(before, mode)
	if err != nil {
		return v, err
	}
	b, err := parseGateMetrics(after, mode)
	if err != nil {
		return v, err
	}
	for bound := range a.buckets {
		if _, ok := b.buckets[bound]; !ok {
			return v, errors.New("histogram series disappeared")
		}
	}
	for key := range a.finals {
		if _, ok := b.finals[key]; !ok {
			return v, errors.New("decision series disappeared")
		}
	}
	total, ok := b.buckets[math.Inf(1)]
	total -= a.buckets[math.Inf(1)]
	if !ok || total <= 0 {
		return v, errors.New("no histogram observations")
	}
	var bounds []float64
	for bound, count := range b.buckets {
		delta := count - a.buckets[bound]
		if delta < 0 {
			return v, errors.New("histogram counter reset")
		}
		b.buckets[bound] = delta
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	previous := 0.0
	for _, bound := range bounds {
		if b.buckets[bound] < previous {
			return v, errors.New("nonmonotonic histogram")
		}
		previous = b.buckets[bound]
	}
	var finals, stand float64
	for key, count := range b.finals {
		delta := count - a.finals[key]
		if delta < 0 {
			return v, errors.New("decision counter reset")
		}
		finals += delta
		if strings.HasPrefix(key, "STAND_IN/") {
			stand += delta
		}
	}
	if finals <= 0 {
		return v, errors.New("no final decisions")
	}
	rank := 0.99 * total
	lower, count := 0.0, 0.0
	for _, bound := range bounds {
		upper := b.buckets[bound]
		if upper >= rank {
			if math.IsInf(bound, 1) {
				v.P99 = time.Duration(math.MaxInt64)
			} else {
				v.P99 = time.Duration((lower + (bound-lower)*(rank-count)/(upper-count)) * float64(time.Second))
			}
			break
		}
		lower, count = bound, upper
	}
	if total > math.MaxInt64 {
		return v, fmt.Errorf("too many observations: %g", total)
	}
	v.Samples = int64(total)
	v.StandInShare = stand / finals
	v.Pass = v.P99 <= target && v.StandInShare < 0.01
	return v, nil
}
