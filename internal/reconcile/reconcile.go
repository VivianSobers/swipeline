package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/druid"
)

type GatewayCounts = auth.RunCounts
type DruidCounts = druid.DecisionCounts

type Result struct {
	Gateway  int64 `json:"gateway"`
	Druid    int64 `json:"druid"`
	Distinct int64 `json:"distinct"`
	Equal    bool  `json:"equal"`
}

func Compare(ctx context.Context, gw GatewayCounts, d DruidCounts) Result {
	equal := d.Count == d.Distinct && d.Count == gw.Final && gw.Pending == 0
	return Result{
		Gateway:  gw.Final,
		Druid:    d.Count,
		Distinct: d.Distinct,
		Equal:    equal,
	}
}

func Run(ctx context.Context, gatewayURL, druidURL, runID string, wait time.Duration, out io.Writer) error {
	client := &http.Client{Timeout: 10 * time.Second}
	druidClient := druid.NewClient(druidURL)
	deadline := time.Now().Add(wait)

	var lastRes Result
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 1. Query Gateway counts
		gwURL := fmt.Sprintf("%s/v1/runs/%s/counts", strings.TrimRight(gatewayURL, "/"), runID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gwURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to fetch gateway counts: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("gateway counts error (HTTP %d): %s", resp.StatusCode, string(body))
		}
		var gw GatewayCounts
		err = json.NewDecoder(resp.Body).Decode(&gw)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("failed to decode gateway counts: %w", err)
		}

		// 2. Query Druid counts
		d, err := druidClient.DecisionCounts(ctx, runID)
		if err != nil {
			return fmt.Errorf("failed to query druid counts: %w", err)
		}

		// Check for duplicate rows
		if d.Count > d.Distinct {
			return fmt.Errorf("DUPLICATE_ROWS: count=%d distinct=%d", d.Count, d.Distinct)
		}

		res := Compare(ctx, gw, d)
		lastRes = res
		if res.Equal {
			data, _ := json.Marshal(res)
			fmt.Fprintln(out, string(data))
			return nil
		}

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("reconciliation failed: gateway=%d druid=%d", lastRes.Gateway, lastRes.Druid)
}
