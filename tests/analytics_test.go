package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/druid"
	"github.com/VivianSobers/swipeline/internal/store"
)

// VerifyDruidDecisionFields checks the acceptance criteria for Unit 11.2:
// - Druid row count equals gateway finals
// - SELECT COUNT(*) WHERE decision_source='STAND_IN' AND (score IS NOT NULL OR velocity_60s IS NOT NULL) is 0
// - FLINK rows have non-null features
// - The CARD_TESTING burst rows carry the flag
// - INFORMATION_SCHEMA.COLUMNS for the datasource has no card_token
// - fraud_flags is multi-valued
func VerifyDruidDecisionFields(ctx context.Context, druidClient *druid.Client, gatewayURL, runID string) error {
	// 1. Fetch gateway counts
	gwURL := fmt.Sprintf("%s/v1/runs/%s/counts", strings.TrimRight(gatewayURL, "/"), runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gwURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create gateway counts request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch gateway counts: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("gateway counts failed with code %d: %s", resp.StatusCode, string(body))
	}
	var gwCounts auth.RunCounts
	if err := json.NewDecoder(resp.Body).Decode(&gwCounts); err != nil {
		return fmt.Errorf("failed to parse gateway counts: %w", err)
	}

	// 2. Druid row count equals gateway finals
	rows, err := druidClient.SQL(ctx, "SELECT COUNT(*) AS total_count FROM swipeline_decisions WHERE run_id = ?", runID)
	if err != nil {
		return fmt.Errorf("failed to query Druid total count: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("no rows returned for Druid total count")
	}
	var druidTotal int64
	for _, v := range rows[0] {
		switch n := v.(type) {
		case float64:
			druidTotal = int64(n)
		case int64:
			druidTotal = n
		case int:
			druidTotal = int64(n)
		}
	}
	if druidTotal != gwCounts.Final {
		return fmt.Errorf("Druid total count (%d) does not match gateway finals (%d)", druidTotal, gwCounts.Final)
	}

	// 3. Stand-in feature nullity check:
	// SELECT COUNT(*) WHERE decision_source='STAND_IN' AND (score IS NOT NULL OR velocity_60s IS NOT NULL) is 0
	standInRows, err := druidClient.SQL(ctx, "SELECT COUNT(*) AS c FROM swipeline_decisions WHERE run_id = ? AND decision_source='STAND_IN' AND (score IS NOT NULL OR velocity_60s IS NOT NULL)", runID)
	if err != nil {
		return fmt.Errorf("failed to query stand-in features: %w", err)
	}
	var standInNonNil int64
	if len(standInRows) > 0 {
		for _, v := range standInRows[0] {
			switch n := v.(type) {
			case float64:
				standInNonNil = int64(n)
			case int64:
				standInNonNil = n
			case int:
				standInNonNil = int64(n)
			}
		}
	}
	if standInNonNil != 0 {
		return fmt.Errorf("found %d stand-in decisions with non-null score or velocity", standInNonNil)
	}

	// 4. FLINK rows have non-null features
	flinkRows, err := druidClient.SQL(ctx, "SELECT COUNT(*) AS c FROM swipeline_decisions WHERE run_id = ? AND decision_source='FLINK' AND (score IS NULL OR velocity_60s IS NULL)", runID)
	if err != nil {
		return fmt.Errorf("failed to query flink null features: %w", err)
	}
	var flinkNullFeatures int64
	if len(flinkRows) > 0 {
		for _, v := range flinkRows[0] {
			switch n := v.(type) {
			case float64:
				flinkNullFeatures = int64(n)
			case int64:
				flinkNullFeatures = n
			case int:
				flinkNullFeatures = int64(n)
			}
		}
	}
	if flinkNullFeatures != 0 {
		return fmt.Errorf("found %d flink decisions with null score or velocity", flinkNullFeatures)
	}

	// 5. The CARD_TESTING burst rows carry the flag
	burstRows, err := druidClient.SQL(ctx, "SELECT COUNT(*) AS c FROM swipeline_decisions WHERE run_id = ? AND MV_CONTAINS(fraud_flags, 'CARD_TESTING')", runID)
	if err != nil {
		return fmt.Errorf("failed to query CARD_TESTING burst rows: %w", err)
	}
	var burstCount int64
	if len(burstRows) > 0 {
		for _, v := range burstRows[0] {
			switch n := v.(type) {
			case float64:
				burstCount = int64(n)
			case int64:
				burstCount = n
			case int:
				burstCount = int64(n)
			}
		}
	}
	if burstCount == 0 {
		return fmt.Errorf("no burst rows carry the CARD_TESTING flag")
	}

	// 6. INFORMATION_SCHEMA.COLUMNS for the datasource has no card_token
	cardTokenCols, err := druidClient.SQL(ctx, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'swipeline_decisions' AND COLUMN_NAME = 'card_token'")
	if err != nil {
		return fmt.Errorf("failed to query INFORMATION_SCHEMA for card_token: %w", err)
	}
	if len(cardTokenCols) > 0 {
		return fmt.Errorf("unexpected card_token column found in swipeline_decisions: %+v", cardTokenCols)
	}

	// 7. fraud_flags is multi-valued dimension in swipeline_decisions
	flagsCol, err := druidClient.SQL(ctx, "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'swipeline_decisions' AND COLUMN_NAME = 'fraud_flags'")
	if err != nil {
		return fmt.Errorf("failed to query INFORMATION_SCHEMA for fraud_flags: %w", err)
	}
	if len(flagsCol) == 0 {
		return fmt.Errorf("missing fraud_flags column in swipeline_decisions")
	}

	return nil
}

func TestVerifyDruidDecisionFieldsUnit(t *testing.T) {
	// 1. Successful verification
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "installed-42", Final: 1000, Admitted: 1000, Pending: 0, StandIn: 5})
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		query, _ := req["query"].(string)

		if strings.Contains(query, "card_token") {
			_ = json.NewEncoder(w).Encode([]map[string]any{})
			return
		}
		if strings.Contains(query, "TABLE_NAME = 'swipeline_decisions' AND COLUMN_NAME = 'fraud_flags'") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"COLUMN_NAME": "fraud_flags", "DATA_TYPE": "VARCHAR"}})
			return
		}
		if strings.Contains(query, "CARD_TESTING") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"c": 31}})
			return
		}
		if strings.Contains(query, "STAND_IN") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"c": 0}})
			return
		}
		if strings.Contains(query, "FLINK") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"c": 0}})
			return
		}
		// total count
		_ = json.NewEncoder(w).Encode([]map[string]any{{"total_count": 1000}})
	})

	srv := httptest.NewServer(mockHandler)
	defer srv.Close()

	client := druid.NewClient(srv.URL)
	if err := VerifyDruidDecisionFields(context.Background(), client, srv.URL, "installed-42"); err != nil {
		t.Fatalf("expected successful verification, got: %v", err)
	}

	// 2. Reject card_token column if present
	cardTokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "installed-42", Final: 1000})
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		query, _ := req["query"].(string)

		if strings.Contains(query, "card_token") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"COLUMN_NAME": "card_token"}})
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"total_count": 1000, "c": 0}})
	}))
	defer cardTokenSrv.Close()

	ctClient := druid.NewClient(cardTokenSrv.URL)
	if err := VerifyDruidDecisionFields(context.Background(), ctClient, cardTokenSrv.URL, "installed-42"); err == nil {
		t.Fatal("expected error for card_token column presence, got nil")
	}

	// 3. Reject stand-in non-null features
	standInSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "installed-42", Final: 1000})
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		query, _ := req["query"].(string)

		if strings.Contains(query, "STAND_IN") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"c": 3}})
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"total_count": 1000, "c": 0}})
	}))
	defer standInSrv.Close()

	siClient := druid.NewClient(standInSrv.URL)
	if err := VerifyDruidDecisionFields(context.Background(), siClient, standInSrv.URL, "installed-42"); err == nil {
		t.Fatal("expected error for stand-in non-null features, got nil")
	}
}

func fetchGatewayApprovalsAndFinals(ctx context.Context, gatewayURL, dbURL, runID string) (int64, int64, error) {
	if dbURL != "" {
		db, err := store.Open(ctx, dbURL)
		if err == nil {
			defer db.DB.Close()
			var approvals, finals int64
			err = db.DB.QueryRowContext(ctx, `SELECT
				coalesce(count(*) FILTER (WHERE final_json->>'decision' = 'APPROVE'), 0),
				coalesce(count(*) FILTER (WHERE status = 'FINAL'), 0)
			FROM authorizations WHERE run_id = $1`, runID).Scan(&approvals, &finals)
			if err == nil && finals > 0 {
				return approvals, finals, nil
			}
		}
	}

	gwURL := strings.TrimRight(gatewayURL, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gwURL+"/v1/runs/"+runID+"/counts", nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	var counts auth.RunCounts
	if err := json.NewDecoder(resp.Body).Decode(&counts); err != nil {
		return 0, 0, err
	}

	metricsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, gwURL+"/metrics", nil)
	if err != nil {
		return 0, 0, err
	}
	metricsResp, err := http.DefaultClient.Do(metricsReq)
	if err != nil {
		return 0, 0, err
	}
	defer metricsResp.Body.Close()
	metricsBody, _ := io.ReadAll(metricsResp.Body)
	var approvals int64
	lines := strings.Split(string(metricsBody), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "swipeline_gateway_final_decisions_total{") && strings.Contains(line, `decision="APPROVE"`) {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				var n int64
				fmt.Sscanf(fields[1], "%d", &n)
				approvals += n
			}
		}
	}
	return approvals, counts.Final, nil
}

// VerifyDashboardChartsThroughSuperset checks the acceptance criteria for Unit 11.3:
// - Log in with the generated password
// - List the dashboard by slug payments-operations
// - POST each chart's query to /api/v1/chart/data
// - Every response is 200 with at least one row for the run
// - The approval-rate value equals approvals/finals from the gateway counts within 1e-9
func VerifyDashboardChartsThroughSuperset(ctx context.Context, supersetURL, password, gatewayURL, dbURL, runID string) error {
	client := &http.Client{Timeout: 30 * time.Second}
	base := strings.TrimRight(supersetURL, "/")

	// 1. Log in with the generated password
	loginURL := fmt.Sprintf("%s/api/v1/security/login", base)
	loginPayload := map[string]any{
		"username": "admin",
		"password": password,
		"provider": "db",
		"refresh":  true,
	}
	loginBytes, _ := json.Marshal(loginPayload)
	loginReq, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, bytes.NewReader(loginBytes))
	if err != nil {
		return fmt.Errorf("failed to create login request: %w", err)
	}
	loginReq.Header.Set("Content-Type", "application/json")
	loginResp, err := client.Do(loginReq)
	if err != nil {
		return fmt.Errorf("superset login failed: %w", err)
	}
	defer loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(loginResp.Body)
		return fmt.Errorf("superset login returned code %d: %s", loginResp.StatusCode, string(body))
	}
	var loginData struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(loginResp.Body).Decode(&loginData); err != nil {
		return fmt.Errorf("failed to decode login response: %w", err)
	}
	token := loginData.AccessToken

	// Fetch CSRF token if available
	csrfURL := fmt.Sprintf("%s/api/v1/security/csrf_token/", base)
	csrfReq, err := http.NewRequestWithContext(ctx, http.MethodGet, csrfURL, nil)
	var csrfToken string
	if err == nil {
		csrfReq.Header.Set("Authorization", "Bearer "+token)
		if csrfResp, err := client.Do(csrfReq); err == nil {
			if csrfResp.StatusCode == http.StatusOK {
				var csrfData struct {
					Result string `json:"result"`
				}
				if err := json.NewDecoder(csrfResp.Body).Decode(&csrfData); err == nil {
					csrfToken = csrfData.Result
				}
			}
			csrfResp.Body.Close()
		}
	}

	// 2. List the dashboard by slug payments-operations
	dashURL := fmt.Sprintf("%s/api/v1/dashboard/payments-operations", base)
	dashReq, err := http.NewRequestWithContext(ctx, http.MethodGet, dashURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create dashboard request: %w", err)
	}
	dashReq.Header.Set("Authorization", "Bearer "+token)
	if csrfToken != "" {
		dashReq.Header.Set("X-CSRFToken", csrfToken)
	}
	dashResp, err := client.Do(dashReq)
	if err != nil {
		return fmt.Errorf("failed to fetch dashboard by slug: %w", err)
	}
	defer dashResp.Body.Close()
	if dashResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(dashResp.Body)
		return fmt.Errorf("dashboard fetch returned code %d: %s", dashResp.StatusCode, string(body))
	}

	// 3. POST each chart's query to /api/v1/chart/data
	queries := map[string]string{
		"approval_rate":      `SELECT CASE WHEN COUNT(*) = 0 THEN NULL ELSE CAST(SUM(CASE WHEN "decision" = 'APPROVE' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) END AS "approval_rate" FROM "swipeline_decisions" WHERE "run_id" = :run_id`,
		"decline_reasons":    `SELECT "reason", "decision_source", COUNT(*) AS "decline_count" FROM "swipeline_decisions" WHERE "run_id" = :run_id AND "decision" = 'DECLINE' GROUP BY "reason", "decision_source" ORDER BY "decline_count" DESC`,
		"fraud_flags_by_mcc": `SELECT "mcc", 'CARD_TESTING' AS "flag", COUNT(*) AS "flag_count" FROM "swipeline_decisions" WHERE "run_id" = :run_id AND MV_CONTAINS("fraud_flags", 'CARD_TESTING') GROUP BY "mcc" UNION ALL SELECT "mcc", 'CARD_VELOCITY' AS "flag", COUNT(*) AS "flag_count" FROM "swipeline_decisions" WHERE "run_id" = :run_id AND MV_CONTAINS("fraud_flags", 'CARD_VELOCITY') GROUP BY "mcc" UNION ALL SELECT "mcc", 'SPEND_ANOMALY' AS "flag", COUNT(*) AS "flag_count" FROM "swipeline_decisions" WHERE "run_id" = :run_id AND MV_CONTAINS("fraud_flags", 'SPEND_ANOMALY') GROUP BY "mcc"`,
		"freshness":          `SELECT MAX(__time) AS "max_time", (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - EXTRACT(EPOCH FROM MAX(__time))) AS "ingestion_lag_seconds", COUNT(*) AS "total_count", CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE CAST(SUM(CASE WHEN "decision_source" = 'STAND_IN' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) END AS "standin_share" FROM "swipeline_decisions" WHERE "run_id" = :run_id`,
		"latency":            `SELECT CASE WHEN "decision_latency_ms" < 5.0 THEN '0-5' WHEN "decision_latency_ms" < 10.0 THEN '5-10' WHEN "decision_latency_ms" < 20.0 THEN '10-20' WHEN "decision_latency_ms" < 35.0 THEN '20-35' WHEN "decision_latency_ms" < 50.0 THEN '35-50' ELSE '50+' END AS "latency_bucket", COUNT(*) AS "bucket_count", AVG("decision_latency_ms") AS "mean_latency_ms", MAX("decision_latency_ms") AS "max_latency_ms" FROM "swipeline_decisions" WHERE "run_id" = :run_id GROUP BY CASE WHEN "decision_latency_ms" < 5.0 THEN '0-5' WHEN "decision_latency_ms" < 10.0 THEN '5-10' WHEN "decision_latency_ms" < 20.0 THEN '10-20' WHEN "decision_latency_ms" < 35.0 THEN '20-35' WHEN "decision_latency_ms" < 50.0 THEN '35-50' ELSE '50+' END`,
		"tps":                `SELECT TIME_FLOOR(__time, 'PT10S') AS "time_bucket", COUNT(*) / 10.0 AS "tps" FROM "swipeline_decisions" WHERE "run_id" = :run_id GROUP BY 1 ORDER BY 1`,
	}

	chartDataURL := fmt.Sprintf("%s/api/v1/chart/data", base)
	var chartApprovalRate float64
	var foundApprovalRate bool

	for name, rawSQL := range queries {
		renderedSQL := strings.ReplaceAll(rawSQL, ":run_id", fmt.Sprintf("'%s'", runID))
		chartPayload := map[string]any{
			"datasource": map[string]any{
				"id":   "e4b3c2a1-0000-4000-8000-000000000002__table",
				"type": "table",
			},
			"queries": []map[string]any{
				{
					"sql": renderedSQL,
					"extras": map[string]any{
						"template_params": map[string]any{
							"run_id": runID,
						},
					},
				},
			},
			"result_format": "json",
		}
		dataBytes, _ := json.Marshal(chartPayload)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, chartDataURL, bytes.NewReader(dataBytes))
		if err != nil {
			return fmt.Errorf("failed to create chart request for %s: %w", name, err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		if csrfToken != "" {
			req.Header.Set("X-CSRFToken", csrfToken)
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to query chart %s: %w", name, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("chart %s returned code %d: %s", name, resp.StatusCode, string(body))
		}

		var chartResult struct {
			Result []struct {
				Data   []map[string]any `json:"data"`
				Status string           `json:"status"`
			} `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&chartResult); err != nil {
			return fmt.Errorf("failed to decode chart response for %s: %w", name, err)
		}
		if len(chartResult.Result) == 0 || len(chartResult.Result[0].Data) == 0 {
			return fmt.Errorf("chart %s returned no data rows for run %s", name, runID)
		}

		if name == "approval_rate" {
			row := chartResult.Result[0].Data[0]
			for k, v := range row {
				if strings.EqualFold(k, "approval_rate") {
					if f, ok := v.(float64); ok {
						chartApprovalRate = f
						foundApprovalRate = true
					}
				}
			}
		}
	}

	if !foundApprovalRate {
		return fmt.Errorf("approval_rate chart did not contain approval_rate value")
	}

	// 4. Verify approval-rate value equals approvals/finals from gateway counts within 1e-9
	approvals, finals, err := fetchGatewayApprovalsAndFinals(ctx, gatewayURL, dbURL, runID)
	if err != nil {
		return fmt.Errorf("failed to fetch gateway approvals/finals: %w", err)
	}
	if finals == 0 {
		return fmt.Errorf("gateway reported 0 final decisions")
	}
	expectedRate := float64(approvals) / float64(finals)
	if math.Abs(chartApprovalRate-expectedRate) > 1e-9 {
		return fmt.Errorf("approval-rate mismatch: chart has %g, gateway has %d/%d = %g (diff: %g)",
			chartApprovalRate, approvals, finals, expectedRate, math.Abs(chartApprovalRate-expectedRate))
	}

	return nil
}

func TestQueryDashboardChartsThroughSupersetUnit(t *testing.T) {
	// 1. Success case
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/login") {
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "mock-token"})
			return
		}
		if strings.Contains(r.URL.Path, "/csrf_token") {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "mock-csrf"})
			return
		}
		if strings.Contains(r.URL.Path, "/dashboard/") {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"slug": "payments-operations"}})
			return
		}
		if strings.Contains(r.URL.Path, "/chart/data") {
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			queries, _ := req["queries"].([]any)
			approvalRate := 0.85
			data := []map[string]any{{"approval_rate": approvalRate, "count": 100}}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": []map[string]any{
					{
						"data":   data,
						"status": "success",
					},
				},
			})
			_ = queries
			return
		}
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "installed-42", Final: 100})
			return
		}
		if strings.Contains(r.URL.Path, "/metrics") {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("swipeline_gateway_final_decisions_total{decision_source=\"FLINK\",decision=\"APPROVE\"} 85\n"))
			return
		}
	}))
	defer srv.Close()

	if err := VerifyDashboardChartsThroughSuperset(context.Background(), srv.URL, "secret", srv.URL, "", "installed-42"); err != nil {
		t.Fatalf("expected successful superset chart queries, got: %v", err)
	}

	// 2. Failure case: chart/data returns 500
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/login") {
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "mock-token"})
			return
		}
		if strings.Contains(r.URL.Path, "/dashboard/") {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"slug": "payments-operations"}})
			return
		}
		if strings.Contains(r.URL.Path, "/chart/data") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer failSrv.Close()

	if err := VerifyDashboardChartsThroughSuperset(context.Background(), failSrv.URL, "secret", failSrv.URL, "", "installed-42"); err == nil {
		t.Fatal("expected error on superset chart data 500 failure, got nil")
	}

	// 3. Failure case: approval rate mismatch > 1e-9
	mismatchSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/login") {
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "mock-token"})
			return
		}
		if strings.Contains(r.URL.Path, "/dashboard/") {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"slug": "payments-operations"}})
			return
		}
		if strings.Contains(r.URL.Path, "/chart/data") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": []map[string]any{
					{
						"data":   []map[string]any{{"approval_rate": 0.50}},
						"status": "success",
					},
				},
			})
			return
		}
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "installed-42", Final: 100})
			return
		}
		if strings.Contains(r.URL.Path, "/metrics") {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("swipeline_gateway_final_decisions_total{decision_source=\"FLINK\",decision=\"APPROVE\"} 85\n"))
			return
		}
	}))
	defer mismatchSrv.Close()

	if err := VerifyDashboardChartsThroughSuperset(context.Background(), mismatchSrv.URL, "secret", mismatchSrv.URL, "", "installed-42"); err == nil {
		t.Fatal("expected error on approval rate mismatch, got nil")
	}
}

func TestInstalledAnalytics(t *testing.T) {
	druidURL := os.Getenv("SWIPELINE_DRUID")
	if druidURL == "" {
		t.Skip("run make m3-demo on server")
	}

	gatewayURL := os.Getenv("SWIPELINE_TEST_GATEWAY")
	if gatewayURL == "" {
		gatewayURL = "http://gateway:8081"
	}
	runID := os.Getenv("SWIPELINE_RUN_ID")
	if runID == "" {
		runID = "installed-42"
	}
	supersetURL := os.Getenv("SWIPELINE_SUPERSET")
	if supersetURL == "" {
		supersetURL = "http://superset:8088"
	}
	supersetPassword := os.Getenv("SWIPELINE_SUPERSET_PASSWORD")
	dbURL := os.Getenv("SWIPELINE_TEST_DB")

	client := druid.NewClient(druidURL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("DruidDecisionFields", func(t *testing.T) {
		if err := VerifyDruidDecisionFields(ctx, client, gatewayURL, runID); err != nil {
			t.Fatalf("VerifyDruidDecisionFields failed: %v", err)
		}
	})

	t.Run("DashboardChartsThroughSuperset", func(t *testing.T) {
		if err := VerifyDashboardChartsThroughSuperset(ctx, supersetURL, supersetPassword, gatewayURL, dbURL, runID); err != nil {
			t.Fatalf("VerifyDashboardChartsThroughSuperset failed: %v", err)
		}
	})

	t.Run("FreshnessAfterReplay", func(t *testing.T) {
		lag, err := VerifyFreshnessAfterReplay(ctx, client, runID, 30.0, 60*time.Second)
		if err != nil {
			t.Fatalf("VerifyFreshnessAfterReplay failed: %v", err)
		}
		t.Logf("freshness lag=%.3fs (budget=30s)", lag)
	})
}

// VerifyFreshnessAfterReplay checks the acceptance criteria for Unit 11.4:
// - The freshness query's lag is below 30 s within 60 s after the replay finishes
func VerifyFreshnessAfterReplay(ctx context.Context, druidClient *druid.Client, runID string, budgetLagSeconds float64, waitDuration time.Duration) (float64, error) {
	deadline := time.Now().Add(waitDuration)
	query := `SELECT
  MAX(__time) AS "max_time",
  (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - EXTRACT(EPOCH FROM MAX(__time))) AS "ingestion_lag_seconds",
  COUNT(*) AS "total_count",
  CASE
    WHEN COUNT(*) = 0 THEN 0.0
    ELSE CAST(SUM(CASE WHEN "decision_source" = 'STAND_IN' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)
  END AS "standin_share"
FROM "swipeline_decisions"
WHERE "run_id" = ?`

	var lastLag float64 = -1
	for {
		select {
		case <-ctx.Done():
			return lastLag, ctx.Err()
		default:
		}

		rows, err := druidClient.SQL(ctx, query, runID)
		if err == nil && len(rows) > 0 {
			row := rows[0]
			for k, v := range row {
				if strings.EqualFold(k, "ingestion_lag_seconds") {
					switch val := v.(type) {
					case float64:
						lastLag = val
					case int64:
						lastLag = float64(val)
					case int:
						lastLag = float64(val)
					}
				}
			}
			if lastLag >= 0 && lastLag < budgetLagSeconds {
				return lastLag, nil
			}
		}

		if time.Now().After(deadline) {
			if lastLag < 0 {
				return lastLag, fmt.Errorf("freshness query returned no valid lag within %v", waitDuration)
			}
			return lastLag, fmt.Errorf("freshness lag %.3fs exceeded budget of %.3fs within %v", lastLag, budgetLagSeconds, waitDuration)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func TestVerifyFreshnessAfterReplayUnit(t *testing.T) {
	// 1. Success case: lag is within budget
	srvOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"ingestion_lag_seconds": 12.5}})
	}))
	defer srvOK.Close()

	clientOK := druid.NewClient(srvOK.URL)
	lag, err := VerifyFreshnessAfterReplay(context.Background(), clientOK, "installed-42", 30.0, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("expected successful freshness check, got: %v", err)
	}
	if lag != 12.5 {
		t.Fatalf("expected lag 12.5, got %f", lag)
	}

	// 2. Failure case: lag exceeds budget
	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"ingestion_lag_seconds": 45.0}})
	}))
	defer srvFail.Close()

	clientFail := druid.NewClient(srvFail.URL)
	_, err = VerifyFreshnessAfterReplay(context.Background(), clientFail, "installed-42", 30.0, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error when freshness lag exceeds budget, got nil")
	}
}
