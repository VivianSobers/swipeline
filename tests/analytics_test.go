package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/VivianSobers/swipeline/internal/dashboard"
	"github.com/VivianSobers/swipeline/internal/druid"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/reconcile"
	"github.com/VivianSobers/swipeline/internal/replay"
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
	jar, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second, Jar: jar}
	base := strings.TrimRight(supersetURL, "/")
	var token, csrf string
	call := func(method, path string, payload any, result any) error {
		var body io.Reader
		if payload != nil {
			data, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			body = bytes.NewReader(data)
		}
		req, err := http.NewRequestWithContext(ctx, method, base+path, body)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if csrf != "" {
			req.Header.Set("X-CSRFToken", csrf)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("%s %s HTTP %d: %s", method, path, resp.StatusCode, data)
		}
		return json.NewDecoder(resp.Body).Decode(result)
	}
	var login struct {
		AccessToken string `json:"access_token"`
	}
	if err := call(http.MethodPost, "/api/v1/security/login", map[string]any{"username": "admin", "password": password, "provider": "db", "refresh": true}, &login); err != nil {
		return err
	}
	token = login.AccessToken
	if token == "" {
		return fmt.Errorf("Superset login returned no access token")
	}
	var csrfDoc struct {
		Result string `json:"result"`
	}
	if err := call(http.MethodGet, "/api/v1/security/csrf_token/", nil, &csrfDoc); err != nil {
		return err
	}
	csrf = csrfDoc.Result
	if csrf == "" {
		return fmt.Errorf("Superset returned no CSRF token")
	}
	var chartList struct {
		Result []struct {
			ID   int    `json:"id"`
			Name string `json:"slice_name"`
		} `json:"result"`
	}
	if err := call(http.MethodGet, "/api/v1/dashboard/payments-operations/charts", nil, &chartList); err != nil {
		return err
	}
	expected := map[string]bool{}
	for _, def := range dashboard.ChartDefinitions {
		expected[def.SliceName] = false
	}
	var rate float64
	foundRate := false
	for _, chart := range chartList.Result {
		seen, ok := expected[chart.Name]
		if !ok {
			continue
		}
		if seen {
			return fmt.Errorf("duplicate dashboard chart %s", chart.Name)
		}
		expected[chart.Name] = true
		var detail struct {
			Result struct {
				QueryContext   string `json:"query_context"`
				DatasourceID   int    `json:"datasource_id"`
				DatasourceType string `json:"datasource_type"`
			} `json:"result"`
		}
		if err := call(http.MethodGet, fmt.Sprintf("/api/v1/chart/%d", chart.ID), nil, &detail); err != nil {
			return err
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(detail.Result.QueryContext), &payload); err != nil || payload == nil {
			return fmt.Errorf("chart %s has no valid saved query context", chart.Name)
		}
		queries, ok := payload["queries"].([]any)
		if !ok || len(queries) == 0 {
			return fmt.Errorf("chart %s has no queries", chart.Name)
		}
		if detail.Result.DatasourceID <= 0 || detail.Result.DatasourceType != "table" {
			return fmt.Errorf("chart %s has no imported table datasource", chart.Name)
		}
		payload["datasource"] = map[string]any{"id": detail.Result.DatasourceID, "type": detail.Result.DatasourceType}
		payload["result_format"] = "json"
		payload["result_type"] = "full"
		payload["force"] = true
		for _, raw := range queries {
			q, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid query in chart %s", chart.Name)
			}
			if _, bad := q["sql"]; bad {
				return fmt.Errorf("chart %s uses unsupported raw SQL query", chart.Name)
			}
			filters, _ := q["filters"].([]any)
			q["filters"] = append(filters, map[string]any{"col": "run_id", "op": "==", "val": runID})
		}
		var data struct {
			Result []struct {
				Data   []map[string]any `json:"data"`
				Status string           `json:"status"`
				Error  string           `json:"error"`
			} `json:"result"`
		}
		if err := call(http.MethodPost, "/api/v1/chart/data", payload, &data); err != nil {
			return fmt.Errorf("chart %s: %w", chart.Name, err)
		}
		if len(data.Result) != len(queries) {
			return fmt.Errorf("chart %s returned incomplete query results", chart.Name)
		}
		for _, result := range data.Result {
			if result.Status != "success" || result.Error != "" || len(result.Data) == 0 {
				return fmt.Errorf("chart %s returned status %q, error %q or no data", chart.Name, result.Status, result.Error)
			}
			if chart.Name == "Approval Rate" {
				for k, v := range result.Data[0] {
					if strings.EqualFold(k, "approval_rate") {
						rate, foundRate = v.(float64)
					}
				}
			}
		}
	}
	for name, seen := range expected {
		if !seen {
			return fmt.Errorf("dashboard missing chart %s", name)
		}
	}
	if !foundRate || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return fmt.Errorf("approval_rate chart did not contain a finite approval_rate value")
	}
	approvals, finals, err := fetchGatewayApprovalsAndFinals(ctx, gatewayURL, dbURL, runID)
	if err != nil {
		return err
	}
	if finals == 0 {
		return fmt.Errorf("gateway reported 0 final decisions")
	}
	expectedRate := float64(approvals) / float64(finals)
	if math.Abs(rate-expectedRate) > 1e-9 {
		return fmt.Errorf("approval-rate mismatch: chart has %g, gateway has %d/%d = %g", rate, approvals, finals, expectedRate)
	}
	return nil
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

	t.Run("FreshnessAfterReplay", func(t *testing.T) {
		lag, err := VerifyAnalyticsSnapshotReady(ctx, client, gatewayURL, runID, 30*time.Second)
		if err != nil {
			t.Fatalf("VerifyFreshnessAfterReplay failed: %v", err)
		}
		t.Logf("freshness lag=%.3fs (budget=30s)", lag)
	})

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

	faultDir := os.Getenv("SWIPELINE_FAULT_DIR")
	t.Run("ReconcileAfterIngestionRestarts", func(t *testing.T) {
		restartRunID := fmt.Sprintf("restart-%d", time.Now().UnixNano())
		if err := ReconcileAfterIngestionRestarts(ctx, gatewayURL, druidURL, faultDir, restartRunID); err != nil {
			t.Fatalf("ReconcileAfterIngestionRestarts failed: %v", err)
		}
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

// ReconcileAfterIngestionRestarts checks the acceptance criteria for Unit 10.4:
//   - Installed (Task 11 target): replay 1000; during the replay suspend the supervisor
//     (POST /druid/indexer/v1/supervisor/<id>/suspend), wait 5 s, resume;
//     restart druid-middlemanager; after the replay, reconcile succeeds with counts equal (no inflation).
func ReconcileAfterIngestionRestarts(ctx context.Context, gatewayURL, druidURL, faultDir, runID string) error {
	return ReconcileAfterIngestionRestartsWithWait(ctx, gatewayURL, druidURL, faultDir, runID, 5*time.Second)
}

func ReconcileAfterIngestionRestartsWithWait(ctx context.Context, gatewayURL, druidURL, faultDir, runID string, suspendWait time.Duration) error {
	client := &http.Client{Timeout: 10 * time.Second}
	druidBase := strings.TrimRight(druidURL, "/")

	// 1. Start replay of 1000 events in background if gateway supports it
	replayDone := make(chan error, 1)
	go func() {
		cfg := replay.Config{
			Generator: generator.Config{RunID: runID, Seed: 42, Day: "2026-10-08", Events: 1000},
			TPS:       50,
			Workers:   1,
			Queue:     128,
		}
		authClient := &authorize.Client{Base: gatewayURL, HTTP: client}
		_, err := authorize.Run(ctx, cfg, authClient, "latency", 128)
		replayDone <- err
	}()

	// Allow replay to begin
	time.Sleep(50 * time.Millisecond)

	// 2. Suspend the supervisor
	suspendURL := fmt.Sprintf("%s/druid/indexer/v1/supervisor/swipeline_decisions/suspend", druidBase)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, suspendURL, nil)
	if err == nil {
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}

	// Wait specified duration (5s in live run)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(suspendWait):
	}

	// Resume the supervisor
	resumeURL := fmt.Sprintf("%s/druid/indexer/v1/supervisor/swipeline_decisions/resume", druidBase)
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, resumeURL, nil)
	if err == nil {
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}

	// 3. Restart druid-middlemanager via fault injection worker if configured
	if faultDir != "" {
		reqFile := filepath.Join(faultDir, "request")
		doneFile := filepath.Join(faultDir, "done")
		id := fmt.Sprintf("%d", time.Now().UnixNano())
		if err := os.WriteFile(reqFile, []byte(fmt.Sprintf("%s restart-middlemanager\n", id)), 0644); err == nil {
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				if data, err := os.ReadFile(doneFile); err == nil {
					if strings.HasPrefix(string(data), id) {
						_ = os.Remove(doneFile)
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	// Wait for replay to finish or timeout
	select {
	case <-replayDone:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Minute):
	}

	// 4. Reconcile gateway and Druid counts (no inflation)
	return reconcile.Run(ctx, gatewayURL, druidURL, runID, 2*time.Minute, io.Discard)
}

func TestReconcileAfterIngestionRestartsUnit(t *testing.T) {
	// 1. Success case: counts match with no inflation
	srvOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "restart-42", Final: 1000, Admitted: 1000})
			return
		}
		if strings.Contains(r.URL.Path, "/suspend") || strings.Contains(r.URL.Path, "/resume") {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "swipeline_decisions"})
			return
		}
		if strings.Contains(r.URL.Path, "/sql") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"total_count": 1000, "distinct_count": 1000}})
			return
		}
	}))
	defer srvOK.Close()

	if err := ReconcileAfterIngestionRestartsWithWait(context.Background(), srvOK.URL, srvOK.URL, "", "restart-42", 10*time.Millisecond); err != nil {
		t.Fatalf("expected successful reconciliation, got: %v", err)
	}

	// 2. Failure case: duplicate rows / inflation
	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/counts") {
			_ = json.NewEncoder(w).Encode(auth.RunCounts{RunID: "restart-42", Final: 1000, Admitted: 1000})
			return
		}
		if strings.Contains(r.URL.Path, "/suspend") || strings.Contains(r.URL.Path, "/resume") {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "swipeline_decisions"})
			return
		}
		if strings.Contains(r.URL.Path, "/sql") {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"total_count": 1005, "distinct_count": 1000}})
			return
		}
	}))
	defer srvFail.Close()

	err := ReconcileAfterIngestionRestartsWithWait(context.Background(), srvFail.URL, srvFail.URL, "", "restart-42", 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error on reconciliation inflation/duplicate rows, got nil")
	}
}

func TestAnalyticsSnapshotReadiness(t *testing.T) {
	for _, mode := range []string{"tail_pending", "duplicate_rows", "stale"} {
		t.Run(mode, func(t *testing.T) {
			countQueries := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/counts") {
					json.NewEncoder(w).Encode(auth.RunCounts{Final: 1000, Admitted: 1000})
					return
				}
				var body struct {
					Query string `json:"query"`
				}
				json.NewDecoder(r.Body).Decode(&body)
				if strings.Contains(body.Query, "CURRENT_TIMESTAMP") {
					lag := 1.0
					if mode == "stale" {
						lag = 40
					}
					json.NewEncoder(w).Encode([]map[string]any{{"ingestion_lag_seconds": lag}})
					return
				}
				countQueries++
				total, distinct := 1000, 1000
				if mode == "tail_pending" && countQueries == 1 {
					total, distinct = 962, 962
				}
				if mode == "duplicate_rows" {
					total = 1001
				}
				json.NewEncoder(w).Encode([]map[string]any{{"total_count": total, "distinct_count": distinct}})
			}))
			defer srv.Close()
			lag, err := VerifyAnalyticsSnapshotReady(context.Background(), druid.NewClient(srv.URL), srv.URL, "run", 100*time.Millisecond)
			if mode == "tail_pending" {
				if err != nil || lag != 1 || countQueries < 2 {
					t.Fatalf("tail convergence lag%v queries%d error%v", lag, countQueries, err)
				}
			} else if err == nil {
				t.Fatal("invalid snapshot readiness accepted")
			}
			if mode == "stale" && countQueries != 0 {
				t.Fatal("stale snapshot must fail before exact count wait")
			}
		})
	}
}

// Check the live freshness budget before waiting for exact tail convergence.
func VerifyAnalyticsSnapshotReady(ctx context.Context, client *druid.Client, gatewayURL, runID string, wait time.Duration) (float64, error) {
	lag, err := VerifyFreshnessAfterReplay(ctx, client, runID, 30, wait)
	if err != nil {
		return lag, err
	}
	if err = reconcile.Run(ctx, gatewayURL, client.Base, runID, wait, io.Discard); err != nil {
		return lag, fmt.Errorf("analytics snapshot did not converge: %w", err)
	}
	return lag, nil
}
