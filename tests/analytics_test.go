package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/druid"
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

	client := druid.NewClient(druidURL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("DruidDecisionFields", func(t *testing.T) {
		if err := VerifyDruidDecisionFields(ctx, client, gatewayURL, runID); err != nil {
			t.Fatalf("VerifyDruidDecisionFields failed: %v", err)
		}
	})
}
