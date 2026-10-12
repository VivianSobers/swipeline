package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VivianSobers/swipeline/internal/dashboard"
)

func TestDashboardUsesImportedStructuredCharts(t *testing.T) {
	for _, failure := range []string{"", "missing context", "chart error", "missing chart", "invalid query", "missing datasource", "empty data", "wrong rate", "missing csrf", "changed metrics", "changed columns", "changed orderby"} {
		t.Run(failure, func(t *testing.T) {
			var queried atomic.Int32
			var savedContext atomic.Value
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/login"):
					json.NewEncoder(w).Encode(map[string]any{"access_token": "token"})
				case strings.Contains(r.URL.Path, "csrf_token"):
					http.SetCookie(w, &http.Cookie{Name: "session", Value: "csrf-session", Path: "/"})
					csrf := "csrf"
					if failure == "missing csrf" {
						csrf = ""
					}
					json.NewEncoder(w).Encode(map[string]any{"result": csrf})
				case strings.HasSuffix(r.URL.Path, "/charts"):
					charts := []any{}
					for i, d := range dashboard.ChartDefinitions {
						if failure == "missing chart" && i == 0 {
							continue
						}
						charts = append(charts, map[string]any{"id": i + 1, "slice_name": d.SliceName})
					}
					json.NewEncoder(w).Encode(map[string]any{"result": charts})
				case strings.Contains(r.URL.Path, "/dashboard/"):
					json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"id": 9}})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/chart/"):
					var id int
					if _, err := fmt.Sscanf(r.URL.Path, "/api/v1/chart/%d", &id); err != nil {
						http.Error(w, "invalid chart ID", 400)
						return
					}
					if id < 1 || id > len(dashboard.ChartDefinitions) {
						http.NotFound(w, r)
						return
					}
					q := dashboard.ChartQuery(dashboard.ChartDefinitions[id-1].Name)
					// A saved filter must survive alongside the generated chart query.
					q["filters"] = append(q["filters"].([]any), map[string]any{"col": "decision", "op": "==", "val": "APPROVE"})
					qc, marshalErr := json.Marshal(map[string]any{"datasource": map[string]any{"id": dashboard.DatasetUUID, "type": "table"}, "queries": []any{q}, "result_type": "full"})
					if marshalErr != nil {
						t.Error(marshalErr)
						http.Error(w, "fixture encoding failed", 500)
						return
					}
					savedContext.Store(string(qc))
					if failure == "missing context" {
						qc = []byte("null")
					}
					if failure == "invalid query" {
						qc = []byte(`{"queries":[null]}`)
					}
					datasourceID := 73
					if failure == "missing datasource" {
						datasourceID = 0
					}
					json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"query_context": string(qc), "datasource_id": datasourceID, "datasource_type": "table"}})
				case strings.Contains(r.URL.Path, "/chart/data"):
					if cookie, err := r.Cookie("session"); err != nil || cookie.Value != "csrf-session" || r.Header.Get("X-CSRFToken") != "csrf" {
						http.Error(w, "missing csrf session", 400)
						return
					}
					var payload map[string]any
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
						http.Error(w, "invalid request JSON", 400)
						return
					}
					ds, _ := payload["datasource"].(map[string]any)
					if ds["id"] != float64(73) {
						http.Error(w, "expected imported dataset ID", 400)
						return
					}
					queries, _ := payload["queries"].([]any)
					if len(queries) != 1 {
						http.Error(w, "missing query", 400)
						return
					}
					q, ok := queries[0].(map[string]any)
					if !ok {
						t.Errorf("query is not an object: %T", queries[0])
						http.Error(w, "invalid query", 400)
						return
					}
					if _, bad := q["sql"]; bad {
						http.Error(w, "unsupported raw SQL", 400)
						return
					}
					scoped, originalFilter := false, false
					filters, ok := q["filters"].([]any)
					if !ok {
						t.Error("missing filter list")
						http.Error(w, "missing filters", 400)
						return
					}
					for _, raw := range filters {
						f, ok := raw.(map[string]any)
						if !ok {
							t.Error("filter is not an object")
							http.Error(w, "invalid filter", 400)
							return
						}
						if f["col"] == "decision" && f["val"] == "APPROVE" {
							originalFilter = true
						}
						if f["col"] == "run_id" && f["op"] == "==" && f["val"] == "installed-42" {
							scoped = true
						}
					}
					if !scoped || !originalFilter {
						http.Error(w, "missing run filter", 400)
						return
					}
					if strings.HasPrefix(failure, "changed ") {
						q[strings.TrimPrefix(failure, "changed ")] = []any{"replacement"}
					}
					if err := verifySavedChartQueries(savedContext.Load().(string), queries, "installed-42"); err != nil {
						http.Error(w, err.Error(), 400)
						return
					}
					queried.Add(1)
					status := "success"
					if failure == "chart error" {
						status = "failed"
					}
					rate := 0.85
					if failure == "wrong rate" {
						rate = 0.5
					}
					rows := []any{map[string]any{"approval_rate": rate}}
					if failure == "empty data" {
						rows = nil
					}
					json.NewEncoder(w).Encode(map[string]any{"result": []any{map[string]any{"data": rows, "status": status}}})
				case strings.HasSuffix(r.URL.Path, "/counts"):
					fmt.Fprint(w, `{"final":100}`)
				case strings.HasSuffix(r.URL.Path, "/metrics"):
					fmt.Fprintln(w, `swipeline_gateway_final_decisions_total{decision="APPROVE"} 85`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			err := VerifyDashboardChartsThroughSuperset(context.Background(), srv.URL, "secret", srv.URL, "", "installed-42")
			if failure == "" {
				if err != nil {
					t.Fatal(err)
				}
				if count := queried.Load(); count != 6 {
					t.Fatalf("queried %d charts", count)
				}
			} else {
				want := map[string]string{"missing context": "no valid saved query context", "chart error": "returned status", "missing chart": "dashboard missing chart", "invalid query": "invalid query in chart", "missing datasource": "no imported table datasource", "empty data": "no data", "wrong rate": "approval-rate mismatch", "missing csrf": "no CSRF token", "changed metrics": "changed saved chart query", "changed columns": "changed saved chart query", "changed orderby": "changed saved chart query"}[failure]
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("expected %q failure, got %v", want, err)
				}
			}
		})
	}
}

// Compare the complete saved query, including metrics, columns, ordering and
// limits. Only adding the run filter is allowed inside a query object.
func verifySavedChartQueries(saved string, incoming []any, runID string) error {
	var context map[string]any
	if err := json.Unmarshal([]byte(saved), &context); err != nil {
		return err
	}
	expected, ok := context["queries"].([]any)
	if !ok {
		return fmt.Errorf("saved query missing")
	}
	for _, raw := range expected {
		query, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("saved query invalid")
		}
		filters, _ := query["filters"].([]any)
		query["filters"] = append(filters, map[string]any{"col": "run_id", "op": "==", "val": runID})
	}
	if !reflect.DeepEqual(expected, incoming) {
		return fmt.Errorf("changed saved chart query")
	}
	return nil
}
