package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/store"
)

func service(t *testing.T) (*Service, auth.Run, contract.Event) {
	t.Helper()
	dsn := os.Getenv("SWIPELINE_DB")
	if dsn == "" {
		t.Skip("run make db-test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DB.Close() })
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := contract.ID(t.Name(), time.Now().String())[:24]
	g, err := generator.New(generator.Config{RunID: id, Seed: 42, Day: "2026-10-08", Events: 1000})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.At(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := auth.Run{ID: id, Identity: id, Day: e.AuthorizationDate, Rule: e.RuleVersion, Reference: e.ReferenceVersion,
		Mode: "latency", TPS: 50, Compression: 720, Concurrency: 128, Partitioner: "sha256-json-v1", Partitions: 4}
	return New(ctx, db), r, e
}

func invoke(s *Service, method, path string, v any, key string) *httptest.ResponseRecorder {
	var payload []byte
	switch b := v.(type) {
	case nil:
	case []byte:
		payload = b
	default:
		payload, _ = json.Marshal(v)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(payload))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestRegisterHTTP(t *testing.T) {
	s, r, _ := service(t)
	for range 2 {
		if w := invoke(s, http.MethodPost, "/v1/runs", r, ""); w.Code != http.StatusCreated {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	changed := r
	changed.Identity = "different"
	if w := invoke(s, http.MethodPost, "/v1/runs", changed, ""); w.Code != http.StatusConflict {
		t.Fatal("changed manifest", w.Code)
	}
	invalid := r
	invalid.Partitions = 6
	if w := invoke(s, http.MethodPost, "/v1/runs", invalid, ""); w.Code != http.StatusBadRequest {
		t.Fatal("invalid manifest", w.Code)
	}
	data, _ := json.Marshal(r)
	if w := invoke(s, http.MethodPost, "/v1/runs", append(data, data...), ""); w.Code != http.StatusBadRequest {
		t.Fatal("trailing data", w.Code)
	}
}

func TestRunManifestHTTP(t *testing.T) {
	s, r, _ := service(t)
	if w := invoke(s, http.MethodGet, "/v1/runs/"+r.ID, nil, ""); w.Code != http.StatusNotFound {
		t.Fatal("unknown run", w.Code)
	}
	if err := s.DB.Register(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	w := invoke(s, http.MethodGet, "/v1/runs/"+r.ID, nil, "")
	var got auth.Run
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil || got != r {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestStatusHTTP(t *testing.T) {
	s, r, e := service(t)
	ctx := context.Background()
	path := "/v1/authorizations/" + e.TransactionID
	if w := invoke(s, http.MethodGet, path, nil, ""); w.Code != http.StatusNotFound {
		t.Fatal("unknown", w.Code)
	}
	if err := s.DB.Register(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Admit(ctx, []store.Input{{Event: e, Received: time.Now().Add(-time.Second).UnixMilli()}}); err != nil {
		t.Fatal(err)
	}
	var pending auth.Request
	if w := invoke(s, http.MethodGet, path, nil, ""); w.Code != http.StatusAccepted || json.Unmarshal(w.Body.Bytes(), &pending) != nil || pending.Event != e {
		t.Fatal("pending", w.Code, w.Body.String())
	}
	if _, err := s.DB.Finalize(ctx, []store.Choice{{ID: e.TransactionID}}); err != nil {
		t.Fatal(err)
	}
	var final auth.Final
	if w := invoke(s, http.MethodGet, path, nil, ""); w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &final) != nil || final.Source != "STAND_IN" {
		t.Fatal("final", w.Code, w.Body.String())
	}
}

func authorizeHTTP(t *testing.T, s *Service, e contract.Event) (int, auth.Final) {
	t.Helper()
	w := invoke(s, http.MethodPost, "/v1/authorizations", e, e.TransactionID)
	var f auth.Final
	if w.Code == http.StatusOK && json.Unmarshal(w.Body.Bytes(), &f) != nil {
		t.Fatal(w.Body.String())
	}
	return w.Code, f
}

func TestHTTPTimeoutFinalAndRetry(t *testing.T) {
	s, run, e := service(t)
	ctx := context.Background()
	if err := s.DB.Register(ctx, run); err != nil {
		t.Fatal(err)
	}
	code, first := authorizeHTTP(t, s, e)
	if code != http.StatusOK && code != http.StatusServiceUnavailable {
		t.Fatal(code)
	}
	row, err := s.DB.Lookup(ctx, e.TransactionID)
	if err != nil {
		t.Fatal("admission not durable:", err)
	}
	if row.Final == nil {
		time.Sleep(60 * time.Millisecond)
		if _, err = s.DB.Finalize(ctx, []store.Choice{{ID: e.TransactionID}}); err != nil {
			t.Fatal(err)
		}
	}
	code, f := authorizeHTTP(t, s, e)
	if code != http.StatusOK || f.Decision != "DECLINE" || f.Source != "STAND_IN" || f.Reason != "SCORING_TIMEOUT" {
		t.Fatal(code, f)
	}
	if first.DecisionID != "" && first.DecisionID != f.DecisionID {
		t.Fatal("retry changed the outcome")
	}
	changed := e.Business
	changed.AmountMinor++
	conflict, err := contract.New(changed, 1)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ = authorizeHTTP(t, s, conflict); code != http.StatusConflict {
		t.Fatal("payload conflict", code)
	}
}

func TestTimelyProposalAnswersRequest(t *testing.T) {
	s, run, e := service(t)
	ctx := context.Background()
	run.Mode = "throughput"
	if err := s.DB.Register(ctx, run); err != nil {
		t.Fatal(err)
	}
	go func() {
		deadline := time.Now().Add(80 * time.Millisecond)
		for time.Now().Before(deadline) {
			row, err := s.DB.Lookup(ctx, e.TransactionID)
			if err == nil {
				p := auth.Proposal{Request: row.Request, ProposalID: contract.ID(e.TransactionID, "proposal"), Decision: "APPROVE",
					Reason: "RULES_CLEAR", Flags: []string{}, Usual: 100, Scored: row.Request.Received}
				_, _ = s.Finals.Submit(ctx, store.Choice{ID: e.TransactionID, Proposal: &p}, time.Now())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	code, f := authorizeHTTP(t, s, e)
	if code != http.StatusOK || f.Source != "FLINK" || f.Decision != "APPROVE" {
		t.Fatal(code, f)
	}
}

func TestAuthorizationRejectsBadRequests(t *testing.T) {
	s, run, e := service(t)
	if code, _ := authorizeHTTP(t, s, e); code != http.StatusUnprocessableEntity {
		t.Fatal("unregistered run", code)
	}
	if err := s.DB.Register(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if w := invoke(s, http.MethodPost, "/v1/authorizations", e, contract.ID("other")); w.Code != http.StatusBadRequest {
		t.Fatal("mismatched idempotency key", w.Code)
	}
	if w := invoke(s, http.MethodPost, "/v1/authorizations", []byte("{"), e.TransactionID); w.Code != http.StatusBadRequest {
		t.Fatal("malformed body", w.Code)
	}
	if _, err := s.DB.Lookup(context.Background(), e.TransactionID); err == nil {
		t.Fatal("rejected request was admitted")
	}
}

func TestReadinessRequiresProposalConsumer(t *testing.T) {
	s, _, _ := service(t)
	if invoke(s, http.MethodGet, "/readyz", nil, "").Code != http.StatusServiceUnavailable {
		t.Fatal("uninitialized consumer ready")
	}
	s.ConsumerReady.Store(true)
	if invoke(s, http.MethodGet, "/readyz", nil, "").Code != http.StatusOK {
		t.Fatal("ready dependencies unavailable")
	}
	_ = s.DB.DB.Close()
	if invoke(s, http.MethodGet, "/readyz", nil, "").Code != http.StatusServiceUnavailable {
		t.Fatal("ready without a database")
	}
}

// Liveness reports only that the process serves HTTP; a database outage must
// not get the gateway restarted while it holds pending deadlines.
func TestGatewayLiveness(t *testing.T) {
	s, _, _ := service(t)
	_ = s.DB.DB.Close()
	if invoke(s, http.MethodGet, "/healthz", nil, "").Code != http.StatusOK {
		t.Fatal("no liveness")
	}
}

func TestBudgetMetrics(t *testing.T) {
	s, run, e := service(t)
	if err := s.DB.Register(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	authorizeHTTP(t, s, e)
	w := invoke(s, http.MethodGet, "/metrics", nil, "")
	if w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	for _, line := range []string{
		`swipeline_gateway_budget_duration_seconds_count{deadline_mode="latency"} 1`,
		`swipeline_gateway_budget_duration_seconds_count{deadline_mode="throughput"} 0`,
		`swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.05"} `,
		`swipeline_gateway_budget_duration_seconds_bucket{deadline_mode="latency",le="0.1"} `,
		`swipeline_gateway_latency_target_seconds{deadline_mode="latency"} 0.05`,
		`swipeline_gateway_latency_target_seconds{deadline_mode="throughput"} 0.1`,
	} {
		if !strings.Contains(w.Body.String(), line) {
			t.Fatalf("missing %q in\n%s", line, w.Body.String())
		}
	}
}

func TestBacklogMetrics(t *testing.T) {
	s, _, _ := service(t)
	body := invoke(s, http.MethodGet, "/metrics", nil, "").Body.String()
	for _, prefix := range []string{
		"swipeline_gateway_database_up 1\n",
		"swipeline_gateway_pending_authorizations ",
		"swipeline_gateway_outbox_unsent ",
		"swipeline_gateway_outbox_oldest_age_seconds ",
	} {
		if !strings.Contains(body, "\n"+prefix) {
			t.Fatalf("missing %q", prefix)
		}
	}
	_ = s.DB.DB.Close()
	body = invoke(s, http.MethodGet, "/metrics", nil, "").Body.String()
	if !strings.Contains(body, "\nswipeline_gateway_database_up 0\n") || strings.Contains(body, "outbox_oldest_age_seconds ") {
		t.Fatal("backlog reported without a database")
	}
}
