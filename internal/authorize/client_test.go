package authorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
)

func TestRegisterAcceptsOnlyCreated(t *testing.T) {
	status := http.StatusCreated
	var got auth.Run
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(status)
	}))
	defer s.Close()
	c := Client{Base: s.URL + "/", HTTP: s.Client()}
	if err := c.Register(context.Background(), auth.Run{ID: "run"}); err != nil || got.ID != "run" {
		t.Fatal(err, got)
	}
	status = http.StatusConflict
	if c.Register(context.Background(), auth.Run{}) == nil {
		t.Fatal("conflict ignored")
	}
	s.Close()
	if c.Register(context.Background(), auth.Run{}) == nil {
		t.Fatal("unreachable gateway ignored")
	}
}

func final(id string) auth.Final {
	return auth.Final{Request: auth.Request{Event: contract.Event{Business: contract.Business{TransactionID: id}}}, Decision: "DECLINE", DecisionID: "final"}
}

func TestLostReplyQueriesSameID(t *testing.T) {
	var gets, posts atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			if r.Header.Get("Idempotency-Key") != "id" {
				t.Error("missing idempotency key")
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		gets.Add(1)
		_ = json.NewEncoder(w).Encode(final("id"))
	}))
	defer s.Close()
	c := Client{Base: s.URL, HTTP: s.Client()}
	f, err := c.Send(context.Background(), contract.Event{Business: contract.Business{TransactionID: "id"}})
	if err != nil || f.DecisionID != "final" || gets.Load() != 1 || posts.Load() != 1 {
		t.Fatal(f, err, gets.Load(), posts.Load())
	}
}

func TestPendingRetriesSameID(t *testing.T) {
	var posts atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if posts.Add(1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(final("id"))
	}))
	defer s.Close()
	c := Client{Base: s.URL, HTTP: s.Client()}
	if f, err := c.Send(context.Background(), contract.Event{Business: contract.Business{TransactionID: "id"}}); err != nil || f.DecisionID != "final" || posts.Load() != 3 {
		t.Fatal(f, err, posts.Load())
	}
}

func TestSendRejectsConflictsAndForeignFinals(t *testing.T) {
	status := http.StatusConflict
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if status == http.StatusOK {
			_ = json.NewEncoder(w).Encode(final("other"))
		}
	}))
	defer s.Close()
	c := Client{Base: s.URL, HTTP: s.Client()}
	e := contract.Event{Business: contract.Business{TransactionID: "id"}}
	if _, err := c.Send(context.Background(), e); err == nil {
		t.Fatal("conflict accepted")
	}
	status = http.StatusOK
	if _, err := c.Send(context.Background(), e); err == nil {
		t.Fatal("final for another transaction accepted")
	}
}

func TestSendWaitsForDurableFinal(t *testing.T) {
	start := time.Now()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if time.Since(start) < 1500*time.Millisecond {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		_ = json.NewEncoder(w).Encode(final("id"))
	}))
	defer s.Close()
	c := Client{Base: s.URL, HTTP: s.Client()}
	f, err := c.Send(context.Background(), contract.Event{Business: contract.Business{TransactionID: "id"}})
	if err != nil || f.DecisionID != "final" {
		t.Fatalf("durable final not resolved: %v %v", f, err)
	}
}

func TestSendBudgetBoundsUnresolved(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer s.Close()
	c := Client{Base: s.URL, HTTP: s.Client()}
	// Reflection lets the regression run against the client before the field exists.
	field := reflect.ValueOf(&c).Elem().FieldByName("RetryBudget")
	if !field.IsValid() {
		t.Fatal("client has no configurable retry budget")
	}
	budget := 80 * time.Millisecond
	field.SetInt(int64(budget))
	start := time.Now()
	_, err := c.Send(context.Background(), contract.Event{Business: contract.Business{TransactionID: "id"}})
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "unresolved authorization id:") {
		t.Fatalf("got %v", err)
	}
	if elapsed < budget || elapsed > budget+500*time.Millisecond {
		t.Fatalf("budget=%s elapsed=%s", budget, elapsed)
	}
}

func TestSendLostReplyPollsKnownPending(t *testing.T) {
	var posts, gets atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		if gets.Add(1) < 3 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		_ = json.NewEncoder(w).Encode(final("id"))
	}))
	defer s.Close()
	c := Client{Base: s.URL, HTTP: s.Client()}
	_, err := c.Send(context.Background(), contract.Event{Business: contract.Business{TransactionID: "id"}})
	if err != nil || posts.Load() != 1 {
		t.Fatalf("known pending transaction resubmitted: posts=%d err=%v", posts.Load(), err)
	}
}
