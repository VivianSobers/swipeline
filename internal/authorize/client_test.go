package authorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

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
