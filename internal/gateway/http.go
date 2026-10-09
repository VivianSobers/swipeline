package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/store"
)

// Service is the authorization gateway. It has no endpoint that accepts
// proposals: those arrive only from the streaming job's reply topic.
type Service struct {
	DB            *store.Store
	Admissions    *Queue[store.Input]
	Finals        *Queue[store.Choice]
	ConsumerReady atomic.Bool
}

// New starts the admission and finalization queues; they stop with ctx.
func New(ctx context.Context, db *store.Store) *Service {
	return &Service{DB: db, Admissions: NewQueue(ctx, 1024, db.Admit), Finals: NewQueue(ctx, 1024, db.Finalize)}
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func failure(w http.ResponseWriter, status int, code string) {
	reply(w, status, map[string]string{"error": code})
}

// body decodes exactly one JSON object of at most 16 KiB into v.
func body(w http.ResponseWriter, r *http.Request, v any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("one JSON object required")
	}
	return nil
}

// Handler routes the gateway API.
func (s *Service) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /v1/runs", func(w http.ResponseWriter, r *http.Request) {
		var run auth.Run
		if body(w, r, &run) != nil || run.Validate() != nil {
			failure(w, http.StatusBadRequest, "INVALID_RUN")
			return
		}
		if err := s.DB.Register(r.Context(), run); errors.Is(err, store.ErrRunConflict) {
			failure(w, http.StatusConflict, "RUN_CONFLICT")
			return
		} else if err != nil {
			failure(w, http.StatusServiceUnavailable, "RETRY")
			return
		}
		reply(w, http.StatusCreated, run)
	})
	m.HandleFunc("GET /v1/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		run, err := s.DB.Manifest(r.Context(), r.PathValue("id"))
		if errors.Is(err, sql.ErrNoRows) {
			failure(w, http.StatusNotFound, "UNKNOWN_RUN")
			return
		} else if err != nil {
			failure(w, http.StatusServiceUnavailable, "RETRY")
			return
		}
		reply(w, http.StatusOK, run)
	})
	m.HandleFunc("GET /v1/authorizations/{id}", func(w http.ResponseWriter, r *http.Request) {
		row, err := s.DB.Lookup(r.Context(), r.PathValue("id"))
		if errors.Is(err, sql.ErrNoRows) {
			failure(w, http.StatusNotFound, "UNKNOWN_ID")
			return
		} else if err != nil {
			failure(w, http.StatusServiceUnavailable, "RETRY_SAME_ID")
			return
		}
		if row.Final == nil {
			reply(w, http.StatusAccepted, row.Request)
			return
		}
		reply(w, http.StatusOK, row.Final)
	})
	return m
}
