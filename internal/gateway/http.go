package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

// Service is the authorization gateway. It has no endpoint that accepts
// proposals: those arrive only from the streaming job's reply topic.
type Service struct {
	DB            *store.Store
	Admissions    *Queue[store.Input]
	Finals        *Queue[store.Choice]
	ConsumerReady atomic.Bool

	mu      sync.Mutex
	waiters map[string][]chan *auth.Final
	runs    sync.Map // run ID -> auth.Run; manifests never change
	budget  map[string]*histogram
	counts  counters
}

// New starts the admission and finalization queues; they stop with ctx.
// Every committed final, whether from a proposal or a deadline, is handed to
// the requests waiting on its transaction.
func New(ctx context.Context, db *store.Store) *Service {
	s := &Service{DB: db, waiters: map[string][]chan *auth.Final{}, budget: map[string]*histogram{}}
	for mode := range targets {
		s.budget[mode] = newHistogram(0.005, 0.01, 0.02, 0.035, 0.05, 0.08, 0.1, 0.25, 0.5, 1)
	}
	s.Admissions = NewQueue(ctx, 1024, db.Admit)
	s.Finals = NewQueue(ctx, 1024, s.finalize)
	return s
}

// finalize commits choices, counts each newly committed final once and
// hands finals to waiting requests.
func (s *Service) finalize(ctx context.Context, in []store.Choice) ([]store.Row, error) {
	rows, err := s.DB.Finalize(ctx, in)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r.Fresh {
			s.counts.add(r.Final)
		}
	}
	s.deliver(rows)
	return rows, nil
}

// watch registers interest in id's final until the returned func is called.
func (s *Service) watch(id string) (<-chan *auth.Final, func()) {
	ch := make(chan *auth.Final, 1)
	s.mu.Lock()
	s.waiters[id] = append(s.waiters[id], ch)
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		list := s.waiters[id]
		for i, c := range list {
			if c == ch {
				list = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(s.waiters, id)
		} else {
			s.waiters[id] = list
		}
	}
}

func (s *Service) deliver(rows []store.Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rows {
		if r.Final == nil {
			continue
		}
		for _, ch := range s.waiters[r.Request.TransactionID] {
			select {
			case ch <- r.Final:
			default:
			}
		}
	}
}

func (s *Service) manifest(ctx context.Context, id string) (auth.Run, error) {
	if r, ok := s.runs.Load(id); ok {
		return r.(auth.Run), nil
	}
	r, err := s.DB.Manifest(ctx, id)
	if err == nil {
		s.runs.Store(id, r)
	}
	return r, err
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
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	m.HandleFunc("GET /metrics", s.metrics)
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.ConsumerReady.Load() || s.DB.DB.PingContext(r.Context()) != nil {
			reply(w, http.StatusServiceUnavailable, map[string]string{"status": "recovering"})
			return
		}
		reply(w, http.StatusOK, map[string]string{"status": "ready"})
	})
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
	m.HandleFunc("POST /v1/authorizations", s.authorize)
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

// authorize admits one request and answers within its deadline mode's
// response budget, measured from entry, with a committed final only. When no
// final can be confirmed in time the answer is 503 RETRY_SAME_ID; a retry
// with the same ID returns the stored outcome.
func (s *Service) authorize(w http.ResponseWriter, r *http.Request) {
	entered := time.Now()
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
	if err != nil {
		failure(w, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	e, err := auth.Decode(data, r.Header.Get("Idempotency-Key"))
	if err != nil {
		failure(w, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	run, err := s.manifest(r.Context(), e.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		failure(w, http.StatusUnprocessableEntity, "UNKNOWN_RUN")
		return
	} else if err != nil {
		failure(w, http.StatusServiceUnavailable, "RETRY_SAME_ID")
		return
	}
	if run.Check(e) != nil {
		failure(w, http.StatusConflict, "RUN_CONFLICT")
		return
	}
	// Every answer from here on, including a missed budget, is observed.
	defer func() { s.budget[run.Mode].observe(time.Since(entered).Seconds()) }()
	ctx, cancel := context.WithDeadline(r.Context(), entered.Add(targets[run.Mode]))
	defer cancel()
	finals, stop := s.watch(e.TransactionID)
	defer stop()
	row, err := s.Admissions.Submit(ctx, store.Input{Event: e, Received: entered.UnixMilli()}, entered.Add(2*time.Millisecond))
	if err != nil {
		var pg *pgconn.PgError
		switch {
		case errors.Is(err, ErrBusy) || errors.Is(err, store.ErrFull):
			failure(w, http.StatusTooManyRequests, "RETRY_SAME_ID")
		case errors.As(err, &pg) && pg.Code == "23505":
			failure(w, http.StatusConflict, "PAYLOAD_CONFLICT")
		default:
			failure(w, http.StatusServiceUnavailable, "RETRY_SAME_ID")
		}
		return
	}
	if row.Final != nil {
		reply(w, http.StatusOK, row.Final)
		return
	}
	timer := time.NewTimer(time.Until(time.UnixMilli(row.Request.Scoring)))
	defer timer.Stop()
	for {
		select {
		case f := <-finals:
			reply(w, http.StatusOK, f)
			return
		case <-timer.C:
			row, err = s.Finals.Submit(ctx, store.Choice{ID: e.TransactionID}, time.Now())
			if err != nil {
				failure(w, http.StatusServiceUnavailable, "RETRY_SAME_ID")
				return
			}
			if row.Final != nil {
				reply(w, http.StatusOK, row.Final)
				return
			}
			// The database clock has not yet passed the deadline.
			timer.Reset(time.Millisecond)
		case <-ctx.Done():
			failure(w, http.StatusServiceUnavailable, "RETRY_SAME_ID")
			return
		}
	}
}
