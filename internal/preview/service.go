// Package preview runs one replay at a time into a named JSON Lines file,
// publishes it atomically, and serves status, files and Prometheus metrics.
package preview

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/replay"
)

// Request asks for one generated run written to previews/<Output>.
type Request struct {
	Config replay.Config `json:"config"`
	Output string        `json:"output"`
}

// Status is idle, running, complete or failed, with live progress.
type Status struct {
	State    string          `json:"state"`
	Output   string          `json:"output"`
	Error    string          `json:"error,omitempty"`
	Config   replay.Config   `json:"config"`
	Progress replay.Snapshot `json:"progress"`
}

type Service struct {
	root   string
	runMu  sync.Mutex
	mu     sync.Mutex
	status Status
	stats  *replay.Stats
	past   replay.Snapshot
}

// New prepares root/runs (namespace identities) and root/previews.
func New(root string) (*Service, error) {
	for _, sub := range []string{"runs", "previews"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0700); err != nil {
			return nil, err
		}
	}
	return &Service{root: root, status: Status{State: "idle"}, stats: &replay.Stats{}}, nil
}

func (s *Service) snapshot() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.status
	v.Progress = s.stats.Snapshot()
	return v
}

func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	respond(w, code, map[string]string{"error": err.Error()})
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, s.snapshot()) })
	mux.HandleFunc("POST /v1/replays", s.replay)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /v1/previews/{name}", func(w http.ResponseWriter, r *http.Request) { s.download(w, r, "events.jsonl") })
	mux.HandleFunc("GET /v1/previews/{name}/manifest", func(w http.ResponseWriter, r *http.Request) { s.download(w, r, "manifest.json") })
	return mux
}

func (s *Service) download(w http.ResponseWriter, r *http.Request, file string) {
	name := r.PathValue("name")
	if !contract.ValidName(name) {
		http.Error(w, "invalid name", 400)
		return
	}
	dir := filepath.Join(s.root, "previews", name)
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		http.NotFound(w, r)
		return
	}
	if file == "events.jsonl" {
		w.Header().Set("Content-Type", "application/x-ndjson")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	http.ServeFile(w, r, filepath.Join(dir, file))
}

// identity records the run namespace's business identity on first use and
// rejects later requests that reuse the namespace with different content.
func (s *Service) identity(req Request) error {
	name := filepath.Join(s.root, "runs", req.Config.Generator.RunID+".json")
	want := req.Config.Generator.Identity()
	data, err := os.ReadFile(name)
	if err == nil {
		var have struct {
			Identity string `json:"identity"`
		}
		if json.Unmarshal(data, &have) != nil || have.Identity != want {
			return errors.New("run namespace configuration conflict")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	data, err = json.Marshal(struct {
		Identity string `json:"identity"`
		Config   any    `json:"generator"`
	}{want, req.Config.Generator})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(name), ".identity-")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer os.Remove(temp)
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(temp, name)
}

func (s *Service) replay(w http.ResponseWriter, r *http.Request) {
	var req Request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		fail(w, 400, err)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		fail(w, 400, errors.New("one JSON request required"))
		return
	}
	if err := req.Config.Validate(); err != nil {
		fail(w, 400, err)
		return
	}
	if !contract.ValidName(req.Output) {
		fail(w, 400, errors.New("invalid output name"))
		return
	}
	if !s.runMu.TryLock() {
		fail(w, 409, errors.New("one active preview at a time"))
		return
	}
	defer s.runMu.Unlock()
	final := filepath.Join(s.root, "previews", req.Output)
	if _, err := os.Stat(final); err == nil {
		fail(w, 409, errors.New("output already exists"))
		return
	} else if !os.IsNotExist(err) {
		fail(w, 500, err)
		return
	}
	if err := s.identity(req); err != nil {
		fail(w, 409, err)
		return
	}
	s.mu.Lock()
	old := s.stats.Snapshot()
	s.past.Offered += old.Offered
	s.past.Admitted += old.Admitted
	s.past.Completed += old.Completed
	stats := &replay.Stats{}
	s.stats = stats
	s.status = Status{State: "running", Output: req.Output, Config: req.Config}
	s.mu.Unlock()
	err := s.write(r, req, final, stats)
	s.mu.Lock()
	s.status.State = "complete"
	if err != nil {
		s.status.State = "failed"
		s.status.Error = err.Error()
	}
	s.mu.Unlock()
	if err != nil {
		fail(w, 500, err)
		return
	}
	respond(w, 200, s.snapshot())
}

// write replays into a hidden temporary directory and renames it into place
// only after the events and manifest are synced, so a preview is either
// complete or absent.
func (s *Service) write(r *http.Request, req Request, final string, stats *replay.Stats) error {
	temp, err := os.MkdirTemp(filepath.Join(s.root, "previews"), ".pending-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	file, err := os.OpenFile(filepath.Join(temp, "events.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	err = replay.Run(r.Context(), req.Config, func(e contract.Event) error { return encoder.Encode(e) }, stats)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	manifest := struct {
		Request                Request         `json:"request"`
		Identity               string          `json:"identity"`
		ClockCompressionTarget float64         `json:"clock_compression_target"`
		Progress               replay.Snapshot `json:"progress"`
		Mode                   string          `json:"mode"`
	}{req, req.Config.Generator.Identity(), 86400 * req.Config.TPS / float64(req.Config.Generator.Events), stats.Snapshot(), "preview-only"}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	mf, err := os.OpenFile(filepath.Join(temp, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = mf.Write(data); err == nil {
		err = mf.Sync()
	}
	closeErr = mf.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = r.Context().Err(); err != nil {
		return err
	}
	return os.Rename(temp, final)
}

func (s *Service) metrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	v := s.stats.Snapshot()
	past := s.past
	status := s.status
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP swipeline_replayer_up Preview control service is available.")
	fmt.Fprintln(w, "# TYPE swipeline_replayer_up gauge\nswipeline_replayer_up 1")
	for _, m := range []struct {
		name  string
		value int64
	}{
		{"offered", past.Offered + v.Offered}, {"admitted", past.Admitted + v.Admitted}, {"completed", past.Completed + v.Completed},
	} {
		fmt.Fprintf(w, "# HELP swipeline_replayer_%s_total Preview records since process start.\n# TYPE swipeline_replayer_%s_total counter\nswipeline_replayer_%s_total %d\n", m.name, m.name, m.name, m.value)
	}
	active := 0
	if status.State == "running" {
		active = 1
	}
	gauges := []struct {
		name  string
		value float64
	}{
		{"active", float64(active)}, {"in_flight", float64(v.InFlight)}, {"peak_in_flight", float64(v.PeakInFlight)},
		{"offered_tps", v.OfferedTPS}, {"admitted_tps", v.AdmittedTPS}, {"achieved_tps", v.AchievedTPS},
		{"target_tps", status.Config.TPS}, {"scheduler_lag_seconds", v.SchedulerLagSeconds}, {"abandoned", float64(v.Abandoned)},
	}
	for _, m := range gauges {
		fmt.Fprintf(w, "# HELP swipeline_replayer_%s Current or most recent preview run.\n# TYPE swipeline_replayer_%s gauge\nswipeline_replayer_%s %g\n", m.name, m.name, m.name, m.value)
	}
}
