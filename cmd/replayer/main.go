// Command swipeline runs the preview service and is its command-line client.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/druid"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/inspect"
	"github.com/VivianSobers/swipeline/internal/preview"
	"github.com/VivianSobers/swipeline/internal/reconcile"
	"github.com/VivianSobers/swipeline/internal/replay"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func call(ctx context.Context, method, path string, body any, out io.Writer) error {
	base := os.Getenv("SWIPELINE_URL")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, payload)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(res.Body, 8192))
		return fmt.Errorf("HTTP %d: %s", res.StatusCode, data)
	}
	_, err = io.Copy(out, io.LimitReader(res.Body, 1<<20))
	return err
}

func run(ctx context.Context, args []string, out, errs io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: swipeline serve|replay|status|health|bootstrap|reconcile")
	}
	switch args[0] {
	case "serve":
		flags := flag.NewFlagSet("serve", flag.ContinueOnError)
		flags.SetOutput(errs)
		listen := flags.String("listen", ":8080", "control and metrics address")
		data := flags.String("data", "/data", "preview storage directory")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("unexpected serve arguments")
		}
		service, err := preview.New(*data)
		if err != nil {
			return err
		}
		requests, cancelRequests := context.WithCancel(ctx)
		var handlers sync.WaitGroup
		var admission sync.Mutex
		stopping := false
		handler := service.Handler()
		// No write timeout: a replay request legitimately runs for minutes.
		server := &http.Server{
			Addr:        *listen,
			BaseContext: func(net.Listener) context.Context { return requests },
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				admission.Lock()
				if stopping {
					admission.Unlock()
					http.Error(w, "service shutting down", http.StatusServiceUnavailable)
					return
				}
				handlers.Add(1)
				admission.Unlock()
				defer handlers.Done()
				handler.ServeHTTP(w, r)
			}),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384,
		}
		defer func() {
			// Prevent new work before waiting, including after a forced close.
			admission.Lock()
			stopping = true
			admission.Unlock()
			cancelRequests()
			_ = server.Close()
			handlers.Wait()
		}()
		done := make(chan error, 1)
		go func() { done <- server.ListenAndServe() }()
		select {
		case err := <-done:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdown); err != nil {
				return err
			}
			return nil
		}
	case "replay":
		flags := flag.NewFlagSet("replay", flag.ContinueOnError)
		flags.SetOutput(errs)
		source := flags.String("source", "synthetic", "input source")
		seed := flags.Uint64("seed", 42, "generator seed")
		tps := flags.Float64("tps", 50, "maximum offered events per second")
		events := flags.Int64("events", 6000, "events in one business day")
		day := flags.String("business-date", "2026-10-08", "UTC business day")
		namespace := flags.String("run-id", "demo-42", "stable business namespace")
		output := flags.String("output", "preview", "new output name")
		workers := flags.Int("workers", 8, "generator workers")
		queue := flags.Int("queue", 128, "total outstanding event bound")
		previewOnly := flags.Bool("preview", false, "write a file preview instead of authorizing")
		gatewayURL := flags.String("gateway", os.Getenv("SWIPELINE_GATEWAY_URL"), "authorization gateway URL")
		mode := flags.String("deadline-mode", "latency", "latency (35/50 ms) or throughput (80/100 ms) deadlines")
		lanes := flags.Int("lanes", 128, "bounded ordered card lanes")
		queueDir := flags.String("queue-dir", "", "persistent disk queue directory for lifecycle events")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *source != "synthetic" {
			return errors.New("only synthetic input is supported")
		}
		cfg := replay.Config{Generator: generator.Config{RunID: *namespace, Seed: *seed, Day: *day, Events: *events}, TPS: *tps, Workers: *workers, Queue: *queue}
		if err := cfg.Validate(); err != nil {
			return err
		}
		if !*previewOnly {
			base := *gatewayURL
			if base == "" {
				base = "http://127.0.0.1:8081"
			}
			c := &authorize.Client{Base: base, HTTP: &http.Client{Timeout: 2 * time.Second}}
			var opts []authorize.Option
			if *queueDir != "" {
				opts = append(opts, authorize.WithQueueDir(*queueDir))
			}
			summary, err := authorize.Run(ctx, cfg, c, *mode, *lanes, opts...)
			if encodeErr := json.NewEncoder(out).Encode(summary); err == nil {
				err = encodeErr
			}
			return err
		}
		if !contract.ValidName(*output) {
			return errors.New("invalid output name")
		}
		return call(ctx, "POST", "/v1/replays", preview.Request{Config: cfg, Output: *output}, out)
	case "status", "health":
		if len(args) != 1 {
			return errors.New("unexpected command arguments")
		}
		short, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		path := "/v1/status"
		if args[0] == "health" {
			path = "/healthz"
		}
		return call(short, "GET", path, nil, out)
	case "bootstrap":
		flags := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
		flags.SetOutput(errs)
		druidURL := flags.String("druid", "http://druid-router:8888", "Druid router URL")
		specPath := flags.String("spec", "/etc/swipeline/supervisor.json", "path to supervisor spec file")
		waitDuration := flags.Duration("wait", 5*time.Minute, "maximum wait duration for supervisor RUNNING")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("unexpected bootstrap arguments")
		}
		specData, err := os.ReadFile(*specPath)
		if err != nil {
			return fmt.Errorf("failed to read supervisor spec: %w", err)
		}
		client := druid.NewClient(*druidURL)
		created, err := client.Install(ctx, specData)
		if err != nil {
			return fmt.Errorf("failed to install supervisor: %w", err)
		}
		if created {
			fmt.Fprintln(out, "supervisor created")
		} else {
			fmt.Fprintln(out, "supervisor already installed")
		}

		var specDoc struct {
			Type string `json:"type"`
			Spec struct {
				DataSchema struct {
					DataSource string `json:"dataSource"`
				} `json:"dataSchema"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(specData, &specDoc); err != nil {
			return fmt.Errorf("failed to parse supervisor spec: %w", err)
		}
		id := specDoc.Spec.DataSchema.DataSource
		if id == "" {
			return errors.New("supervisor spec missing dataSource")
		}

		deadline := time.Now().Add(*waitDuration)
		var lastState string
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			state, err := client.State(ctx, id)
			if err == nil {
				lastState = state
				if state == "RUNNING" {
					fmt.Fprintf(out, "supervisor %s is RUNNING\n", id)
					return nil
				}
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		return fmt.Errorf("timeout waiting for supervisor %s to be RUNNING (current state: %s)", id, lastState)
	case "reconcile":
		flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
		flags.SetOutput(errs)
		runID := flags.String("run-id", "", "run ID to reconcile")
		gatewayURL := flags.String("gateway", "http://127.0.0.1:8080", "Gateway URL")
		druidURL := flags.String("druid", "http://druid-router:8888", "Druid router URL")
		waitDuration := flags.Duration("wait", 2*time.Minute, "maximum wait duration for reconciliation")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("unexpected reconcile arguments")
		}
		if *runID == "" {
			return errors.New("missing --run-id")
		}
		if *gatewayURL == "" {
			return errors.New("missing --gateway")
		}
		if *druidURL == "" {
			return errors.New("missing --druid")
		}
		return reconcile.Run(ctx, *gatewayURL, *druidURL, *runID, *waitDuration, out)
	case "inspect":
		flags := flag.NewFlagSet("inspect", flag.ContinueOnError)
		flags.SetOutput(errs)
		txID := flags.String("transaction-id", "", "transaction ID to inspect")
		sparkURL := flags.String("spark", os.Getenv("SWIPELINE_SPARK_URL"), "Spark worker endpoint or command")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("unexpected inspect arguments")
		}
		if *txID == "" {
			return errors.New("missing --transaction-id")
		}
		res, err := inspect.Inspect(ctx, *txID, *sparkURL)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}

}
