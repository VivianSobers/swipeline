// Command gateway serves synchronous card authorizations: it admits each
// request durably, relays it to the streaming job, arbitrates the reply
// against the deadline and recovers anything left pending.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VivianSobers/swipeline/internal/gateway"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/VivianSobers/swipeline/internal/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func listen() string {
	if addr := os.Getenv("GATEWAY_LISTEN"); addr != "" {
		return addr
	}
	return ":8081"
}

func run(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: gateway serve|health")
	}
	switch args[0] {
	case "health":
		c := http.Client{Timeout: 2 * time.Second}
		r, err := c.Get("http://127.0.0.1:8081/healthz")
		if err != nil {
			return err
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			return errors.New("unhealthy")
		}
		return nil
	case "serve":
		return serve(ctx)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(parent context.Context) error {
	db, err := store.Open(parent, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.DB.Close()
	if err = db.Migrate(parent); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s := gateway.New(ctx, db)
	// Pending deadlines from before a restart are resolved before serving.
	if err = s.Recover(ctx); err != nil {
		return err
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	p, err := transport.Producer(brokers)
	if err != nil {
		return err
	}
	defer p.Close()
	c, err := transport.Consumer(brokers, "swipeline-gateway-v1", func() { s.ConsumerReady.Store(true) })
	if err != nil {
		return err
	}
	defer c.Close()
	report := func(name string) func(error) {
		return func(err error) { fmt.Fprintf(os.Stderr, "%s: %v\n", name, err) }
	}
	failures := make(chan error, 2)
	go func() { failures <- fmt.Errorf("replies: %w", transport.Replies(ctx, s, c, p)) }()
	go func() { _ = transport.Relay(ctx, db, p, s.Outbox, report("relay")) }()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				call, stop := context.WithTimeout(ctx, time.Second)
				if err := s.Recover(call); err != nil {
					report("recovery")(err)
				}
				stop()
			}
		}
	}()
	server := &http.Server{Addr: listen(), Handler: s.Handler(), ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	go func() { failures <- server.ListenAndServe() }()
	select {
	case <-parent.Done():
	case err = <-failures:
	}
	// Answer in-flight requests while the queues still run, then stop them.
	s.ConsumerReady.Store(false)
	shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	_ = server.Shutdown(shutdown)
	cancel()
	if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
