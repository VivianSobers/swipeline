package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/gateway"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRepliesArbitrateAndQuarantine(t *testing.T) {
	s, brokers := live(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	row := admitted(t, s, time.Now().Add(time.Minute))
	timely := auth.Proposal{Request: row.Request, ProposalID: contract.ID(row.Request.TransactionID, "proposal"), Decision: "APPROVE",
		Reason: "RULES_CLEAR", Flags: []string{}, Usual: 100, Scored: row.Request.Received}
	orphan := timely
	orphan.TransactionID = contract.ID(t.Name(), time.Now().String())
	orphan.ProposalID = contract.ID(orphan.TransactionID, "proposal")
	garbage := []byte(`{"not":"a proposal","nonce":"` + time.Now().String() + `"}`)
	p, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var records []*kgo.Record
	for _, v := range []any{timely, orphan} {
		data, _ := json.Marshal(v)
		records = append(records, &kgo.Record{Topic: "swipeline.auth.proposals.v1", Key: []byte(v.(auth.Proposal).TransactionID), Value: data})
	}
	records = append(records, &kgo.Record{Topic: "swipeline.auth.proposals.v1", Key: []byte("garbage"), Value: garbage})
	for _, r := range records {
		r.Partition = auth.Partition(r.Key)
	}
	if err = p.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	service := gateway.New(ctx, s)
	c, err := Consumer(brokers, "swipeline-test-"+contract.ID(t.Name(), time.Now().String())[:12], func() { service.ConsumerReady.Store(true) })
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan error, 1)
	run, stop := context.WithCancel(ctx)
	go func() { done <- Replies(run, service, c, p) }()
	for {
		got, err := s.Lookup(ctx, row.Request.TransactionID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Final != nil {
			if got.Final.Source != "FLINK" || got.Final.Decision != "APPROVE" {
				t.Fatalf("%+v", got.Final)
			}
			break
		}
		select {
		case err := <-done:
			t.Fatal("reply loop stopped:", err)
		case <-ctx.Done():
			t.Fatal("proposal never arbitrated")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !service.ConsumerReady.Load() {
		t.Fatal("assignment did not mark the consumer ready")
	}
	want := map[string]string{hash(garbage): "INVALID_PROPOSAL", hash(records[1].Value): "UNKNOWN_TRANSACTION"}
	dlq, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics("swipeline.dlq.v1"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()
	for len(want) > 0 {
		fetch := dlq.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("dead letters missing:", want)
		}
		fetch.EachRecord(func(r *kgo.Record) {
			var letter map[string]any
			if json.Unmarshal(r.Value, &letter) == nil && want[letter["payload_hash"].(string)] == letter["error_code"] {
				delete(want, letter["payload_hash"].(string))
			}
		})
	}
	stop()
	if err := <-done; err != context.Canceled {
		t.Fatal("reply loop:", err)
	}
}

func hash(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

// Cancellation inside finalization must escape the poll as the plain context error.
func TestRepliesCancellationDuringArbitration(t *testing.T) {
	s, brokers := live(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	row := admitted(t, s, time.Now().Add(time.Minute))
	proposal := auth.Proposal{Request: row.Request, ProposalID: contract.ID(row.Request.TransactionID, "proposal"), Decision: "APPROVE", Reason: "RULES_CLEAR", Flags: []string{}, Usual: 100, Scored: row.Request.Received}
	data, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	record := &kgo.Record{Topic: "swipeline.auth.proposals.v1", Key: []byte(row.Request.TransactionID), Value: data}
	record.Partition = auth.Partition(record.Key)
	if err = p.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{record.Topic: {record.Partition: kgo.NewOffset().At(record.Offset)}}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	run, stop := context.WithCancel(ctx)
	defer stop()
	entered := make(chan struct{}, 1)
	service := &gateway.Service{Finals: gateway.NewQueue(ctx, ReplyPollRecords, func(call context.Context, values []store.Choice) ([]store.Row, error) {
		entered <- struct{}{}
		stop()
		return nil, context.Canceled
	})}
	err = Replies(run, service, c, p)
	select {
	case <-entered:
	default:
		t.Fatal("cancellation did not occur during arbitration")
	}
	if err != context.Canceled {
		t.Fatalf("reply loop returned %T: %v; want plain context.Canceled", err, err)
	}
}

// A slow finalization batch must not terminate the consumer or lose its record.
func TestRepliesRetryFinalizationTimeouts(t *testing.T) {
	for _, delay := range []time.Duration{0, 1100 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			s, brokers := live(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			row := admitted(t, s, time.Now().Add(time.Minute))
			proposal := auth.Proposal{Request: row.Request, ProposalID: contract.ID(row.Request.TransactionID, "proposal"), Decision: "APPROVE", Reason: "RULES_CLEAR", Flags: []string{}, Usual: 100, Scored: row.Request.Received}
			data, err := json.Marshal(proposal)
			if err != nil {
				t.Fatal(err)
			}
			p, err := Producer(brokers)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			record := &kgo.Record{Topic: "swipeline.auth.proposals.v1", Key: []byte(row.Request.TransactionID), Value: data}
			record.Partition = auth.Partition(record.Key)
			if err = p.ProduceSync(ctx, record).FirstErr(); err != nil {
				t.Fatal(err)
			}
			c, err := Consumer(brokers, "timeout-"+contract.ID(t.Name(), time.Now().String())[:12], func() {})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var first time.Time
			attempts := 0
			service := &gateway.Service{Finals: gateway.NewQueue(ctx, ReplyPollRecords, func(call context.Context, values []store.Choice) ([]store.Row, error) {
				for _, value := range values {
					if value.ID != row.Request.TransactionID {
						continue
					}
					attempts++
					if first.IsZero() {
						first = time.Now()
					}
					if attempts <= 3 || time.Since(first) < delay {
						return nil, context.DeadlineExceeded
					}
				}
				return s.Finalize(call, values)
			})}
			run, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- Replies(run, service, c, p) }()
			for {
				got, err := s.Lookup(ctx, row.Request.TransactionID)
				if err != nil {
					t.Fatal(err)
				}
				if got.Final != nil {
					if got.Final.Source != "FLINK" || got.Final.Decision != "APPROVE" {
						t.Fatalf("wrong final: %+v", got.Final)
					}
					break
				}
				select {
				case err := <-done:
					t.Fatalf("reply loop stopped before arbitration: %v", err)
				case <-ctx.Done():
					t.Fatal("proposal never arbitrated")
				case <-time.After(10 * time.Millisecond):
				}
			}
			select {
			case err := <-done:
				t.Fatalf("reply loop stopped after arbitration: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			stop()
			if err := <-done; err != context.Canceled {
				t.Fatalf("reply loop: %v", err)
			}
		})
	}
}

func TestRepliesReportRetriedErrors(t *testing.T) {
	s, brokers := live(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	row := admitted(t, s, time.Now().Add(time.Minute))
	proposal := auth.Proposal{Request: row.Request, ProposalID: contract.ID(row.Request.TransactionID, "proposal"), Decision: "APPROVE", Reason: "RULES_CLEAR", Flags: []string{}, Usual: 100, Scored: row.Request.Received}
	data, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	record := &kgo.Record{Topic: "swipeline.auth.proposals.v1", Key: []byte(row.Request.TransactionID), Value: data}
	record.Partition = auth.Partition(record.Key)
	if err = p.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c, err := Consumer(brokers, "report-"+contract.ID(t.Name(), time.Now().String())[:12], func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	service := gateway.New(ctx, s)
	var attempts atomic.Int32
	// A failed batch can be applied once more per item before Submit returns.
	// Six apply failures guarantee at least three observable reply retries.
	failure := errors.New("finalization unavailable for retry test")
	service.Finals = gateway.NewQueue(ctx, ReplyPollRecords, func(call context.Context, values []store.Choice) ([]store.Row, error) {
		for _, value := range values {
			if value.ID == row.Request.TransactionID && attempts.Add(1) <= 6 {
				return nil, failure
			}
		}
		return s.Finalize(call, values)
	})
	output, err := os.CreateTemp(t.TempDir(), "reply-stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	originalStderr := os.Stderr
	os.Stderr = output
	defer func() { os.Stderr = originalStderr }()
	run, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- Replies(run, service, c, p) }()
	for {
		got, err := s.Lookup(ctx, row.Request.TransactionID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Final != nil {
			if got.Final.Source != "FLINK" || got.Final.Decision != "APPROVE" {
				t.Fatalf("wrong final: %+v", got.Final)
			}
			break
		}
		select {
		case err := <-done:
			t.Fatalf("reply loop ended: %v", err)
		case <-ctx.Done():
			t.Fatal("record never arbitrated")
		case <-time.After(10 * time.Millisecond):
		}
	}
	stop()
	if err = <-done; err != context.Canceled {
		t.Fatalf("reply loop: %v", err)
	}
	logs, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(string(logs), failure.Error()); count != 1 {
		t.Errorf("want one rate-limited stderr report, got %d: %s", count, logs)
	}
	metrics := httptest.NewRecorder()
	service.Handler().ServeHTTP(metrics, httptest.NewRequest("GET", "/metrics", nil))
	found := false
	for _, line := range strings.Split(metrics.Body.String(), "\n") {
		var count uint64
		if _, err := fmt.Sscanf(line, "swipeline_gateway_reply_retries_total %d", &count); err == nil && count >= 3 {
			found = true
		}
	}
	if !found {
		t.Errorf("missing retry counter >=3 in metrics:\n%s", metrics.Body.String())
	}
	if attempts.Load() < 7 {
		t.Fatalf("record was not retried to success: %d", attempts.Load())
	}
}
