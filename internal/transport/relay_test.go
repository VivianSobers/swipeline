package transport

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/gateway"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/lifecycle"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAckPrefixStopsAtFailure(t *testing.T) {
	if got := Prefix([]bool{true, false, true}); got != 1 {
		t.Fatal(got)
	}
	if Prefix([]bool{true, true}) != 2 || Prefix(nil) != 0 {
		t.Fatal("lost ack")
	}
}

// live returns a migrated store and the broker list, or skips.
func live(t *testing.T) (*store.Store, string) {
	t.Helper()
	dsn, brokers := os.Getenv("SWIPELINE_DB"), os.Getenv("SWIPELINE_KAFKA")
	if dsn == "" || brokers == "" {
		t.Skip("run make db-test")
	}
	s, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DB.Close() })
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, brokers
}

// admitted registers a fresh run and admits its first event.
func admitted(t *testing.T, s *store.Store, received time.Time) store.Row {
	t.Helper()
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
	ctx := context.Background()
	if err = s.Register(ctx, r); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Admit(ctx, []store.Input{{Event: e, Received: received.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}
	return rows[0]
}

func TestRelayDeliversToNamedPartition(t *testing.T) {
	s, brokers := live(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	row := admitted(t, s, time.Now().Add(time.Minute))
	p, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var offset *int64
	var partition int32
	for offset == nil {
		n, err := RelayOnce(ctx, s, p)
		if err != nil {
			t.Fatal(err)
		}
		err = s.DB.QueryRowContext(ctx, "SELECT broker_offset, partition FROM outbox WHERE event_id=$1 AND sent_at IS NOT NULL", row.Request.EventID).Scan(&offset, &partition)
		if err != nil && n == 0 {
			t.Fatal("relay stopped before sending the row:", err)
		}
	}
	key := auth.Key(row.Request.RunID, row.Request.CardToken)
	if partition != auth.Partition(key) {
		t.Fatal("wrong partition", partition)
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		"swipeline.auth.requests.v1": {partition: kgo.NewOffset().At(*offset)}}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fetch := c.PollRecords(ctx, 1)
	if err = fetch.Err(); err != nil {
		t.Fatal(err)
	}
	got := fetch.Records()[0]
	var sent auth.Request
	if err = json.Unmarshal(got.Value, &sent); err != nil || string(got.Key) != string(key) || sent != row.Request {
		t.Fatalf("relayed %s / %s", got.Key, got.Value)
	}
}

func TestRelayLoopSendsOnWake(t *testing.T) {
	s, brokers := live(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	service := gateway.New(ctx, s)
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- Relay(run, s, p, service.Outbox, func(err error) { t.Log("relay:", err) }) }()
	row := admitted(t, s, time.Now().Add(time.Minute))
	service.Wake()
	for {
		var sent bool
		if err := s.DB.QueryRowContext(ctx, "SELECT sent_at IS NOT NULL FROM outbox WHERE event_id=$1", row.Request.EventID).Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if sent {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("relay loop never sent the admission")
		case <-time.After(5 * time.Millisecond):
		}
	}
	stop()
	if err := <-done; err != context.Canceled {
		t.Fatal(err)
	}
}

func TestRelayDeliversLifecycleTopic(t *testing.T) {
	if !IsRelayableTopic("swipeline.lifecycle.v1") {
		t.Fatal("relay ignores lifecycle topic: swipeline.lifecycle.v1")
	}
}

func TestRelayDeliversLifecycleOutbox(t *testing.T) {
	s, brokers := live(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	row := admitted(t, s, time.Now().Add(time.Minute))

	// Finalize authorization so it is approved
	prop := &auth.Proposal{
		Request:    row.Request,
		ProposalID: contract.ID(row.Request.TransactionID, "prop"),
		Decision:   "APPROVE",
		Reason:     "OK",
		Flags:      []string{},
		Score:      10,
	}
	if _, err := s.Finalize(ctx, []store.Choice{{ID: row.Request.TransactionID, Proposal: prop}}); err != nil {
		t.Fatal(err)
	}

	// Admit lifecycle event
	lcEvt, err := lifecycle.New(
		row.Request.RunID,
		row.Request.TransactionID,
		row.Request.SourceSeq,
		2,
		string(lifecycle.KindClear),
		row.Request.EventID,
		time.Now().UnixMilli(),
		time.Now().UnixMilli(),
		row.Request.AuthorizationDate,
		row.Request.Currency,
		row.Request.AmountMinor,
		row.Request.MerchantID,
		row.Request.IssuerBankID,
		row.Request.AcquirerBankID,
		row.Request.ReferenceVersion,
		row.Request.Epoch,
		row.Request.CloseDay,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceiveLifecycle(ctx, lcEvt); err != nil {
		t.Fatal(err)
	}

	p, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var offset *int64
	var partition int32
	for offset == nil {
		n, err := RelayOnce(ctx, s, p)
		if err != nil {
			t.Fatal(err)
		}
		err = s.DB.QueryRowContext(ctx, "SELECT broker_offset, partition FROM outbox WHERE event_id=$1 AND sent_at IS NOT NULL", lcEvt.EventID).Scan(&offset, &partition)
		if err != nil && n == 0 {
			t.Fatal("relay stopped before sending lifecycle row:", err)
		}
	}

	c, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		LifecycleTopic: {partition: kgo.NewOffset().At(*offset)}}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fetch := c.PollRecords(ctx, 1)
	if err = fetch.Err(); err != nil {
		t.Fatal(err)
	}
	got := fetch.Records()[0]
	if string(got.Key) != lcEvt.TransactionID {
		t.Fatalf("key = %s, want %s", got.Key, lcEvt.TransactionID)
	}
	var relayedEvt lifecycle.Event
	if err = json.Unmarshal(got.Value, &relayedEvt); err != nil || relayedEvt.EventID != lcEvt.EventID {
		t.Fatalf("relayed %s", got.Value)
	}
}
