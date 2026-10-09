package transport

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
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
