package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/inspect"
	"github.com/VivianSobers/swipeline/internal/lifecycle"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

func lifecycleBatch(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/batch", bytes.NewReader(nil))
		if err != nil {
			t.Fatal(err)
		}
		response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		status := response.StatusCode
		response.Body.Close()
		if status == 200 {
			return
		}
		if status != 503 {
			t.Fatalf("batch HTTP %d", status)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}

}

func lifecycleSnapshot(t *testing.T, ctx context.Context, endpoint, tx string) inspect.Result {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		result, err := inspect.Inspect(ctx, tx, endpoint)
		if err == nil && result.Status != "NOT_FOUND" {
			return result
		}
		if time.Now().After(deadline) {
			t.Fatalf("inspection %s: %v (%s)", tx, err, result.Status)
		}
		time.Sleep(time.Second)
	}
}

func lifecycleFlush(t *testing.T, ctx context.Context, db *store.Store, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var pending int
		if err := db.DB.QueryRowContext(ctx, "SELECT count(*) FROM outbox WHERE sent_at IS NULL").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox did not drain")
		}
		time.Sleep(250 * time.Millisecond)
	}
	lifecycleCanonicalBarrier(t, ctx, db)
	lifecycleBatch(t, ctx, endpoint)
	// An HTTP trigger may encounter the polling worker's lock. Wait for all
	// accepted records to be in a completed manifest before returning.
	for {
		var pending int
		if err := db.DB.QueryRowContext(ctx, "SELECT count(*) FROM ingest_batches WHERE state != 'COMPLETE'").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("batch did not complete")
		}
		time.Sleep(time.Second)
	}
}

func TestInstalledLifecyclePrepare(t *testing.T) {
	endpoint := os.Getenv("SWIPELINE_SPARK")
	if endpoint == "" {
		t.Skip("run make m4-demo")
	}
	for _, key := range []string{"SWIPELINE_TEST_GATEWAY", "SWIPELINE_TEST_DB", "SWIPELINE_FIXTURES", "SWIPELINE_KAFKA"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing %s", key)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db, err := store.Open(ctx, os.Getenv("SWIPELINE_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB.Close()
	client := installedClient(t)
	// Warm the installed scoring job before the four fixed business requests.
	// Cold-start stand-in declines remain durable and belong to a separate run.
	if _, err := authorize.Run(ctx, installedConfig("m4-warm", 300, 50), client, "throughput", 4); err != nil {
		t.Fatal(err)
	}
	run := "m4-deterministic"
	day := "2026-10-08"
	if err := client.Register(ctx, auth.Run{ID: run, Identity: contract.ID(run), Day: day, Rule: authorize.Rules, Reference: generator.Reference, Mode: "throughput", TPS: 1, Compression: 1, Concurrency: 1, Partitioner: "sha256-json-v1", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	finals := make([]auth.Final, 4)
	for i := range finals {
		business := contract.Business{RunID: run, TransactionID: contract.ID(run, fmt.Sprint(i)), SourceSeq: int64(i), EventTimeMS: 1791417600000 + int64(i)*1000, CardToken: "card_" + contract.ID(run, "card", fmt.Sprint(i)), MerchantID: "merchant-001", MCC: "5411", IssuerBankID: "bank-0", AcquirerBankID: "bank-1", Currency: "USD", AmountMinor: 100, AuthorizationDate: day, RuleVersion: authorize.Rules, ReferenceVersion: generator.Reference}
		event, err := contract.New(business, time.Now().UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		finals[i], err = client.Send(ctx, event)
		if err != nil {
			t.Fatal(err)
		}
		if finals[i].Decision != "APPROVE" {
			t.Fatalf("fixture %d declined: %+v", i, finals[i])
		}
	}
	makeEvent := func(i, seq int, kind, parent string) lifecycle.Event {
		f := finals[i]
		event, err := lifecycle.New(run, f.TransactionID, f.SourceSeq, seq, kind, parent, f.EventTimeMS+int64(seq)*1000, time.Now().UnixMilli(), day, f.Currency, f.AmountMinor, f.MerchantID, f.IssuerBankID, f.AcquirerBankID, f.ReferenceVersion, f.Epoch, f.CloseDay)
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	send := func(event lifecycle.Event) {
		if _, err := client.SendLifecycle(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	clears := make([]lifecycle.Event, 4)
	for i := range finals {
		clears[i] = makeEvent(i, 2, "CLEAR", finals[i].EventID)
	}
	send(clears[0])
	cb := makeEvent(1, 3, "CHARGEBACK", clears[1].EventID)
	send(cb)
	pre := makeEvent(2, 2, "REVERSE", finals[2].EventID)
	send(pre)
	send(clears[3])
	lifecycleFlush(t, ctx, db, endpoint)
	if _, err := VerifyLifecycleOutOfOrder(ctx, finals[1].TransactionID, endpoint, 1); err != nil {
		t.Fatal(err)
	}
	send(clears[1])
	post := makeEvent(3, 3, "REVERSE", clears[3].EventID)
	send(post)
	lifecycleFlush(t, ctx, db, endpoint)
	if _, err := client.SendLifecycle(ctx, clears[2]); !errors.Is(err, authorize.ErrConflict) {
		t.Fatalf("competing terminal accepted: %v", err)
	}
	if _, err := client.SendLifecycle(ctx, makeEvent(3, 3, "CHARGEBACK", clears[3].EventID)); !errors.Is(err, authorize.ErrConflict) {
		t.Fatalf("competing terminal accepted: %v", err)
	}
	baseline := map[string]inspect.Result{}
	for _, f := range finals {
		baseline[f.TransactionID] = lifecycleSnapshot(t, ctx, endpoint, f.TransactionID)
		for _, event := range baseline[f.TransactionID].Events {
			if event.EventTimeMS <= 0 {
				t.Fatal("installed journal event timestamp missing")
			}
		}
	}
	for _, e := range []lifecycle.Event{clears[0], clears[1], cb, pre, clears[3], post} {
		send(e)
	}
	lifecycleFlush(t, ctx, db, endpoint)
	for tx, expected := range baseline {
		if got := lifecycleSnapshot(t, ctx, endpoint, tx); !reflect.DeepEqual(got, expected) {
			t.Fatalf("duplicate changed %s", tx)
		}
	}
	fixtureDir := os.Getenv("SWIPELINE_FIXTURES")
	vars := fmt.Sprintf("SWIPELINE_TEST_TX_ID=%s\nSWIPELINE_TEST_TX_ID_OOO=%s\nSWIPELINE_TEST_TX_PRE_REV=%s\nSWIPELINE_TEST_TX_POST_REV=%s\nSWIPELINE_TEST_TX_QUAR=%s\n", finals[0].TransactionID, finals[1].TransactionID, finals[2].TransactionID, finals[3].TransactionID, finals[2].TransactionID)
	if err := os.WriteFile(filepath.Join(fixtureDir, "ids.env"), []byte(vars), 0644); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixtureDir, "baseline.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("approved deterministic fixtures: %s", vars)
}

func TestInstalledLifecycleStability(t *testing.T) {
	endpoint := os.Getenv("SWIPELINE_SPARK")
	if endpoint == "" {
		t.Skip("run make m4-demo")
	}
	if os.Getenv("SWIPELINE_FIXTURES") == "" {
		t.Fatal("missing SWIPELINE_FIXTURES")
	}
	data, err := os.ReadFile(filepath.Join(os.Getenv("SWIPELINE_FIXTURES"), "baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	var baseline map[string]inspect.Result
	if err := json.Unmarshal(data, &baseline); err != nil {
		t.Fatal(err)
	}
	if len(baseline) != 4 {
		t.Fatal("missing lifecycle fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	lifecycleBatch(t, ctx, endpoint)
	for tx, expected := range baseline {
		if got := lifecycleSnapshot(t, ctx, endpoint, tx); !reflect.DeepEqual(got, expected) {
			t.Fatalf("restart/rerun changed %s", tx)
		}
	}
}

func TestInstalledLifecycleInspectionLock(t *testing.T) {
	endpoint := os.Getenv("SWIPELINE_SPARK")
	if endpoint == "" {
		t.Skip("run make m4-demo")
	}
	if os.Getenv("SWIPELINE_TEST_DB") == "" || os.Getenv("SWIPELINE_TEST_TX_ID") == "" {
		t.Fatal("missing inspection lock fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := store.Open(ctx, os.Getenv("SWIPELINE_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB.Close()
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(72002)"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(72002)")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Get(endpoint + "/inspect?transaction_id=" + os.Getenv("SWIPELINE_TEST_TX_ID"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatalf("inspection bypassed independent writer lock: HTTP %d", response.StatusCode)
	}
}

// Wait for actual committed canonical records, not just delivery into the raw
// decision topic. Flink checkpoints may lag the SQL outbox's sent_at marker.
func lifecycleCanonicalBarrier(t *testing.T, parent context.Context, db *store.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	rows, err := db.DB.QueryContext(ctx, `SELECT topic, payload->>'event_id' FROM outbox WHERE topic IN ('swipeline.decisions.raw.v1','swipeline.lifecycle.v1')`)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{}
	for rows.Next() {
		var topic, id string
		if err := rows.Scan(&topic, &id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if topic == "swipeline.decisions.raw.v1" {
			topic = "swipeline.decisions.v1"
		}
		expected[topic+":"+id] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	partitions := map[string]map[int32]kgo.Offset{}
	for _, topic := range []string{"swipeline.decisions.v1", "swipeline.lifecycle.v1"} {
		partitions[topic] = map[int32]kgo.Offset{}
		for p := int32(0); p < 4; p++ {
			partitions[topic][p] = kgo.NewOffset().AtStart()
		}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("SWIPELINE_KAFKA")), kgo.ConsumePartitions(partitions), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for len(expected) > 0 {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("canonical records missing: %d (%v)", len(expected), err)
		}
		for _, failure := range fetches.Errors() {
			t.Fatalf("canonical Kafka fetch failed: %v", failure.Err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var envelope struct {
				EventID string `json:"event_id"`
			}
			if err := json.Unmarshal(record.Value, &envelope); err != nil {
				t.Fatal(err)
			}
			delete(expected, record.Topic+":"+envelope.EventID)
		})
	}
	t.Log("all admitted canonical records are read_committed before ingestion")
}
