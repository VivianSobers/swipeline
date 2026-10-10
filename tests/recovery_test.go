package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func TestLateProposalAfterStandIn(t *testing.T) {
	c := installedClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runFault(t, "stop-taskmanager")
	cfg := installedConfig("late-proposal", 1, 20)
	if _, err := authorize.Run(ctx, cfg, c, "latency", 1); err != nil {
		t.Fatal(err)
	}
	before := installedFinals(t, c, cfg)[0]
	if before.Source != "STAND_IN" || before.Decision != "DECLINE" {
		t.Fatalf("expected durable stand-in: %+v", before)
	}
	runFault(t, "start-taskmanager")
	brokers := os.Getenv("SWIPELINE_KAFKA")
	if brokers == "" {
		t.Fatal("missing installed Kafka endpoint")
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics("swipeline.auth.proposals.v1"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.MaxVersions(kversion.V3_0_0()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var late *kgo.Record
	for late == nil {
		fetch := consumer.PollRecords(ctx, 256)
		if err := fetch.Err(); err != nil {
			t.Fatal(err)
		}
		for _, r := range fetch.Records() {
			var p auth.Proposal
			if json.Unmarshal(r.Value, &p) == nil && p.TransactionID == before.TransactionID {
				late = r
				break
			}
		}
	}
	for {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "swipeline-gateway-v1"
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: late.Topic, Partitions: []int32{late.Partition}}}
		response, err := consumer.Request(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		got := response.(*kmsg.OffsetFetchResponse)
		if got.ErrorCode != 0 {
			t.Fatalf("offset fetch error %d", got.ErrorCode)
		}
		consumed := false
		for _, topic := range got.Topics {
			for _, p := range topic.Partitions {
				if p.ErrorCode != 0 {
					t.Fatalf("partition offset error %d", p.ErrorCode)
				}
				if topic.Topic == late.Topic && p.Partition == late.Partition && p.Offset > late.Offset {
					consumed = true
				}
			}
		}
		if consumed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("late proposal was not consumed")
		case <-time.After(50 * time.Millisecond):
		}
	}
	after := installedFinals(t, c, cfg)[0]
	if !reflect.DeepEqual(before, after) {
		t.Fatal("late proposal changed the durable final")
	}
	t.Logf("late proposal consumed: partition=%d offset=%d; final unchanged", late.Partition, late.Offset)
}

// The host driver accepts only named faults and applies them to its own
// disposable project. Tests never receive a Docker socket.
func runFault(t *testing.T, action string) {
	t.Helper()
	dir := os.Getenv("SWIPELINE_FAULT_DIR")
	if dir == "" {
		t.Fatal("run through make m2-demo for fault control")
	}
	id := fmt.Sprint(time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(dir, "next"), []byte(id+" "+action+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "next"), filepath.Join(dir, "request")); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(3 * time.Minute)
	for time.Now().Before(until) {
		data, err := os.ReadFile(filepath.Join(dir, "done"))
		if err == nil && strings.HasPrefix(string(data), id+" ") {
			if string(data) != id+" 0\n" {
				t.Fatalf("fault %s failed: %s", action, data)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("fault %s timed out", action)
}

// Hold only updates for this run so the crash is guaranteed to interrupt
// durable pending admissions, independently of host scheduling speed.
func TestGatewayRestartRecovers(t *testing.T) {
	c := installedClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := store.Open(ctx, os.Getenv("SWIPELINE_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB.Close()
	cfg := installedConfig("gateway-restart", 64, 50)
	_, err = db.DB.ExecContext(ctx, `
 CREATE FUNCTION test_restart_barrier() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
  IF NEW.run_id = 'gateway-restart' AND (NEW.request_json->>'source_seq')::bigint > 8 THEN
   PERFORM pg_advisory_xact_lock(8010105);
  END IF;
  RETURN NEW;
 END $$;
 CREATE TRIGGER test_restart_barrier BEFORE UPDATE ON authorizations
 FOR EACH ROW EXECUTE FUNCTION test_restart_barrier();`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.DB.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS test_restart_barrier ON authorizations; DROP FUNCTION IF EXISTS test_restart_barrier()")
	}()
	guard, err := db.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Rollback()
	if _, err = guard.ExecContext(ctx, "SELECT pg_advisory_xact_lock(8010105)"); err != nil {
		t.Fatal(err)
	}
	replayCtx, stopReplay := context.WithCancel(ctx)
	defer stopReplay()
	replayDone := make(chan error, 1)
	go func() { _, err := authorize.Run(replayCtx, cfg, c, "latency", 128); replayDone <- err }()
	var admitted, pending int
	for {
		err = db.DB.QueryRowContext(ctx, "SELECT count(*), count(*) FILTER (WHERE status='PENDING') FROM authorizations WHERE run_id=$1", cfg.Generator.RunID).Scan(&admitted, &pending)
		if err != nil {
			t.Fatal(err)
		}
		if admitted >= 16 && pending > 0 {
			break
		}
		select {
		case err := <-replayDone:
			t.Fatalf("replay ended before crash point: %v", err)
		case <-ctx.Done():
			t.Fatal("no durable pending batch")
		case <-time.After(5 * time.Millisecond):
		}
	}
	runFault(t, "kill-gateway")
	stopReplay()
	select {
	case <-replayDone:
	case <-ctx.Done():
		t.Fatal("interrupted replay did not stop")
	}
	// The killed process cannot admit any more rows. Capture every durable
	// admission and any final it returned before recovery.
	rows, err := db.DB.QueryContext(ctx, "SELECT transaction_id, final_json FROM authorizations WHERE run_id=$1", cfg.Generator.RunID)
	if err != nil {
		t.Fatal(err)
	}
	before := map[string]*auth.Final{}
	for rows.Next() {
		var id string
		var data []byte
		if err = rows.Scan(&id, &data); err != nil {
			t.Fatal(err)
		}
		var f *auth.Final
		if len(data) > 0 {
			if err = json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
		}
		before[id] = f
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(before) < 16 || len(before) >= 64 {
		t.Fatalf("crash was not mid-run: %d admitted", len(before))
	}
	if err = guard.Rollback(); err != nil {
		t.Fatal(err)
	}
	runFault(t, "start-gateway")
	// Require recovery before resubmitting anything.
	for {
		err = db.DB.QueryRowContext(ctx, "SELECT count(*) FROM authorizations WHERE run_id=$1 AND status='PENDING'", cfg.Generator.RunID).Scan(&pending)
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("restart left durable admissions pending")
		case <-time.After(10 * time.Millisecond):
		}
	}
	recovered := map[string]auth.Final{}
	for id, prior := range before {
		row, err := db.Lookup(ctx, id)
		if err != nil || row.Final == nil {
			t.Fatalf("unrecovered admission %s: %v", id, err)
		}
		if prior != nil && !reflect.DeepEqual(prior, row.Final) {
			t.Fatal("restart changed a returned final")
		}
		recovered[id] = *row.Final
	}
	summary, err := authorize.Run(ctx, cfg, c, "latency", 128)
	if err != nil || summary.Completed != 64 {
		t.Fatalf("resume: %+v %v", summary, err)
	}
	finals := installedFinals(t, c, cfg)
	for _, f := range finals {
		if prior, ok := recovered[f.TransactionID]; ok && !reflect.DeepEqual(prior, f) {
			t.Fatal("resubmission changed a recovered final")
		}
	}
	if _, err = authorize.Run(ctx, cfg, c, "latency", 128); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(finals, installedFinals(t, c, cfg)) {
		t.Fatal("rerun changed stored outcomes")
	}
	var total, requests, decisions, accepted int
	err = db.DB.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM authorizations WHERE run_id=$1),
 (SELECT count(*) FROM outbox WHERE payload->>'run_id'=$1 AND topic='swipeline.auth.requests.v1'),
 (SELECT count(*) FROM outbox WHERE payload->>'run_id'=$1 AND topic='swipeline.decisions.raw.v1'),
 (SELECT accepted_count FROM runs WHERE run_id=$1)`, cfg.Generator.RunID).Scan(&total, &requests, &decisions, &accepted)
	if err != nil {
		t.Fatal(err)
	}
	if total != 64 || requests != 64 || decisions != 64 || accepted != 64 {
		t.Fatalf("duplicate or missing durable records: admissions=%d requests=%d decisions=%d accepted=%d", total, requests, decisions, accepted)
	}
	t.Logf("admitted-before-crash=%d recovered=%d completed=%d decision-outbox=%d; replay unchanged", len(before), len(recovered), total, decisions)
}
