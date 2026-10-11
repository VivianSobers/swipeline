package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

func getFlinkJob(ctx context.Context, flinkBase string) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", flinkBase+"/jobs/overview", nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var doc struct {
		Jobs []struct {
			JID   string `json:"jid"`
			Name  string `json:"name"`
			State string `json:"state"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", err
	}
	for _, j := range doc.Jobs {
		if j.Name == "authorization" && j.State == "RUNNING" {
			return j.JID, nil
		}
	}
	return "", fmt.Errorf("running authorization job not found")
}

func getFlinkCompletedCheckpoints(ctx context.Context, flinkBase, jid string) (int64, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", flinkBase+"/jobs/"+jid+"/checkpoints", nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var doc struct {
		Counts struct {
			Completed int64 `json:"completed"`
			Restored  int64 `json:"restored"`
		} `json:"counts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return 0, err
	}
	return doc.Counts.Completed, nil
}

func getFlinkRestoredCheckpoints(ctx context.Context, flinkBase, jid string) (int64, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "GET", flinkBase+"/jobs/"+jid+"/checkpoints", nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var doc struct {
		Counts struct {
			Completed int64 `json:"completed"`
			Restored  int64 `json:"restored"`
		} `json:"counts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return 0, err
	}
	return doc.Counts.Restored, nil
}

func TestCanonicalDecisionsMatchFinals(t *testing.T) {
	c := installedClient(t)
	brokers := os.Getenv("SWIPELINE_KAFKA")
	if brokers == "" {
		t.Skip("run through make m2-demo")
	}

	cfg := installedConfig("installed-42", 1000, 50)
	finals := installedFinals(t, c, cfg)

	expected := make(map[string]auth.Final, len(finals))
	for _, f := range finals {
		resp, err := c.HTTP.Get(c.Base + "/v1/authorizations/" + f.TransactionID)
		if err != nil {
			t.Fatal(err)
		}
		var got auth.Final
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			resp.Body.Close()
			t.Fatal(err)
		}
		resp.Body.Close()
		expected[f.TransactionID] = got
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.decisions.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	seen := make(map[string]auth.Final)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for len(seen) < len(finals) {
		fetch := consumer.PollRecords(ctx, 256)
		if err := fetch.Err(); err != nil {
			if ctx.Err() != nil {
				break
			}
			t.Fatal(err)
		}
		for _, r := range fetch.Records() {
			var d auth.Final
			if err := json.Unmarshal(r.Value, &d); err != nil {
				t.Fatalf("failed to decode canonical decision: %v", err)
			}
			if d.TransactionID == "" {
				t.Fatalf("missing transaction_id in record: %s", string(r.Value))
			}
			if string(r.Key) != d.TransactionID {
				t.Fatalf("record key %q != transaction_id %q", string(r.Key), d.TransactionID)
			}
			if d.RunID == "installed-42" {
				if _, exists := seen[d.TransactionID]; exists {
					t.Fatalf("duplicate canonical decision for transaction %s", d.TransactionID)
				}
				seen[d.TransactionID] = d
			} else {
				resp, err := c.HTTP.Get(c.Base + "/v1/authorizations/" + d.TransactionID)
				if err != nil || resp.StatusCode != http.StatusOK {
					t.Fatalf("canonical record for unfinalized transaction %s: status=%v err=%v", d.TransactionID, resp.StatusCode, err)
				}
				resp.Body.Close()
			}
		}
	}

	if len(seen) != len(finals) {
		t.Fatalf("canonical decisions for installed-42 = %d, want %d", len(seen), len(finals))
	}

	for txID, want := range expected {
		got, ok := seen[txID]
		if !ok {
			t.Fatalf("missing canonical decision for transaction %s", txID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("canonical decision mismatch for %s:\ngot  %+v\nwant %+v", txID, got, want)
		}
	}
}

func TestCanonicalDropDuplicateRawDecisions(t *testing.T) {
	brokers := os.Getenv("SWIPELINE_KAFKA")
	if brokers == "" {
		t.Skip("run through make m2-demo")
	}
	flinkBase := os.Getenv("SWIPELINE_FLINK")
	if flinkBase == "" {
		flinkBase = "http://flink-jobmanager:8081"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	jid, err := getFlinkJob(ctx, flinkBase)
	if err != nil {
		t.Fatal(err)
	}

	rawConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.decisions.raw.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rawConsumer.Close()

	var toDuplicate []*kgo.Record
	for len(toDuplicate) < 20 {
		fetch := rawConsumer.PollRecords(ctx, 256)
		if err := fetch.Err(); err != nil {
			t.Fatal(err)
		}
		for _, r := range fetch.Records() {
			var d struct {
				RunID string `json:"run_id"`
			}
			if json.Unmarshal(r.Value, &d) == nil && d.RunID == "installed-42" {
				toDuplicate = append(toDuplicate, r)
				if len(toDuplicate) == 20 {
					break
				}
			}
		}
	}

	initialCheckpoints, err := getFlinkCompletedCheckpoints(ctx, flinkBase, jid)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ProducerBatchMaxBytes(32768),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	for _, r := range toDuplicate {
		dup := &kgo.Record{
			Topic: "swipeline.decisions.raw.v1",
			Key:   r.Key,
			Value: r.Value,
		}
		producer.Produce(ctx, dup, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Fatalf("produce error: %v", err)
			}
		})
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Wait for at least one completed Flink checkpoint after injection
	for {
		cp, err := getFlinkCompletedCheckpoints(ctx, flinkBase, jid)
		if err == nil && cp >= initialCheckpoints+1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for flink checkpoint")
		case <-time.After(500 * time.Millisecond):
		}
	}

	// Verify canonical decisions count for installed-42 is unchanged
	canonConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.decisions.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer canonConsumer.Close()

	canonCount := 0
	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pollCancel()
	for {
		fetch := canonConsumer.PollRecords(pollCtx, 256)
		if fetch.IsClientClosed() || pollCtx.Err() != nil {
			break
		}
		if fetch.Empty() {
			break
		}
		for _, r := range fetch.Records() {
			var d struct {
				RunID string `json:"run_id"`
			}
			if json.Unmarshal(r.Value, &d) == nil && d.RunID == "installed-42" {
				canonCount++
			}
		}
	}

	if canonCount != 1000 {
		t.Fatalf("canonical count for installed-42 after duplicate injection = %d, want 1000", canonCount)
	}
}

func TestCanonicalQuarantineConflictingRawDecisions(t *testing.T) {
	brokers := os.Getenv("SWIPELINE_KAFKA")
	if brokers == "" {
		t.Skip("run through make m2-demo")
	}
	flinkBase := os.Getenv("SWIPELINE_FLINK")
	if flinkBase == "" {
		flinkBase = "http://flink-jobmanager:8081"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	jid, err := getFlinkJob(ctx, flinkBase)
	if err != nil {
		t.Fatal(err)
	}

	rawConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.decisions.raw.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rawConsumer.Close()

	var target *kgo.Record
	for target == nil {
		fetch := rawConsumer.PollRecords(ctx, 256)
		if err := fetch.Err(); err != nil {
			t.Fatal(err)
		}
		for _, r := range fetch.Records() {
			var d struct {
				RunID string `json:"run_id"`
			}
			if json.Unmarshal(r.Value, &d) == nil && d.RunID == "installed-42" {
				target = r
				break
			}
		}
	}

	var originalMap map[string]any
	if err := json.Unmarshal(target.Value, &originalMap); err != nil {
		t.Fatal(err)
	}
	originalAmount := originalMap["amount_minor"].(float64)

	conflictingMap := make(map[string]any, len(originalMap))
	for k, v := range originalMap {
		conflictingMap[k] = v
	}
	conflictingMap["amount_minor"] = originalAmount + 1
	conflictingBytes, err := json.Marshal(conflictingMap)
	if err != nil {
		t.Fatal(err)
	}

	initialCheckpoints, err := getFlinkCompletedCheckpoints(ctx, flinkBase, jid)
	if err != nil {
		t.Fatal(err)
	}

	dlqConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.dlq.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dlqConsumer.Close()

	drainCtx, drainCancel := context.WithTimeout(ctx, 2*time.Second)
	for {
		fetch := dlqConsumer.PollRecords(drainCtx, 256)
		if fetch.IsClientClosed() || drainCtx.Err() != nil || fetch.Empty() {
			break
		}
	}
	drainCancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ProducerBatchMaxBytes(32768),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	conflictingRecord := &kgo.Record{
		Topic: "swipeline.decisions.raw.v1",
		Key:   target.Key,
		Value: conflictingBytes,
	}
	producer.Produce(ctx, conflictingRecord, func(_ *kgo.Record, err error) {
		if err != nil {
			t.Fatalf("produce error: %v", err)
		}
	})
	if err := producer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	for {
		cp, err := getFlinkCompletedCheckpoints(ctx, flinkBase, jid)
		if err == nil && cp >= initialCheckpoints+1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for flink checkpoint")
		case <-time.After(500 * time.Millisecond):
		}
	}

	h := sha256.Sum256(conflictingBytes)
	dlqKey := hex.EncodeToString(h[:])

	dlqMatches := 0
	pollCtx, pollCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pollCancel()
	for {
		fetch := dlqConsumer.PollRecords(pollCtx, 256)
		if fetch.IsClientClosed() || pollCtx.Err() != nil {
			break
		}
		for _, r := range fetch.Records() {
			if string(r.Key) == dlqKey {
				var dlq struct {
					ErrorCode string `json:"error_code"`
				}
				if json.Unmarshal(r.Value, &dlq) == nil && dlq.ErrorCode == "DECISION_CONFLICT" {
					dlqMatches++
				}
			}
		}
		if dlqMatches > 0 {
			break
		}
	}

	if dlqMatches != 1 {
		t.Fatalf("dlq records with DECISION_CONFLICT = %d, want 1", dlqMatches)
	}

	canonConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.decisions.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer canonConsumer.Close()

	var canonMatches int
	var canonAmount float64
	cPollCtx, cPollCancel := context.WithTimeout(ctx, 5*time.Second)
	defer cPollCancel()
	for {
		fetch := canonConsumer.PollRecords(cPollCtx, 256)
		if fetch.IsClientClosed() || cPollCtx.Err() != nil || fetch.Empty() {
			break
		}
		for _, r := range fetch.Records() {
			if string(r.Key) == string(target.Key) {
				canonMatches++
				var d struct {
					AmountMinor float64 `json:"amount_minor"`
				}
				if json.Unmarshal(r.Value, &d) == nil {
					canonAmount = d.AmountMinor
				}
			}
		}
	}
	if canonMatches != 1 {
		t.Fatalf("expected 1 canonical record for %s, got %d", string(target.Key), canonMatches)
	}
	if canonAmount != originalAmount {
		t.Fatalf("canonical record for %s changed: got %v, want original %v", string(target.Key), canonAmount, originalAmount)
	}
}

func TestCanonicalRestartWithoutDuplicates(t *testing.T) {
	c := installedClient(t)
	brokers := os.Getenv("SWIPELINE_KAFKA")
	if brokers == "" {
		t.Skip("run through make m2-demo")
	}
	flinkBase := os.Getenv("SWIPELINE_FLINK")
	if flinkBase == "" {
		flinkBase = "http://flink-jobmanager:8081"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	db, err := store.Open(ctx, os.Getenv("SWIPELINE_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB.Close()

	cfg := installedConfig("installed-restart", 300, 50)
	replayDone := make(chan error, 1)
	go func() {
		_, err := authorize.Run(ctx, cfg, c, "latency", 128)
		replayDone <- err
	}()

	for {
		var finalsCount int
		err := db.DB.QueryRowContext(ctx, "SELECT count(*) FROM authorizations WHERE run_id=$1 AND status='FINAL'", cfg.Generator.RunID).Scan(&finalsCount)
		if err == nil && finalsCount >= 100 {
			break
		}
		select {
		case err := <-replayDone:
			t.Fatalf("replay finished before 100 finals: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for 100 finals")
		case <-time.After(20 * time.Millisecond):
		}
	}

	runFault(t, "restart-flink")

	select {
	case err := <-replayDone:
		if err != nil {
			t.Fatalf("replay failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for replay to finish")
	}

	finals := installedFinals(t, c, cfg)
	if len(finals) != 300 {
		t.Fatalf("expected 300 finals, got %d", len(finals))
	}

	canonConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics("swipeline.decisions.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer canonConsumer.Close()

	canonCounts := make(map[string]int)
	var totalCanonRecords int
	cCtx, cCancel := context.WithTimeout(ctx, 60*time.Second)
	defer cCancel()
	for len(canonCounts) < len(finals) {
		fetch := canonConsumer.PollRecords(cCtx, 256)
		if fetch.IsClientClosed() || cCtx.Err() != nil {
			break
		}
		for _, r := range fetch.Records() {
			var d struct {
				RunID         string `json:"run_id"`
				TransactionID string `json:"transaction_id"`
			}
			if json.Unmarshal(r.Value, &d) == nil && d.RunID == cfg.Generator.RunID {
				canonCounts[d.TransactionID]++
				totalCanonRecords++
			}
		}
	}

	if len(canonCounts) != len(finals) {
		t.Fatalf("canonical count for %s = %d, want %d", cfg.Generator.RunID, len(canonCounts), len(finals))
	}
	if totalCanonRecords != len(finals) {
		t.Fatalf("total canonical records for %s = %d, want %d", cfg.Generator.RunID, totalCanonRecords, len(finals))
	}
	for _, f := range finals {
		if count := canonCounts[f.TransactionID]; count != 1 {
			t.Fatalf("transaction %s has %d canonical records, want 1", f.TransactionID, count)
		}
	}

	var jidAfter string
	for attempt := 0; attempt < 30; attempt++ {
		jidAfter, err = getFlinkJob(ctx, flinkBase)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if jidAfter == "" {
		t.Fatal("no running flink job found after restart")
	}

	var restored int64
	for attempt := 0; attempt < 30; attempt++ {
		restored, err = getFlinkRestoredCheckpoints(ctx, flinkBase, jidAfter)
		if err == nil && restored > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if restored != 1 {
		t.Fatalf("flink job restored checkpoints = %d, want 1", restored)
	}
}
