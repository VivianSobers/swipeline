// Package transport moves gateway outbox rows to Kafka in per-partition
// order and feeds the streaming job's proposals back into arbitration.
package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is an idempotent, unlingered producer that writes each record to
// the partition the outbox row already names.
func Producer(brokers string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(0), kgo.ProducerBatchMaxBytes(32768),
		kgo.MaxBufferedBytes(8<<20), kgo.MaxBufferedRecords(4096), kgo.RecordPartitioner(kgo.ManualPartitioner()))
}

// Consumer reads proposals for group. With a one-byte fetch minimum the
// broker answers as soon as a record exists; the 10 ms wait, the client's
// floor, only bounds an idle fetch. Offsets are
// committed only after arbitration, and a missing offset is an error rather
// than a silent jump to the latest record. ready runs on each assignment.
func Consumer(brokers, group string, ready func()) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group), kgo.ConsumeTopics("swipeline.auth.proposals.v1"),
		kgo.FetchMinBytes(1), kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchMaxBytes(8<<20),
		kgo.DisableAutoCommit(), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { ready() }))
}

var proposalFields = []string{"schema_version", "event_id", "run_id", "transaction_id", "source_seq", "event_time_ms",
	"produced_at_ms", "payload_hash", "rule_version", "proposal_id", "decision", "reason", "fraud_flags", "score",
	"velocity_60s", "usual_minor", "spend_ratio", "merchant_distinct_estimate", "features_late", "scored_at_ms"}

var flags = map[string]bool{"CARD_TESTING": true, "CARD_VELOCITY": true, "SPEND_ANOMALY": true}

// Proposal decodes and checks one reply record. A record that fails here is
// dead-lettered on its own and never reaches a database batch.
func Proposal(b []byte) (auth.Proposal, error) {
	var p auth.Proposal
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return p, err
	}
	for _, k := range proposalFields {
		if v, ok := raw[k]; !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return p, fmt.Errorf("missing or null %s", k)
		}
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return p, err
	}
	if p.SchemaVersion != 1 || p.ProposalID != contract.ID(p.TransactionID, "proposal") ||
		(p.Decision != "APPROVE" && p.Decision != "DECLINE") || p.Score < 0 || p.Score > 180 || p.Velocity < 0 ||
		p.Usual < 100 || p.Ratio < 0 || p.Distinct < 0 || p.Flags == nil {
		return p, errors.New("invalid proposal")
	}
	for _, f := range p.Flags {
		if !flags[f] {
			return p, errors.New("unknown fraud flag")
		}
	}
	return p, nil
}

// DeadLetter describes an unusable record without copying its contents:
// operators get its origin and a hash to correlate, never card data.
func DeadLetter(topic string, partition int32, offset int64, code string, b []byte) []byte {
	sum := sha256.Sum256(b)
	out, _ := json.Marshal(map[string]any{"origin_topic": topic, "partition": partition, "offset": offset,
		"error_code": code, "payload_hash": hex.EncodeToString(sum[:]), "sanitized_input": "redacted",
		"detected_at_ms": time.Now().UnixMilli()})
	return out
}
