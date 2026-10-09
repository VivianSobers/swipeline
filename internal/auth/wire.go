// Package auth defines the authorization wire contracts shared by the
// gateway, the replayer and the streaming job.
package auth

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
)

// Request is an admitted authorization: the client's event plus metadata
// that only the gateway may assign.
type Request struct {
	contract.Event
	Received int64  `json:"received_at_ms"`
	Scoring  int64  `json:"scoring_deadline_at_ms"`
	Response int64  `json:"response_deadline_at_ms"`
	Mode     string `json:"deadline_mode"`
	Epoch    int64  `json:"seal_epoch"`
	CloseDay string `json:"close_day"`
	CardSeq  int64  `json:"accepted_card_seq"`
}

// Proposal is the streaming job's scored suggestion. It is never a ledger
// entry or an analytics decision.
type Proposal struct {
	Request
	ProposalID string   `json:"proposal_id"`
	Decision   string   `json:"decision"`
	Reason     string   `json:"reason"`
	Flags      []string `json:"fraud_flags"`
	Score      int      `json:"score"`
	Velocity   int      `json:"velocity_60s"`
	Usual      int64    `json:"usual_minor"`
	Ratio      float64  `json:"spend_ratio"`
	Distinct   float64  `json:"merchant_distinct_estimate"`
	Late       bool     `json:"features_late"`
	Scored     int64    `json:"scored_at_ms"`
}

// Final is the gateway's immutable outcome. Feature fields are null for a
// stand-in decline that had no timely proposal.
type Final struct {
	Request
	DecisionID string   `json:"decision_id"`
	Decision   string   `json:"decision"`
	Reason     string   `json:"reason"`
	Flags      []string `json:"fraud_flags"`
	ProposalID *string  `json:"proposal_id"`
	Score      *int     `json:"score"`
	Velocity   *int     `json:"velocity_60s"`
	Usual      *int64   `json:"usual_minor"`
	Ratio      *float64 `json:"spend_ratio"`
	Distinct   *float64 `json:"merchant_distinct_estimate"`
	Late       *bool    `json:"features_late"`
	Source     string   `json:"decision_source"`
	Finalized  int64    `json:"finalized_at_ms"`
	Latency    float64  `json:"decision_latency_ms"`
}

// trusted lists the admission fields only the gateway may assign.
var trusted = []string{"received_at_ms", "scoring_deadline_at_ms", "response_deadline_at_ms", "deadline_mode", "seal_epoch", "close_day", "accepted_card_seq"}

// Decode parses a client authorization whose Idempotency-Key header is key.
// The key must equal the transaction ID, so a retry can never be bound to a
// different transaction. Client-supplied admission metadata is rejected.
func Decode(b []byte, key string) (contract.Event, error) {
	e, err := contract.Decode(b)
	if err != nil {
		return e, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return e, err
	}
	for _, k := range trusted {
		if _, ok := raw[k]; ok {
			return e, fmt.Errorf("client supplied trusted field %s", k)
		}
	}
	if key != e.TransactionID {
		return e, errors.New("idempotency key mismatch")
	}
	return e, nil
}

// Run binds one authorization run to its business day, rule and reference
// versions and transport choices. Every admitted event must agree with it.
type Run struct {
	ID          string  `json:"run_id"`
	Identity    string  `json:"identity"`
	Day         string  `json:"business_date"`
	Rule        string  `json:"rule_version"`
	Reference   string  `json:"reference_version"`
	Mode        string  `json:"deadline_mode"`
	TPS         float64 `json:"scheduled_tps"`
	Compression float64 `json:"clock_compression"`
	Concurrency int     `json:"producer_concurrency"`
	Partitioner string  `json:"partitioner"`
	Partitions  int     `json:"partitions"`
}

func positive(f float64) bool { return f > 0 && !math.IsNaN(f) && !math.IsInf(f, 0) }

// Validate checks the run's names, deadline mode, partitioning and rates.
func (r Run) Validate() error {
	if _, err := time.Parse("2006-01-02", r.Day); err != nil {
		return errors.New("invalid business date")
	}
	if !contract.ValidName(r.ID) || r.Identity == "" || !contract.ValidName(r.Rule) || !contract.ValidName(r.Reference) {
		return errors.New("invalid run identifiers")
	}
	if r.Mode != "latency" && r.Mode != "throughput" {
		return errors.New("deadline mode must be latency or throughput")
	}
	if r.Partitioner != "sha256-json-v1" || r.Partitions != 4 {
		return errors.New("require sha256-json-v1 over 4 partitions")
	}
	if r.Concurrency < 1 || r.Concurrency > 128 || !positive(r.TPS) || !positive(r.Compression) {
		return errors.New("require 1..128 producers and finite positive TPS and compression")
	}
	return nil
}

// Check validates e and rejects any event whose run, day or versions
// conflict with r.
func (r Run) Check(e contract.Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.RunID != r.ID || e.AuthorizationDate != r.Day || e.RuleVersion != r.Rule || e.ReferenceVersion != r.Reference {
		return errors.New("run content conflict")
	}
	return nil
}

// Key is the Kafka record key for a card in a run: the compact JSON array
// [run, card], which no two distinct pairs share.
func Key(run, card string) []byte { b, _ := json.Marshal([]string{run, card}); return b }

// Partition implements partitioner sha256-json-v1: the first four bytes of
// SHA-256(key) as a big-endian unsigned integer, modulo 4.
func Partition(key []byte) int32 {
	h := sha256.Sum256(key)
	return int32(binary.BigEndian.Uint32(h[:4]) % 4)
}
