// Package auth defines the authorization wire contracts shared by the
// gateway, the replayer and the streaming job.
package auth

import (
	"encoding/json"
	"errors"
	"fmt"

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
