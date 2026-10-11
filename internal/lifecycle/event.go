// Package lifecycle defines the event contract, scheduling, and validation
// for transaction lifecycle transitions (clearing, reversals, and chargebacks).
package lifecycle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
)

// Kind represents the lifecycle transition event kind.
type Kind string

const (
	KindClear      Kind = "CLEAR"
	KindReverse    Kind = "REVERSE"
	KindChargeback Kind = "CHARGEBACK"
)

var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Event represents a financial lifecycle transition event envelope and payload.
type Event struct {
	SchemaVersion     int    `json:"schema_version"`
	EventID           string `json:"event_id"`
	RunID             string `json:"run_id"`
	TransactionID     string `json:"transaction_id"`
	SourceSeq         int64  `json:"source_seq"`
	LifecycleSeq      int    `json:"lifecycle_seq"`
	Kind              string `json:"kind"`
	ParentEventID     string `json:"parent_event_id"`
	EventTimeMS       int64  `json:"event_time_ms"`
	ProducedAtMS      int64  `json:"produced_at_ms"`
	AuthorizationDate string `json:"authorization_date"`
	Currency          string `json:"currency"`
	AmountMinor       int64  `json:"amount_minor"`
	MerchantID        string `json:"merchant_id"`
	IssuerBankID      string `json:"issuer_bank_id"`
	AcquirerBankID    string `json:"acquirer_bank_id"`
	ReferenceVersion  string `json:"reference_version"`
	SealEpoch         int64  `json:"seal_epoch"`
	CloseDay          string `json:"close_day"`
	PayloadHash       string `json:"payload_hash"`
}

// Receipt represents an admission receipt returned by the gateway for a lifecycle event.
type Receipt struct {
	EventID    string `json:"event_id"`
	SealEpoch  int64  `json:"seal_epoch"`
	CloseDay   string `json:"close_day"`
	ReceivedAt int64  `json:"received_at"`
}

type canonicalPayload struct {
	AcquirerBankID    string `json:"acquirer_bank_id"`
	AmountMinor       int64  `json:"amount_minor"`
	AuthorizationDate string `json:"authorization_date"`
	CloseDay          string `json:"close_day"`
	Currency          string `json:"currency"`
	EventTimeMS       int64  `json:"event_time_ms"`
	IssuerBankID      string `json:"issuer_bank_id"`
	Kind              string `json:"kind"`
	LifecycleSeq      int    `json:"lifecycle_seq"`
	MerchantID        string `json:"merchant_id"`
	ParentEventID     string `json:"parent_event_id"`
	ReferenceVersion  string `json:"reference_version"`
	RunID             string `json:"run_id"`
	SealEpoch         int64  `json:"seal_epoch"`
	SourceSeq         int64  `json:"source_seq"`
	TransactionID     string `json:"transaction_id"`
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Canonical returns sorted, compact UTF-8 JSON of immutable business fields.
func Canonical(e Event) []byte {
	p := canonicalPayload{
		AcquirerBankID:    e.AcquirerBankID,
		AmountMinor:       e.AmountMinor,
		AuthorizationDate: e.AuthorizationDate,
		CloseDay:          e.CloseDay,
		Currency:          e.Currency,
		EventTimeMS:       e.EventTimeMS,
		IssuerBankID:      e.IssuerBankID,
		Kind:              e.Kind,
		LifecycleSeq:      e.LifecycleSeq,
		MerchantID:        e.MerchantID,
		ParentEventID:     e.ParentEventID,
		ReferenceVersion:  e.ReferenceVersion,
		RunID:             e.RunID,
		SealEpoch:         e.SealEpoch,
		SourceSeq:         e.SourceSeq,
		TransactionID:     e.TransactionID,
	}
	data, _ := json.Marshal(p)
	return data
}

// New creates and validates a new lifecycle event.
func New(
	runID, txID string,
	sourceSeq int64,
	seq int,
	kind, parentID string,
	eventTimeMS, producedMS int64,
	authDate, curr string,
	amount int64,
	merchant, issuer, acquirer, refVer string,
	epoch int64,
	closeDay string,
) (Event, error) {
	e := Event{
		SchemaVersion:     1,
		EventID:           contract.ID(txID, kind, strconv.Itoa(seq)),
		RunID:             runID,
		TransactionID:     txID,
		SourceSeq:         sourceSeq,
		LifecycleSeq:      seq,
		Kind:              kind,
		ParentEventID:     parentID,
		EventTimeMS:       eventTimeMS,
		ProducedAtMS:      producedMS,
		AuthorizationDate: authDate,
		Currency:          curr,
		AmountMinor:       amount,
		MerchantID:        merchant,
		IssuerBankID:      issuer,
		AcquirerBankID:    acquirer,
		ReferenceVersion:  refVer,
		SealEpoch:         epoch,
		CloseDay:          closeDay,
	}
	e.PayloadHash = digest(Canonical(e))
	return e, e.Validate()
}

// Validate checks lifecycle event schema version, identifiers, values, and dates.
func (e Event) Validate() error {
	if e.SchemaVersion != 1 || e.SourceSeq < 0 || e.ProducedAtMS < 0 || e.AmountMinor <= 0 || e.Currency != "USD" || e.SealEpoch < 0 {
		return errors.New("invalid schema version, sequence, timestamp, seal epoch or money")
	}
	if e.LifecycleSeq != 2 && e.LifecycleSeq != 3 {
		return errors.New("lifecycle sequence must be 2 or 3")
	}
	if e.Kind != string(KindClear) && e.Kind != string(KindReverse) && e.Kind != string(KindChargeback) {
		return errors.New("invalid lifecycle kind")
	}
	for _, s := range []string{e.RunID, e.MerchantID, e.IssuerBankID, e.AcquirerBankID, e.ReferenceVersion} {
		if !contract.ValidName(s) {
			return errors.New("invalid business identifier")
		}
	}
	if !hashPattern.MatchString(e.TransactionID) || !hashPattern.MatchString(e.ParentEventID) {
		return errors.New("invalid transaction or parent event id")
	}
	if e.EventID != contract.ID(e.TransactionID, e.Kind, strconv.Itoa(e.LifecycleSeq)) {
		return errors.New("invalid event id")
	}
	if e.PayloadHash != digest(Canonical(e)) {
		return errors.New("invalid payload hash")
	}
	if _, err := time.Parse("2006-01-02", e.AuthorizationDate); err != nil {
		return errors.New("invalid authorization date")
	}
	if _, err := time.Parse("2006-01-02", e.CloseDay); err != nil {
		return errors.New("invalid close day")
	}
	return nil
}

// Decode parses and validates one lifecycle event from JSON.
func Decode(data []byte) (Event, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return Event{}, err
	}
	required := []string{
		"schema_version", "event_id", "run_id", "transaction_id", "source_seq",
		"lifecycle_seq", "kind", "parent_event_id", "event_time_ms", "produced_at_ms",
		"authorization_date", "currency", "amount_minor", "merchant_id", "issuer_bank_id",
		"acquirer_bank_id", "reference_version", "seal_epoch", "close_day", "payload_hash",
	}
	for _, key := range required {
		val, ok := raw[key]
		if !ok || bytes.Equal(bytes.TrimSpace(val), []byte("null")) {
			return Event{}, fmt.Errorf("missing or null %s", key)
		}
	}
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, err
	}
	return e, e.Validate()
}
