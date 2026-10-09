// Package contract defines the versioned JSON event envelope shared by
// every swipeline component, with deterministic identities and hashes.
package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var cardPattern = regexp.MustCompile(`^card_[0-9a-f]{64}$`)
var mccPattern = regexp.MustCompile(`^[0-9]{4}$`)

// ValidName reports whether s is a safe run, merchant, bank or version name.
func ValidName(s string) bool { return namePattern.MatchString(s) }

// Business holds the immutable business fields of an authorization request.
// Field order is part of canonical business serialization version 1.
type Business struct {
	RunID             string `json:"run_id"`
	TransactionID     string `json:"transaction_id"`
	SourceSeq         int64  `json:"source_seq"`
	EventTimeMS       int64  `json:"event_time_ms"`
	CardToken         string `json:"card_token"`
	MerchantID        string `json:"merchant_id"`
	MCC               string `json:"mcc"`
	IssuerBankID      string `json:"issuer_bank_id"`
	AcquirerBankID    string `json:"acquirer_bank_id"`
	Currency          string `json:"currency"`
	AmountMinor       int64  `json:"amount_minor"`
	AuthorizationDate string `json:"authorization_date"`
	RuleVersion       string `json:"rule_version"`
	ReferenceVersion  string `json:"reference_version"`
}

// Event is a schema version 1 envelope around a Business payload.
type Event struct {
	SchemaVersion int    `json:"schema_version"`
	EventID       string `json:"event_id"`
	Business
	ProducedAtMS int64  `json:"produced_at_ms"`
	PayloadHash  string `json:"payload_hash"`
}

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// ID hashes the JSON array of parts, so ("ab","c") and ("a","bc") differ.
func ID(parts ...string) string { data, _ := json.Marshal(parts); return digest(data) }

// Canonical returns the compact JSON of b used for the payload hash.
func Canonical(b Business) []byte { data, _ := json.Marshal(b); return data }

// New wraps b in a validated envelope. producedAtMS is transport time and
// never affects identity or the payload hash.
func New(b Business, producedAtMS int64) (Event, error) {
	e := Event{1, ID(b.TransactionID, "authorization", "1"), b, producedAtMS, digest(Canonical(b))}
	return e, e.Validate()
}

// Validate checks field formats, money, dates and identity consistency.
func (e Event) Validate() error {
	day, err := time.Parse("2006-01-02", e.AuthorizationDate)
	if err != nil || e.EventTimeMS < day.UnixMilli() || e.EventTimeMS >= day.Add(24*time.Hour).UnixMilli() {
		return errors.New("event time outside authorization day")
	}
	if e.SchemaVersion != 1 || e.SourceSeq < 0 || e.ProducedAtMS < 0 || e.AmountMinor <= 0 || e.Currency != "USD" {
		return errors.New("invalid schema, sequence, timestamp or money")
	}
	for _, s := range []string{e.RunID, e.MerchantID, e.IssuerBankID, e.AcquirerBankID, e.RuleVersion, e.ReferenceVersion} {
		if !ValidName(s) {
			return errors.New("invalid business identifier")
		}
	}
	if !hashPattern.MatchString(e.TransactionID) || !cardPattern.MatchString(e.CardToken) || !mccPattern.MatchString(e.MCC) {
		return errors.New("invalid transaction, card token or MCC")
	}
	if e.EventID != ID(e.TransactionID, "authorization", "1") || e.PayloadHash != digest(Canonical(e.Business)) {
		return errors.New("identity or payload hash mismatch")
	}
	return nil
}

// Decode parses one event, rejecting missing or null required fields,
// non-integer or overflowing money and trailing data. Unknown optional
// fields are accepted.
func Decode(data []byte) (Event, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return Event{}, err
	}
	required := []string{"schema_version", "event_id", "run_id", "transaction_id", "source_seq", "event_time_ms",
		"produced_at_ms", "payload_hash", "card_token", "merchant_id", "mcc", "issuer_bank_id", "acquirer_bank_id",
		"currency", "amount_minor", "authorization_date", "rule_version", "reference_version"}
	for _, key := range required {
		value, ok := raw[key]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return Event{}, fmt.Errorf("missing or null %s", key)
		}
	}
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, err
	}
	return e, e.Validate()
}
