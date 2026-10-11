package lifecycle

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"strconv"

	"github.com/VivianSobers/swipeline/internal/auth"
)

// Scheduler schedules deterministic lifecycle events for authorizations.
type Scheduler struct {
	Seed        uint64
	Day         string
	RuleVersion string
	RefVersion  string
}

func (s *Scheduler) choice(txID string, field string) uint64 {
	data, _ := json.Marshal([]string{"lifecycle-scheduler-v1", strconv.FormatUint(s.Seed, 10), txID, field})
	sum := sha256.Sum256(data)
	return binary.BigEndian.Uint64(sum[:8])
}

// Schedule produces deterministic lifecycle events for the given authorization outcome.
// Declines produce zero events. Approvals produce sequence 2 (pre-clearing reverse or clear)
// and optional sequence 3 (chargeback or post-clearing reverse).
func (s *Scheduler) Schedule(f auth.Final, ordinal int64) []Event {
	if f.Decision != "APPROVE" {
		return nil
	}

	day := s.Day
	if day == "" {
		day = f.CloseDay
	}
	if day == "" {
		day = f.AuthorizationDate
	}

	refVer := s.RefVersion
	if refVer == "" {
		refVer = f.ReferenceVersion
	}
	if refVer == "" {
		refVer = "synthetic-reference-v1"
	}

	// Choice for Sequence 2:
	// 5% pre-clearing reversal, 95% clearing.
	c2 := s.choice(f.TransactionID, "seq2") % 10000
	if c2 < 500 {
		// Pre-clearing reversal (5%)
		revEvt, err := New(
			f.RunID,
			f.TransactionID,
			ordinal,
			2,
			string(KindReverse),
			f.EventID,
			f.EventTimeMS+3600000,
			f.EventTimeMS+3600000,
			f.AuthorizationDate,
			f.Currency,
			f.AmountMinor,
			f.MerchantID,
			f.IssuerBankID,
			f.AcquirerBankID,
			refVer,
			f.Epoch,
			day,
		)
		if err != nil {
			return nil
		}
		return []Event{revEvt}
	}

	// Clearing (95%)
	clearEvt, err := New(
		f.RunID,
		f.TransactionID,
		ordinal,
		2,
		string(KindClear),
		f.EventID,
		f.EventTimeMS+3600000,
		f.EventTimeMS+3600000,
		f.AuthorizationDate,
		f.Currency,
		f.AmountMinor,
		f.MerchantID,
		f.IssuerBankID,
		f.AcquirerBankID,
		refVer,
		f.Epoch,
		day,
	)
	if err != nil {
		return nil
	}

	// Choice for Sequence 3:
	// 1% chargeback, 0.5% post-clearing reversal, 98.5% settled with no seq 3.
	c3 := s.choice(f.TransactionID, "seq3") % 10000
	if c3 < 100 {
		// Chargeback (1%)
		cbEvt, err := New(
			f.RunID,
			f.TransactionID,
			ordinal,
			3,
			string(KindChargeback),
			clearEvt.EventID,
			clearEvt.EventTimeMS+7200000,
			clearEvt.EventTimeMS+7200000,
			f.AuthorizationDate,
			f.Currency,
			f.AmountMinor,
			f.MerchantID,
			f.IssuerBankID,
			f.AcquirerBankID,
			refVer,
			f.Epoch,
			day,
		)
		if err == nil {
			return []Event{clearEvt, cbEvt}
		}
	} else if c3 < 150 {
		// Post-clearing reversal (0.5%)
		postRevEvt, err := New(
			f.RunID,
			f.TransactionID,
			ordinal,
			3,
			string(KindReverse),
			clearEvt.EventID,
			clearEvt.EventTimeMS+7200000,
			clearEvt.EventTimeMS+7200000,
			f.AuthorizationDate,
			f.Currency,
			f.AmountMinor,
			f.MerchantID,
			f.IssuerBankID,
			f.AcquirerBankID,
			refVer,
			f.Epoch,
			day,
		)
		if err == nil {
			return []Event{clearEvt, postRevEvt}
		}
	}

	return []Event{clearEvt}
}
