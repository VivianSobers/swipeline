// Package generator produces a seeded synthetic card day. Every field is a
// pure function of (seed, ordinal, field name), so output never depends on
// goroutine scheduling, replay rate or worker count.
package generator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
)

const (
	Version   = "synthetic-v1"
	Rules     = "preview-rules-v1"
	Reference = "synthetic-reference-v1"
)

// Config is the business identity of a generated run.
type Config struct {
	RunID  string `json:"run_id"`
	Seed   uint64 `json:"seed"`
	Day    string `json:"business_date"`
	Events int64  `json:"events"`
}

// Validate rejects unsafe run IDs, unparseable days and out-of-range sizes.
func (c Config) Validate() error {
	d, err := time.Parse("2006-01-02", c.Day)
	if err != nil || d.Year() < 2000 || d.Year() > 2100 || !contract.ValidName(c.RunID) || c.Events < 1 || c.Events > 100000 {
		return errors.New("require safe run ID, day in 2000..2100 and 1..100000 events")
	}
	return nil
}

// Identity hashes everything that determines generated business content.
// Transport choices such as TPS or output name are deliberately excluded.
func (c Config) Identity() string {
	return contract.ID(c.RunID, strconv.FormatUint(c.Seed, 10), c.Day, strconv.FormatInt(c.Events, 10), Version, Rules, Reference)
}

// Generator is immutable after New and safe for concurrent use.
type Generator struct {
	cfg      Config
	start    int64
	identity string
}

func New(c Config) (*Generator, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	day, _ := time.Parse("2006-01-02", c.Day)
	return &Generator{c, day.UnixMilli(), c.Identity()}, nil
}

func (g *Generator) choice(i int64, field string) uint64 {
	data, _ := json.Marshal([]string{Version, strconv.FormatUint(g.cfg.Seed, 10), strconv.FormatInt(i, 10), field})
	sum := sha256.Sum256(data)
	return binary.BigEndian.Uint64(sum[:8])
}

// At returns the event at ordinal i. producedAtMS is transport time only.
func (g *Generator) At(i int64, produced int64) (contract.Event, error) {
	if i < 0 || i >= g.cfg.Events {
		return contract.Event{}, errors.New("ordinal outside run")
	}
	card := g.choice(i, "card") % 2000
	merchant := g.choice(i, "merchant") % 100
	if g.choice(i, "popularity")%10 < 7 {
		merchant %= 10
	}
	// Lower ticket values at food/fuel merchants; repeatable card-local baseline.
	mccs := [4]string{"5411", "5812", "5541", "5732"}
	bases := [4]int64{3500, 2200, 6000, 40000}
	category := merchant % 4
	baseline := int64(50 + g.choice(int64(card), "usual")%101)
	amount := bases[category]*baseline/100 + int64(g.choice(i, "amount")%501)
	if g.choice(i, "spend-spike")%100 == 0 {
		amount *= 8
	}
	// Map 10% of traffic into 00:00-06:00, 70% into 06:00-18:00 and 20%
	// into 18:00-24:00. Event time is independent of replay TPS.
	position := i * 1000000 / g.cfg.Events
	var offset int64
	if position < 100000 {
		offset = position * 21600000 / 100000
	} else if position < 800000 {
		offset = 21600000 + (position-100000)*43200000/700000
	} else {
		offset = 64800000 + (position-800000)*21600000/200000
	}
	local := i % 1000
	if local >= 880 && local < 980 {
		// Each full block has a velocity burst followed by 80 distinct cards.
		anchor := (i / 1000) * 86400000 / ((g.cfg.Events + 999) / 1000)
		offset = anchor + 30000 + (local-880)*200
		merchant = 0
		category = 0
		amount = 100
		if local < 900 {
			card = uint64((i/1000)*100) % 2000
		} else {
			card = uint64((i/1000)*100+local-900) % 2000
		}
	}
	b := contract.Business{RunID: g.cfg.RunID, TransactionID: contract.ID(g.identity, strconv.FormatInt(i, 10)),
		SourceSeq: i, EventTimeMS: g.start + offset, CardToken: "card_" + contract.ID(g.identity, "card", strconv.FormatUint(card, 10)),
		MerchantID: fmt.Sprintf("merchant-%03d", merchant), MCC: mccs[category],
		IssuerBankID: fmt.Sprintf("bank-%d", card%4), AcquirerBankID: fmt.Sprintf("bank-%d", merchant%4),
		Currency: "USD", AmountMinor: amount, AuthorizationDate: g.cfg.Day, RuleVersion: Rules, ReferenceVersion: Reference}
	return contract.New(b, produced)
}
