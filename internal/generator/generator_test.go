package generator

import (
	"sync"
	"testing"

	"github.com/VivianSobers/swipeline/internal/contract"
)

func TestDeterministicAcrossConcurrency(t *testing.T) {
	cfg := Config{"demo", 42, "2026-10-08", 6000}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	serial := make([]contract.Event, 6000)
	concurrent := make([]contract.Event, 6000)
	for i := range serial {
		serial[i], err = g.At(int64(i), 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < len(concurrent); i += 8 {
				e, eerr := g.At(int64(i), 999)
				if eerr != nil {
					t.Error(eerr)
					return
				}
				concurrent[i] = e
			}
		}(worker)
	}
	wg.Wait()
	for i, a := range serial {
		b := concurrent[i]
		if a.EventID != b.EventID || a.PayloadHash != b.PayloadHash || a.TransactionID != b.TransactionID {
			t.Fatalf("ordinal %d changed", i)
		}
		if err := a.Validate(); err != nil {
			t.Fatal(err)
		}
		if len(a.CardToken) != 69 || a.CardToken[:5] != "card_" {
			t.Fatal("not a synthetic token")
		}
	}
	cfg.Seed++
	other, _ := New(cfg)
	changed, _ := other.At(0, 1)
	if changed.TransactionID == serial[0].TransactionID {
		t.Fatal("seed absent from identity")
	}
}

func TestEnumerationAndVelocityFixtures(t *testing.T) {
	g, _ := New(Config{"attacks", 42, "2026-10-08", 6000})
	cards := map[string]bool{}
	first, _ := g.At(900, 1)
	last, _ := g.At(979, 1)
	for i := int64(900); i < 980; i++ {
		e, _ := g.At(i, 1)
		cards[e.CardToken] = true
		if e.MerchantID != first.MerchantID || e.AmountMinor > 200 {
			t.Fatal("invalid enumeration burst")
		}
	}
	if len(cards) != 80 || last.EventTimeMS-first.EventTimeMS >= 60000 {
		t.Fatal("enumeration lost distinctness/window")
	}
	first, _ = g.At(880, 1)
	last, _ = g.At(889, 1)
	if first.CardToken != last.CardToken || last.EventTimeMS-first.EventTimeMS >= 60000 {
		t.Fatal("velocity fixture lost")
	}
}

func TestConfigAndOrdinalBounds(t *testing.T) {
	for _, c := range []Config{{"../x", 1, "2026-10-08", 1}, {"ok", 1, "bad", 1}, {"ok", 1, "2026-10-08", 0}, {"ok", 1, "2026-10-08", 100001}} {
		if _, err := New(c); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	g, _ := New(Config{"ok", 1, "2026-10-08", 1})
	for _, i := range []int64{-1, 1} {
		if _, err := g.At(i, 1); err == nil {
			t.Fatal("ordinal accepted")
		}
	}
}
