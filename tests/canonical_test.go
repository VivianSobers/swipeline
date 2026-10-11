package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
