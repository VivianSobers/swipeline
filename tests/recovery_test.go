package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/authorize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func TestLateProposalAfterStandIn(t *testing.T) {
	c := installedClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runFault(t, "stop-taskmanager")
	cfg := installedConfig("late-proposal", 1, 20)
	if _, err := authorize.Run(ctx, cfg, c, "latency", 1); err != nil {
		t.Fatal(err)
	}
	before := installedFinals(t, c, cfg)[0]
	if before.Source != "STAND_IN" || before.Decision != "DECLINE" {
		t.Fatalf("expected durable stand-in: %+v", before)
	}
	runFault(t, "start-taskmanager")
	brokers := os.Getenv("SWIPELINE_KAFKA")
	if brokers == "" {
		t.Fatal("missing installed Kafka endpoint")
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics("swipeline.auth.proposals.v1"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.MaxVersions(kversion.V3_0_0()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var late *kgo.Record
	for late == nil {
		fetch := consumer.PollRecords(ctx, 256)
		if err := fetch.Err(); err != nil {
			t.Fatal(err)
		}
		for _, r := range fetch.Records() {
			var p auth.Proposal
			if json.Unmarshal(r.Value, &p) == nil && p.TransactionID == before.TransactionID {
				late = r
				break
			}
		}
	}
	for {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "swipeline-gateway-v1"
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: late.Topic, Partitions: []int32{late.Partition}}}
		response, err := consumer.Request(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		got := response.(*kmsg.OffsetFetchResponse)
		if got.ErrorCode != 0 {
			t.Fatalf("offset fetch error %d", got.ErrorCode)
		}
		consumed := false
		for _, topic := range got.Topics {
			for _, p := range topic.Partitions {
				if p.ErrorCode != 0 {
					t.Fatalf("partition offset error %d", p.ErrorCode)
				}
				if topic.Topic == late.Topic && p.Partition == late.Partition && p.Offset > late.Offset {
					consumed = true
				}
			}
		}
		if consumed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("late proposal was not consumed")
		case <-time.After(50 * time.Millisecond):
		}
	}
	after := installedFinals(t, c, cfg)[0]
	if !reflect.DeepEqual(before, after) {
		t.Fatal("late proposal changed the durable final")
	}
	t.Logf("late proposal consumed: partition=%d offset=%d; final unchanged", late.Partition, late.Offset)
}

// The host driver accepts only named faults and applies them to its own
// disposable project. Tests never receive a Docker socket.
func runFault(t *testing.T, action string) {
	t.Helper()
	dir := os.Getenv("SWIPELINE_FAULT_DIR")
	if dir == "" {
		t.Fatal("run through make m2-demo for fault control")
	}
	id := fmt.Sprint(time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(dir, "next"), []byte(id+" "+action+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "next"), filepath.Join(dir, "request")); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(3 * time.Minute)
	for time.Now().Before(until) {
		data, err := os.ReadFile(filepath.Join(dir, "done"))
		if err == nil && strings.HasPrefix(string(data), id+" ") {
			if string(data) != id+" 0\n" {
				t.Fatalf("fault %s failed: %s", action, data)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("fault %s timed out", action)
}
