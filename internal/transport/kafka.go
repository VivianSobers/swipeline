// Package transport moves gateway outbox rows to Kafka in per-partition
// order and feeds the streaming job's proposals back into arbitration.
package transport

import (
	"context"
	"strings"
	"time"

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
