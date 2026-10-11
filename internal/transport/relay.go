package transport

import (
	"context"
	"sync"
	"time"

	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Prefix is the length of the leading run of acknowledged records. Rows
// after the first failure stay unsent so a lane is never reordered.
func Prefix(ok []bool) int {
	for i, v := range ok {
		if !v {
			return i
		}
	}
	return len(ok)
}

// LifecycleTopic is the Kafka topic name for lifecycle events.
const LifecycleTopic = "swipeline.lifecycle.v1"

// IsRelayableTopic reports whether a topic in the outbox should be forwarded to Kafka.
func IsRelayableTopic(topic string) bool {
	switch topic {
	case "swipeline.auth.requests.v1", "swipeline.decisions.raw.v1", LifecycleTopic:
		return true
	default:
		return false
	}
}

type lane struct {
	topic     string
	partition int32
}

type item struct {
	id     int64
	record *kgo.Record
}

// RelayOnce sends up to 256 of the oldest unsent outbox rows, each lane (a
// topic partition) in outbox order, waits for every acknowledgement, then
// marks only each lane's acknowledged prefix sent, with its broker offset, in
// one transaction. It returns how many rows it marked. There must be exactly
// one relay per database.
func RelayOnce(ctx context.Context, s *store.Store, p *kgo.Client) (int, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT outbox_id, topic, key, partition, payload FROM outbox WHERE sent_at IS NULL ORDER BY outbox_id LIMIT 256")
	if err != nil {
		return 0, err
	}
	lanes := map[lane][]item{}
	for rows.Next() {
		var it item
		it.record = &kgo.Record{}
		if err = rows.Scan(&it.id, &it.record.Topic, &it.record.Key, &it.record.Partition, &it.record.Value); err != nil {
			rows.Close()
			return 0, err
		}
		if !IsRelayableTopic(it.record.Topic) {
			continue
		}
		l := lane{it.record.Topic, it.record.Partition}
		lanes[l] = append(lanes[l], it)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(lanes) == 0 {
		return 0, err
	}
	var mu sync.Mutex
	var acked []item
	var wg sync.WaitGroup
	for _, batch := range lanes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok := make([]bool, len(batch))
			var sent sync.WaitGroup
			for i, it := range batch {
				sent.Add(1)
				p.Produce(ctx, it.record, func(_ *kgo.Record, err error) {
					ok[i] = err == nil
					sent.Done()
				})
			}
			sent.Wait()
			mu.Lock()
			acked = append(acked, batch[:Prefix(ok)]...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(acked) == 0 {
		return 0, ctx.Err()
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, it := range acked {
		if _, err = tx.ExecContext(ctx, "UPDATE outbox SET sent_at=clock_timestamp(), broker_offset=$2 WHERE outbox_id=$1 AND sent_at IS NULL", it.id, it.record.Offset); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(acked), nil
}

// Relay sends outbox rows until ctx ends: immediately while rows remain,
// then on each wake signal, and every 100 ms in case a signal was missed or
// an earlier send failed. Errors are reported and retried after a pause;
// unacknowledged rows stay unsent and keep their order.
func Relay(ctx context.Context, s *store.Store, p *kgo.Client, wake <-chan struct{}, report func(error)) error {
	for {
		n, err := RelayOnce(ctx, s, p)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			report(err)
		} else if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(100 * time.Millisecond):
		}
	}
}
