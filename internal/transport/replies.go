package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/gateway"
	"github.com/VivianSobers/swipeline/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReplyPollRecords bounds one poll of proposals.
const ReplyPollRecords = 256

// Replies feeds proposals into arbitration as soon as they are fetched,
// without waiting for any streaming checkpoint. Each poll's records are
// arbitrated concurrently, so they share database batches; offsets are
// committed only once every record is final or dead-lettered. Any other
// error stops the loop with offsets uncommitted, so a restart redelivers.
func Replies(ctx context.Context, s *gateway.Service, c, p *kgo.Client) error {
	for {
		fetch := c.PollRecords(ctx, ReplyPollRecords)
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fetch.Err(); err != nil {
			return err
		}
		records := fetch.Records()
		errs := make([]error, len(records))
		var wg sync.WaitGroup
		for i, r := range records {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = arbitrate(ctx, s, p, r)
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return err
		}
		if len(records) > 0 {
			if err := c.CommitRecords(ctx, records...); err != nil {
				return err
			}
		}
	}
}

// arbitrate finalizes one reply, or dead-letters it if it is malformed,
// names an unknown transaction, or conflicts with the admitted request. A
// proposal after the deadline is not an error: the stored stand-in stands.
func arbitrate(ctx context.Context, s *gateway.Service, p *kgo.Client, r *kgo.Record) error {
	code := "INVALID_PROPOSAL"
	proposal, err := Proposal(r.Value)
	if err == nil {
		call, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		for {
			_, err = s.Finals.Submit(call, store.Choice{ID: proposal.TransactionID, Proposal: &proposal}, time.Now())
			if !errors.Is(err, gateway.ErrBusy) {
				break
			}
			select {
			case <-call.Done():
				return call.Err()
			case <-time.After(time.Millisecond):
			}
		}
		var pg *pgconn.PgError
		switch {
		case err == nil:
			return nil
		case errors.As(err, &pg) && pg.Code == "23505":
			code = "PROPOSAL_CONFLICT"
		case errors.As(err, &pg) && pg.Code == "P0002":
			code = "UNKNOWN_TRANSACTION"
		default:
			return err
		}
	}
	letter := &kgo.Record{Topic: "swipeline.dlq.v1", Key: r.Key, Partition: auth.Partition(r.Key),
		Value: DeadLetter(r.Topic, r.Partition, r.Offset, code, r.Value)}
	return p.ProduceSync(ctx, letter).FirstErr()
}
