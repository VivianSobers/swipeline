package transport

import (
	"context"
	"errors"
	"net"
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
// error retries the same records with offsets uncommitted until the loop
// context ends. Repeated proposals preserve the stored final outcome.
func Replies(ctx context.Context, s *gateway.Service, c, p *kgo.Client) (err error) {
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	for {
		fetch := c.PollRecords(ctx, ReplyPollRecords)
		if err := ctx.Err(); err != nil {
			return err
		}
		records := fetch.Records()
		errs := make([]error, len(records))
		var wg sync.WaitGroup
		for i, r := range records {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = retryReply(ctx, func() error { return arbitrate(ctx, s, p, r) })
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return err
		}
		if len(records) > 0 {
			if err := retryReply(ctx, func() error { return c.CommitRecords(ctx, records...) }); err != nil {
				return err
			}
		}
		// A fetch may contain both records and partition errors. Process the
		// records before polling again, and back off on errors without skipping them.
		if fetch.Err() != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

// retryReply retains the operation in this poll until it succeeds. In particular,
// a failed arbitration is never followed by a commit or a poll past its record.
func retryReply(ctx context.Context, operation func() error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := operation(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
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
			var timeout net.Error
			if !errors.Is(err, gateway.ErrBusy) && !errors.Is(err, context.DeadlineExceeded) && !(errors.As(err, &timeout) && timeout.Timeout()) {
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
