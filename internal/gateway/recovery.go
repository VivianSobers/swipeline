package gateway

import (
	"context"

	"github.com/VivianSobers/swipeline/internal/store"
)

// Recover finalizes, as fail-closed stand-ins, every admission whose scoring
// deadline passed without a final, page by page until none remain. It runs
// at startup and periodically, so a crash or a client that disconnected
// never leaves an authorization pending.
func Recover(ctx context.Context, s *store.Store) error {
	for {
		rows, err := s.Pending(ctx)
		if err != nil || len(rows) == 0 {
			return err
		}
		in := make([]store.Choice, len(rows))
		for i, r := range rows {
			in[i] = store.Choice{ID: r.Request.TransactionID}
		}
		if _, err = s.Finalize(ctx, in); err != nil {
			return err
		}
	}
}
