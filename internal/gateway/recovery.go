package gateway

import (
	"context"

	"github.com/VivianSobers/swipeline/internal/store"
)

// Recover finalizes, as fail-closed stand-ins, every admission whose scoring
// deadline passed without a final, page by page until none remain. It runs
// at startup and periodically, so a crash or a client that disconnected
// never leaves an authorization pending.
func (s *Service) Recover(ctx context.Context) error {
	for {
		rows, err := s.DB.Pending(ctx)
		if err != nil || len(rows) == 0 {
			return err
		}
		in := make([]store.Choice, len(rows))
		for i, r := range rows {
			in[i] = store.Choice{ID: r.Request.TransactionID}
		}
		if _, err = s.finalize(ctx, in); err != nil {
			return err
		}
	}
}
