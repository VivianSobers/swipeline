// Package store persists authorization admissions, final outcomes and their
// outbox events in PostgreSQL. Every final is chosen under a row lock against
// the database clock, so exactly one outcome can ever commit.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store is a bounded connection pool to the gateway database. MaxRows and
// MaxBytes cap the unsent outbox plus the final rows reserved for pending
// authorizations; admission stops before either is reached.
type Store struct {
	DB       *sql.DB
	MaxRows  int64
	MaxBytes int64
}

// Open connects to dsn with at most 12 connections, within the server's 32.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{DB: db, MaxRows: 100000, MaxBytes: 512 << 20}, nil
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies each embedded migration once, in name order, inside one
// transaction serialized across processes by an advisory lock.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(72001)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations(name text PRIMARY KEY)"); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var done bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name=$1)", entry.Name()).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		script, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(script)); err != nil {
			return fmt.Errorf("%s: %w", entry.Name(), err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations VALUES($1)", entry.Name()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ErrRunConflict means a run ID was reused with a different manifest.
var ErrRunConflict = errors.New("run conflict")

// Register records r's manifest. Registering an identical manifest again is
// a no-op; any change under the same run ID is rejected.
func (s *Store) Register(ctx context.Context, r auth.Run) error {
	if err := r.Validate(); err != nil {
		return err
	}
	manifest, err := json.Marshal(r)
	if err != nil {
		return err
	}
	result, err := s.DB.ExecContext(ctx, `INSERT INTO runs(run_id, config_json, config_hash) VALUES($1, $2, $3)
		ON CONFLICT(run_id) DO UPDATE SET config_json=runs.config_json WHERE runs.config_hash=excluded.config_hash`,
		r.ID, manifest, contract.ID(string(manifest)))
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n != 1 {
		return ErrRunConflict
	}
	return nil
}

// Input is a client event with the gateway's admission wall time.
type Input struct {
	Event    contract.Event
	Received int64
}

// Row is a stored authorization and its final outcome, nil while pending.
type Row struct {
	Request auth.Request `json:"request"`
	Final   *auth.Final  `json:"final"`
}

// Lookup returns the stored authorization for a transaction ID.
func (s *Store) Lookup(ctx context.Context, id string) (Row, error) {
	var row Row
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT jsonb_build_object('request', request_json, 'final', final_json)
		FROM authorizations WHERE transaction_id=$1`, id).Scan(&data)
	if err != nil {
		return row, err
	}
	return row, json.Unmarshal(data, &row)
}

// Admit durably stores up to 32 events with their request outbox rows in one
// transaction. A retry returns the original receipt; a changed payload under
// the same transaction ID fails the whole batch. One global admission lock
// keeps per-card sequences in arrival order.
func (s *Store) Admit(ctx context.Context, in []Input) ([]Row, error) {
	if len(in) == 0 || len(in) > 32 {
		return nil, errors.New("admission batch size must be 1..32")
	}
	payloads := make([][]byte, len(in))
	ids := make([]string, len(in))
	for i, v := range in {
		if err := v.Event.Validate(); err != nil {
			return nil, err
		}
		data, err := json.Marshal(v.Event)
		if err != nil {
			return nil, err
		}
		payloads[i], ids[i] = data, v.Event.TransactionID
	}
	sort.Strings(ids)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(72002)"); err != nil {
		return nil, err
	}
	// Each admission adds a request outbox row now and reserves one final.
	var full bool
	if err = tx.QueryRowContext(ctx, pressureSQL, s.MaxRows-2*int64(len(in)), s.MaxBytes-2*reserve*int64(len(in))).Scan(&full); err != nil {
		return nil, err
	}
	if full {
		return nil, ErrFull
	}
	// Lock existing rows in ID order, the same order Finalize uses.
	for _, id := range ids {
		if _, err = tx.ExecContext(ctx, "SELECT 1 FROM authorizations WHERE transaction_id=$1 FOR UPDATE", id); err != nil {
			return nil, err
		}
	}
	out := make([]Row, len(in))
	for i, v := range in {
		key := auth.Key(v.Event.RunID, v.Event.CardToken)
		var result []byte
		if err = tx.QueryRowContext(ctx, "SELECT admit($1, $2, $3, $4)", payloads[i], v.Received, key, auth.Partition(key)).Scan(&result); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(result, &out[i]); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// ErrFull means admission stopped so pending finals can still be written.
var ErrFull = errors.New("outbox full")

// reserve is the byte budget held back for one outbox payload.
const reserve = 8192

const pressureSQL = `SELECT
	(SELECT count(*) FROM outbox WHERE sent_at IS NULL) + (SELECT count(*) FROM authorizations WHERE status='PENDING') >= $1
	OR (SELECT coalesce(sum(pg_column_size(payload)), 0) FROM outbox WHERE sent_at IS NULL)
	+ (SELECT count(*) * 8192 FROM authorizations WHERE status='PENDING') >= $2`

// Full reports whether the backlog has reached rows or bytes. It fails closed:
// a query error reports full.
func (s *Store) Full(ctx context.Context, rows, bytes int64) bool {
	var full bool
	err := s.DB.QueryRowContext(ctx, pressureSQL, rows, bytes).Scan(&full)
	return err != nil || full
}
