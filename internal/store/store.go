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

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store is a bounded connection pool to the gateway database.
type Store struct{ DB *sql.DB }

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
	return &Store{DB: db}, nil
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
