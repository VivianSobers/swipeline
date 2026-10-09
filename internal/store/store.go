// Package store persists authorization admissions, final outcomes and their
// outbox events in PostgreSQL. Every final is chosen under a row lock against
// the database clock, so exactly one outcome can ever commit.
package store

import (
	"context"
	"database/sql"

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
