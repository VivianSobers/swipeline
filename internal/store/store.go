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
	"github.com/VivianSobers/swipeline/internal/lifecycle"
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

// Manifest returns a registered run; sql.ErrNoRows if there is none.
func (s *Store) Manifest(ctx context.Context, id string) (auth.Run, error) {
	var r auth.Run
	var data []byte
	if err := s.DB.QueryRowContext(ctx, "SELECT config_json FROM runs WHERE run_id=$1", id).Scan(&data); err != nil {
		return r, err
	}
	return r, json.Unmarshal(data, &r)
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
// Fresh is set only on the Finalize result that committed the final.
type Row struct {
	Request auth.Request `json:"request"`
	Final   *auth.Final  `json:"final"`
	Fresh   bool         `json:"fresh"`
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

// Choice asks for the final outcome of transaction ID. A nil Proposal is the
// deadline timer: it finalizes a stand-in only once the deadline has passed.
type Choice struct {
	ID       string
	Proposal *auth.Proposal
}

// Finalize arbitrates up to 32 choices in one transaction, locking rows in
// ID order. Rows still before their deadline without a proposal come back
// with a nil Final. A commit error is returned as is: the outcome is then
// unknown and must be read back, never guessed.
func (s *Store) Finalize(ctx context.Context, in []Choice) ([]Row, error) {
	if len(in) == 0 || len(in) > 32 {
		return nil, errors.New("final batch size must be 1..32")
	}
	proposals := make([]any, len(in))
	for i, c := range in {
		if p := c.Proposal; p != nil {
			if p.ProposalID == "" || p.Flags == nil || (p.Decision != "APPROVE" && p.Decision != "DECLINE") {
				return nil, errors.New("malformed proposal")
			}
			data, err := json.Marshal(p)
			if err != nil {
				return nil, err
			}
			proposals[i] = data
		}
	}
	order := make([]int, len(in))
	for i := range in {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return in[order[a]].ID < in[order[b]].ID })
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	out := make([]Row, len(in))
	for _, i := range order {
		id := in[i].ID
		var result []byte
		err = tx.QueryRowContext(ctx, "SELECT finalize($1, $2, $3, $4)", id, proposals[i], contract.ID(id, "decision"), auth.Partition([]byte(id))).Scan(&result)
		if err != nil {
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

// Pending returns up to 32 admissions whose scoring deadline has passed on
// the database clock without a final, oldest deadline first. The recovery
// worker finalizes them as stand-ins after a restart or a lost timer.
func (s *Store) Pending(ctx context.Context) ([]Row, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT request_json FROM authorizations
		WHERE status='PENDING' AND scoring_deadline_at <= extract(epoch FROM clock_timestamp()) * 1000
		ORDER BY scoring_deadline_at, transaction_id LIMIT 32`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var data []byte
		var r Row
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &r.Request); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Backlog is the gateway's durable work not yet done.
type Backlog struct {
	Pending      int64   // admissions without a final
	Unsent       int64   // outbox rows not yet acknowledged by Kafka
	OldestUnsent float64 // seconds since the oldest unsent row was written
}

// Backlog reads pending and unsent counts and the oldest unsent age on the
// database clock.
func (s *Store) Backlog(ctx context.Context) (Backlog, error) {
	var b Backlog
	err := s.DB.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM authorizations WHERE status='PENDING'),
		count(*), coalesce(extract(epoch FROM clock_timestamp() - min(created_at)), 0)::float8
		FROM outbox WHERE sent_at IS NULL`).Scan(&b.Pending, &b.Unsent, &b.OldestUnsent)
	return b, err
}

// Counts returns decision counts for runID.
func (s *Store) Counts(ctx context.Context, runID string) (auth.RunCounts, error) {
	var c auth.RunCounts
	c.RunID = runID
	err := s.DB.QueryRowContext(ctx, `
SELECT
	count(a.transaction_id),
	count(a.transaction_id) FILTER (WHERE a.status = 'FINAL'),
	count(a.transaction_id) FILTER (WHERE a.status = 'PENDING'),
	count(a.transaction_id) FILTER (WHERE a.final_json->>'decision_source' = 'STAND_IN')
FROM runs r
LEFT JOIN authorizations a ON a.run_id = r.run_id
WHERE r.run_id = $1
GROUP BY r.run_id`, runID).Scan(&c.Admitted, &c.Final, &c.Pending, &c.StandIn)
	return c, err
}

// Lifecycle error sentinels.
var (
	ErrUnknownTransaction = errors.New("unknown transaction")
	ErrDeclinedLifecycle  = errors.New("cannot apply lifecycle to declined transaction")
	ErrLifecycleConflict  = errors.New("lifecycle conflict")
	ErrLifecycleMismatch  = errors.New("lifecycle payload mismatch")
)

type failBeforeCommitKey struct{}

// FailBeforeCommitInject returns a context that forces ReceiveLifecycle to fail immediately before commit.
func FailBeforeCommitInject(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, failBeforeCommitKey{}, err)
}

// ReceiveLifecycle admits a lifecycle transition event (clear, reverse, chargeback),
// persists the admission receipt, and enqueues an outbox row for downstream streaming.
func (s *Store) ReceiveLifecycle(ctx context.Context, e lifecycle.Event) (lifecycle.Receipt, error) {
	if err := e.Validate(); err != nil {
		return lifecycle.Receipt{}, err
	}
	if s.DB == nil {
		return lifecycle.Receipt{}, errors.New("store db is nil")
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return lifecycle.Receipt{}, err
	}
	defer tx.Rollback()

	// 1. Query authorization under exclusive row lock.
	var requestJSONRaw, finalJSONRaw []byte
	var authSealEpoch int64
	var authCloseDay string
	err = tx.QueryRowContext(ctx, `
		SELECT seal_epoch, close_day::text, request_json, final_json
		FROM authorizations
		WHERE transaction_id = $1
		FOR UPDATE
	`, e.TransactionID).Scan(&authSealEpoch, &authCloseDay, &requestJSONRaw, &finalJSONRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return lifecycle.Receipt{}, ErrUnknownTransaction
		}
		return lifecycle.Receipt{}, err
	}

	// 2. Reject if authorization outcome is missing or not APPROVE.
	if len(finalJSONRaw) == 0 {
		return lifecycle.Receipt{}, ErrDeclinedLifecycle
	}
	var finalObj struct {
		Decision string `json:"decision"`
	}
	if err = json.Unmarshal(finalJSONRaw, &finalObj); err != nil {
		return lifecycle.Receipt{}, fmt.Errorf("decode final_json: %w", err)
	}
	if finalObj.Decision != "APPROVE" {
		return lifecycle.Receipt{}, ErrDeclinedLifecycle
	}

	// 3. Verify amount, currency, and bank IDs match authorization.
	var reqObj struct {
		RunID          string `json:"run_id"`
		CardToken      string `json:"card_token"`
		Currency       string `json:"currency"`
		AmountMinor    int64  `json:"amount_minor"`
		IssuerBankID   string `json:"issuer_bank_id"`
		AcquirerBankID string `json:"acquirer_bank_id"`
	}
	if err = json.Unmarshal(requestJSONRaw, &reqObj); err != nil {
		return lifecycle.Receipt{}, fmt.Errorf("decode request_json: %w", err)
	}
	if reqObj.AmountMinor != e.AmountMinor || reqObj.Currency != e.Currency ||
		reqObj.IssuerBankID != e.IssuerBankID || reqObj.AcquirerBankID != e.AcquirerBankID {
		return lifecycle.Receipt{}, ErrLifecycleMismatch
	}

	// 4. Check if event_id already exists in lifecycle_receipts.
	var existingHash string
	var existingSealEpoch int64
	var existingCloseDay string
	var existingReceivedAt int64
	err = tx.QueryRowContext(ctx, `
		SELECT payload_hash, seal_epoch, close_day::text, floor(extract(epoch FROM created_at)*1000)::bigint
		FROM lifecycle_receipts
		WHERE event_id = $1
	`, e.EventID).Scan(&existingHash, &existingSealEpoch, &existingCloseDay, &existingReceivedAt)
	if err == nil {
		if existingHash != e.PayloadHash {
			return lifecycle.Receipt{}, ErrLifecycleConflict
		}
		return lifecycle.Receipt{
			EventID:    e.EventID,
			SealEpoch:  existingSealEpoch,
			CloseDay:   existingCloseDay,
			ReceivedAt: existingReceivedAt,
		}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return lifecycle.Receipt{}, err
	}

	// 5. Insert into lifecycle_receipts.
	payloadJSON, err := json.Marshal(e)
	if err != nil {
		return lifecycle.Receipt{}, err
	}

	var receivedAt int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO lifecycle_receipts(event_id, transaction_id, payload_hash, payload_json, seal_epoch, close_day)
		VALUES($1, $2, $3, $4, $5, $6::date)
		RETURNING floor(extract(epoch FROM created_at)*1000)::bigint
	`, e.EventID, e.TransactionID, e.PayloadHash, payloadJSON, authSealEpoch, authCloseDay).Scan(&receivedAt)
	if err != nil {
		return lifecycle.Receipt{}, err
	}

	// 6. Insert into outbox with topic swipeline.lifecycle.v1 and partition derived from card hash.
	cardKey := auth.Key(reqObj.RunID, reqObj.CardToken)
	part := auth.Partition(cardKey)

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox(event_id, topic, key, partition, payload)
		VALUES($1, 'swipeline.lifecycle.v1', $2, $3, $4)
	`, e.EventID, []byte(e.TransactionID), part, payloadJSON)
	if err != nil {
		return lifecycle.Receipt{}, err
	}

	_, _ = tx.ExecContext(ctx, `UPDATE runs SET lifecycle_count = lifecycle_count + 1 WHERE run_id = $1`, reqObj.RunID)

	if injected, ok := ctx.Value(failBeforeCommitKey{}).(error); ok && injected != nil {
		return lifecycle.Receipt{}, injected
	}

	if err = tx.Commit(); err != nil {
		return lifecycle.Receipt{}, err
	}

	return lifecycle.Receipt{
		EventID:    e.EventID,
		SealEpoch:  authSealEpoch,
		CloseDay:   authCloseDay,
		ReceivedAt: receivedAt,
	}, nil
}
