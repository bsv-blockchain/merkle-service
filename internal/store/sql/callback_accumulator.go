package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type callbackAccumulator struct {
	db     *sql.DB
	d      *dialect
	ttlSec int
}

var _ storepkg.CallbackAccumulatorStore = (*callbackAccumulator)(nil)

func newCallbackAccumulator(db *sql.DB, d *dialect, ttlSec int) *callbackAccumulator {
	return &callbackAccumulator{db: db, d: d, ttlSec: ttlSec}
}

// Append atomically inserts an entry for the block and refreshes its TTL.
func (s *callbackAccumulator) Append(blockHash, callbackURL string, txids []string, subtreeIndex int, stumpData []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txidsJSON, err := json.Marshal(txids)
	if err != nil {
		return fmt.Errorf("marshal txids: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	qParent := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO callback_accumulator (block_hash, expires_at) VALUES (%s, %s)
        ON CONFLICT (block_hash) DO UPDATE SET expires_at = EXCLUDED.expires_at`,
		s.d.placeholder(1), s.d.intervalSeconds(s.ttlSec))
	if _, err := tx.ExecContext(ctx, qParent, blockHash); err != nil {
		return fmt.Errorf("upsert accumulator: %w", err)
	}

	qEntry := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO callback_accumulator_entries (block_hash, callback_url, subtree_index, txids_json, stump_data)
        VALUES (%s, %s, %s, %s, %s)`,
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3), s.d.placeholder(4), s.d.placeholder(5))
	if _, err := tx.ExecContext(ctx, qEntry, blockHash, callbackURL, subtreeIndex, string(txidsJSON), stumpData); err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}
	return tx.Commit()
}

// ReadAndDelete reads every entry for blockHash, deletes them atomically,
// and groups them by callback URL. Safe to call once per block.
//
// Concurrency: under PostgreSQL's default READ COMMITTED isolation a naive
// SELECT-then-DELETE leaves a window in which a concurrent Append may insert
// an entries row that gets deleted by our DELETE but was never returned by
// our SELECT — silently dropping a callback batch (F-046).
//
// To close that window we:
//  1. Acquire a row lock on the parent callback_accumulator row with
//     SELECT ... FOR UPDATE (Postgres only). Append's
//     INSERT ... ON CONFLICT (block_hash) DO UPDATE on the same parent row
//     blocks until our transaction commits, so no Append for this block can
//     interleave between our read and delete.
//  2. Use DELETE ... RETURNING on the entries rows so the read and the
//     delete are a single statement; nothing can be inserted between them.
//
// On SQLite the database serializes writers globally (BEGIN IMMEDIATE
// semantics under WAL with a single connection in tests), so a separate
// FOR UPDATE is unnecessary; the transaction itself blocks Append.
func (s *callbackAccumulator) ReadAndDelete(blockHash string) (map[string]*storepkg.AccumulatedCallback, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// On Postgres, take a row-level lock on the parent first so any
	// concurrent Append for this block has to wait for us to commit before
	// it can upsert the parent row (and therefore before it can insert into
	// callback_accumulator_entries). If the parent row doesn't exist there
	// is nothing accumulated and no entries can exist either (Append always
	// upserts the parent before inserting an entry within the same txn).
	if isPostgres(s.d) {
		qLock := fmt.Sprintf("SELECT 1 FROM callback_accumulator WHERE block_hash = %s FOR UPDATE", s.d.placeholder(1)) //nolint:gosec // placeholder is from internal function
		var dummy int
		err = tx.QueryRowContext(ctx, qLock, blockHash).Scan(&dummy)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// No accumulator for this block: nothing to read or delete.
			// Commit the (empty) txn so the deferred Rollback is a no-op.
			if cerr := tx.Commit(); cerr != nil {
				return nil, cerr
			}
			return nil, nil
		case err != nil:
			return nil, fmt.Errorf("lock accumulator: %w", err)
		}
	}

	// Single-statement read+delete: DELETE ... RETURNING is supported by
	// PostgreSQL and SQLite >= 3.35 (modernc.org/sqlite is well past that).
	qDelEntries := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`DELETE FROM callback_accumulator_entries WHERE block_hash = %s
        RETURNING callback_url, subtree_index, txids_json, stump_data`, s.d.placeholder(1))
	rows, err := tx.QueryContext(ctx, qDelEntries, blockHash)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := map[string]*storepkg.AccumulatedCallback{}
	for rows.Next() {
		var url, txidsJSON string
		var subtreeIndex int
		var stumpData []byte
		if err := rows.Scan(&url, &subtreeIndex, &txidsJSON, &stumpData); err != nil {
			return nil, err
		}
		var txids []string
		if err := json.Unmarshal([]byte(txidsJSON), &txids); err != nil {
			return nil, fmt.Errorf("unmarshal txids: %w", err)
		}
		acc, ok := result[url]
		if !ok {
			acc = &storepkg.AccumulatedCallback{}
			result[url] = acc
		}
		acc.Entries = append(acc.Entries, storepkg.AccumulatedCallbackEntry{
			TxIDs:        txids,
			SubtreeIndex: subtreeIndex,
			StumpData:    stumpData,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	qDelParent := fmt.Sprintf("DELETE FROM callback_accumulator WHERE block_hash = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
	if _, err := tx.ExecContext(ctx, qDelParent, blockHash); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}
