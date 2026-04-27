package sql

import (
	"context"
	"database/sql"
	"encoding/json"
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
	defer tx.Rollback()

	qParent := fmt.Sprintf(`INSERT INTO callback_accumulator (block_hash, expires_at) VALUES (%s, %s)
        ON CONFLICT (block_hash) DO UPDATE SET expires_at = EXCLUDED.expires_at`,
		s.d.placeholder(1), s.d.intervalSeconds(s.ttlSec))
	if _, err := tx.ExecContext(ctx, qParent, blockHash); err != nil {
		return fmt.Errorf("upsert accumulator: %w", err)
	}

	qEntry := fmt.Sprintf(`INSERT INTO callback_accumulator_entries (block_hash, callback_url, subtree_index, txids_json, stump_data)
        VALUES (%s, %s, %s, %s, %s)`,
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3), s.d.placeholder(4), s.d.placeholder(5))
	if _, err := tx.ExecContext(ctx, qEntry, blockHash, callbackURL, subtreeIndex, string(txidsJSON), stumpData); err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}
	return tx.Commit()
}

// ReadAndDelete reads every entry for blockHash, deletes them atomically,
// and groups them by callback URL. Safe to call once per block.
func (s *callbackAccumulator) ReadAndDelete(blockHash string) (map[string]*storepkg.AccumulatedCallback, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	qSel := fmt.Sprintf(`SELECT callback_url, subtree_index, txids_json, stump_data
        FROM callback_accumulator_entries WHERE block_hash = %s`, s.d.placeholder(1))
	rows, err := tx.QueryContext(ctx, qSel, blockHash)
	if err != nil {
		return nil, err
	}
	result := map[string]*storepkg.AccumulatedCallback{}
	for rows.Next() {
		var url, txidsJSON string
		var subtreeIndex int
		var stumpData []byte
		if err := rows.Scan(&url, &subtreeIndex, &txidsJSON, &stumpData); err != nil {
			rows.Close()
			return nil, err
		}
		var txids []string
		if err := json.Unmarshal([]byte(txidsJSON), &txids); err != nil {
			rows.Close()
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
		rows.Close()
		return nil, err
	}
	rows.Close()

	qDelEntries := fmt.Sprintf("DELETE FROM callback_accumulator_entries WHERE block_hash = %s", s.d.placeholder(1))
	if _, err := tx.ExecContext(ctx, qDelEntries, blockHash); err != nil {
		return nil, err
	}
	qDelParent := fmt.Sprintf("DELETE FROM callback_accumulator WHERE block_hash = %s", s.d.placeholder(1))
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
