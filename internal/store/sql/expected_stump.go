package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type expectedStump struct {
	db     *sql.DB
	d      *dialect
	ttlSec int
}

var _ storepkg.ExpectedStumpStore = (*expectedStump)(nil)

func newExpectedStump(db *sql.DB, d *dialect, ttlSec int) *expectedStump {
	return &expectedStump{db: db, d: d, ttlSec: ttlSec}
}

// AddSubtreeIndex upserts (block, url, index) for each URL, re-stamping the TTL.
// Idempotent: a re-driven subtree re-adds its own index as a no-op (ON CONFLICT
// just refreshes expires_at). Run in one transaction so a subtree's URLs are
// recorded atomically.
func (s *expectedStump) AddSubtreeIndex(blockHash string, subtreeIndex int, callbackURLs []string) error {
	if len(callbackURLs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO expected_stumps (block_hash, callback_url, subtree_index, expires_at) VALUES (%s, %s, %s, %s)
        ON CONFLICT (block_hash, callback_url, subtree_index) DO UPDATE SET expires_at = EXCLUDED.expires_at`,
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3), s.d.intervalSeconds(s.ttlSec),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin expected-stump tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, url := range callbackURLs {
		if _, err := tx.ExecContext(ctx, q, blockHash, url, subtreeIndex); err != nil {
			return fmt.Errorf("record expected stump index for %s: %w", url, err)
		}
	}
	return tx.Commit()
}

// GetSubtreeIndices returns the ascending set of subtree indices for (block,
// URL), empty if none.
func (s *expectedStump) GetSubtreeIndices(blockHash, callbackURL string) ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`SELECT subtree_index FROM expected_stumps WHERE block_hash = %s AND callback_url = %s`,
		s.d.placeholder(1), s.d.placeholder(2),
	)
	rows, err := s.db.QueryContext(ctx, q, blockHash, callbackURL)
	if err != nil {
		return nil, fmt.Errorf("read expected stump indices: %w", err)
	}
	defer ensureRowsClosed(rows)

	var out []int
	for rows.Next() {
		var idx int
		if err := rows.Scan(&idx); err != nil {
			return nil, fmt.Errorf("scan expected stump index: %w", err)
		}
		out = append(out, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Ints(out)
	return out, nil
}
