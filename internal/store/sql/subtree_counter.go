package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type subtreeCounter struct {
	db     *sql.DB
	d      *dialect
	ttlSec int
}

var _ storepkg.SubtreeCounterStore = (*subtreeCounter)(nil)

func newSubtreeCounter(db *sql.DB, d *dialect, ttlSec int) *subtreeCounter {
	return &subtreeCounter{db: db, d: d, ttlSec: ttlSec}
}

// Init upserts the counter with the initial remaining count and a fresh TTL.
func (s *subtreeCounter) Init(blockHash string, count int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO subtree_counters (block_hash, remaining, expires_at) VALUES (%s, %s, %s)
        ON CONFLICT (block_hash) DO UPDATE SET remaining = EXCLUDED.remaining, expires_at = EXCLUDED.expires_at`,
		s.d.placeholder(1), s.d.placeholder(2), s.d.intervalSeconds(s.ttlSec))
	_, err := s.db.ExecContext(ctx, q, blockHash, count)
	return err
}

// Decrement atomically decrements the remaining count and returns the new
// value. Uses UPDATE … RETURNING on PostgreSQL; on SQLite we fall back to a
// transaction with explicit read-then-write under BEGIN IMMEDIATE, which
// serializes writers.
func (s *subtreeCounter) Decrement(blockHash string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if isPostgres(s.d) {
		q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
			"UPDATE subtree_counters SET remaining = remaining - 1 WHERE block_hash = %s RETURNING remaining",
			s.d.placeholder(1))
		var remaining int
		if err := s.db.QueryRowContext(ctx, q, blockHash).Scan(&remaining); err != nil {
			return 0, err
		}
		return remaining, nil
	}

	// SQLite path.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var remaining int
	qSel := fmt.Sprintf("SELECT remaining FROM subtree_counters WHERE block_hash = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
	if err := tx.QueryRowContext(ctx, qSel, blockHash).Scan(&remaining); err != nil {
		return 0, err
	}
	remaining--
	qUp := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"UPDATE subtree_counters SET remaining = %s WHERE block_hash = %s",
		s.d.placeholder(1), s.d.placeholder(2))
	if _, err := tx.ExecContext(ctx, qUp, remaining, blockHash); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return remaining, nil
}
