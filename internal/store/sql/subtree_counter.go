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
// value.
//
// Concurrency: a naive read-modify-write under SQLite's default deferred
// transaction (or PostgreSQL's READ COMMITTED) lets two callers both observe
// the same `remaining` value and write back the same decremented value,
// silently losing a decrement (F-052). To prevent that we acquire a write
// lock on the counter row before reading it:
//
//   - On PostgreSQL we use a single-statement UPDATE ... RETURNING. The
//     UPDATE takes a row-level write lock and returns the post-decrement
//     value atomically — equivalent to the SELECT ... FOR UPDATE pattern
//     used by callback_accumulator.go (PR #75).
//   - On SQLite we explicitly issue `BEGIN IMMEDIATE` on a pinned
//     connection. The default `BeginTx` issues `BEGIN` (deferred), which
//     only takes the write lock on the first write — leaving a window
//     between the SELECT and the UPDATE in which a second connection can
//     read the same value. `BEGIN IMMEDIATE` takes the database write
//     lock at BEGIN time, so concurrent Decrement callers serialize on
//     the lock. We can't use database/sql's TxOptions.Isolation here
//     because modernc.org/sqlite ignores it unless the connection was
//     opened with the `_txlock=immediate` URL parameter, and the rest of
//     the codebase opens connections without it.
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

	// SQLite path. Pin a connection, open the transaction with `BEGIN
	// IMMEDIATE` (write lock acquired at BEGIN time), and manage commit /
	// rollback explicitly. database/sql.BeginTx would issue plain `BEGIN`,
	// which is deferred and reintroduces the read-modify-write race.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return 0, fmt.Errorf("begin immediate: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	var remaining int
	qSel := fmt.Sprintf("SELECT remaining FROM subtree_counters WHERE block_hash = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
	if err := conn.QueryRowContext(ctx, qSel, blockHash).Scan(&remaining); err != nil {
		return 0, err
	}
	remaining--
	qUp := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"UPDATE subtree_counters SET remaining = %s WHERE block_hash = %s",
		s.d.placeholder(1), s.d.placeholder(2))
	if _, err := conn.ExecContext(ctx, qUp, remaining, blockHash); err != nil {
		return 0, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	committed = true
	return remaining, nil
}
