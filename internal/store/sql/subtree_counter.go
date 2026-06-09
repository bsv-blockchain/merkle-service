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
// When data is non-nil it is JSON-encoded into the block_data column so the
// final Decrement can surface it on BLOCK_PROCESSED without a second query.
func (s *subtreeCounter) Init(blockHash string, count int, data *storepkg.BlockProcessedData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var blockData any // nil → NULL column
	if data != nil {
		encoded, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("encode block-processed data: %w", err)
		}
		blockData = string(encoded)
	}

	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO subtree_counters (block_hash, remaining, block_data, expires_at) VALUES (%s, %s, %s, %s)
        ON CONFLICT (block_hash) DO UPDATE SET remaining = EXCLUDED.remaining, block_data = EXCLUDED.block_data, expires_at = EXCLUDED.expires_at`,
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3), s.d.intervalSeconds(s.ttlSec),
	)
	_, err := s.db.ExecContext(ctx, q, blockHash, count, blockData)
	return err
}

// decodeBlockData unmarshals the block_data column (NULL → nil) and logs but
// never fails on a malformed value: a counter that drained but can't surface
// its block data still emits BLOCK_PROCESSED, and the consumer falls back to a
// datahub.
func decodeBlockData(raw sql.NullString) *storepkg.BlockProcessedData {
	if !raw.Valid || raw.String == "" {
		return nil
	}
	var d storepkg.BlockProcessedData
	if err := json.Unmarshal([]byte(raw.String), &d); err != nil {
		return nil
	}
	return &d
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
//
// TTL: like the Aerospike backend, Decrement re-stamps expires_at on every
// call so the counter survives ttlSec of *inactivity* rather than expiring
// ttlSec after Init (which would be a hard deadline on the whole block).
//
// Missing rows: if the counter row does not exist (sql.ErrNoRows), Decrement
// returns (0, storepkg.ErrCounterNotFound) — matching the Aerospike backend.
// The row may legitimately be absent because the sweeper purged an expired
// counter, an Init never ran (e.g. process crash between Init and message
// publish), or the row was already consumed by an earlier successful
// Decrement. The worker handles ErrCounterNotFound by acking without emitting
// BLOCK_PROCESSED and logging an ALERT for operator reprocess. Returning
// (0, nil) here was the previous behavior — it triggered a premature
// BLOCK_PROCESSED emission via the worker's `remaining <= 0` branch while
// other subtrees of the same block were still in flight (and on the
// Aerospike backend the same condition correctly logged an ALERT and
// suppressed the emit). The unbounded-redelivery-loop concern that justified
// the old behavior was eliminated when the worker started ack'ing on
// ErrCounterNotFound — see fix/subtree-counter-ttl-leak.
func (s *subtreeCounter) Decrement(blockHash string) (int, *storepkg.BlockProcessedData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if isPostgres(s.d) {
		// Re-stamp expires_at on every decrement so the counter expires only
		// after ttlSec of *inactivity*, mirroring the Aerospike backend. The
		// value written by Init is otherwise a hard deadline on the whole
		// block: a block that keeps making subtree progress longer than ttlSec
		// would have its counter swept mid-flight, after which Decrement maps
		// the missing row to ErrCounterNotFound and the worker acks the
		// remaining items without ever emitting BLOCK_PROCESSED.
		q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
			"UPDATE subtree_counters SET remaining = remaining - 1, expires_at = %s WHERE block_hash = %s RETURNING remaining, block_data",
			s.d.intervalSeconds(s.ttlSec), s.d.placeholder(1),
		)
		var remaining int
		var blockData sql.NullString
		if err := s.db.QueryRowContext(ctx, q, blockHash).Scan(&remaining, &blockData); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return 0, nil, storepkg.ErrCounterNotFound
			}
			return 0, nil, err
		}
		// Only surface the stashed block data on the final decrement.
		if remaining <= 0 {
			return remaining, decodeBlockData(blockData), nil
		}
		return remaining, nil, nil
	}

	// SQLite path. Pin a connection, open the transaction with `BEGIN
	// IMMEDIATE` (write lock acquired at BEGIN time), and manage commit /
	// rollback explicitly. database/sql.BeginTx would issue plain `BEGIN`,
	// which is deferred and reintroduces the read-modify-write race.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return 0, nil, fmt.Errorf("begin immediate: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	var remaining int
	var blockData sql.NullString
	qSel := fmt.Sprintf("SELECT remaining, block_data FROM subtree_counters WHERE block_hash = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
	if err := conn.QueryRowContext(ctx, qSel, blockHash).Scan(&remaining, &blockData); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, storepkg.ErrCounterNotFound
		}
		return 0, nil, err
	}
	remaining--
	// Re-stamp expires_at alongside remaining (see the Postgres branch above):
	// keeps the counter alive for ttlSec of inactivity rather than ttlSec
	// after Init, so a slow-draining block isn't swept mid-flight.
	qUp := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"UPDATE subtree_counters SET remaining = %s, expires_at = %s WHERE block_hash = %s",
		s.d.placeholder(1), s.d.intervalSeconds(s.ttlSec), s.d.placeholder(2),
	)
	if _, err := conn.ExecContext(ctx, qUp, remaining, blockHash); err != nil {
		return 0, nil, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return 0, nil, fmt.Errorf("commit: %w", err)
	}
	committed = true
	// Only surface the stashed block data on the final decrement.
	if remaining <= 0 {
		return remaining, decodeBlockData(blockData), nil
	}
	return remaining, nil, nil
}
