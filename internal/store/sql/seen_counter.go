package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type seenCounter struct {
	db        *sql.DB
	d         *dialect
	threshold int
}

var _ storepkg.SeenCounterStore = (*seenCounter)(nil)

func newSeenCounter(db *sql.DB, d *dialect, threshold int) *seenCounter {
	return &seenCounter{db: db, d: d, threshold: threshold}
}

func (s *seenCounter) Threshold() int { return s.threshold }

// BatchIncrement applies Increment to each txid in turn. The SQL backend
// serves tests and small single-node deployments, so per-txid transactions
// are acceptable here; the Aerospike backend is the one with the true batched
// implementation (throughput review F-4). Same partial-success contract:
// results for every txid that succeeded, plus the first error (F-058).
func (s *seenCounter) BatchIncrement(txids []string, subtreeID string) (map[string]*storepkg.IncrementResult, error) {
	results := make(map[string]*storepkg.IncrementResult, len(txids))
	var firstErr error
	for _, txid := range txids {
		res, err := s.Increment(txid, subtreeID)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("batch increment %s: %w", txid, err)
			}
			continue
		}
		results[txid] = res
	}
	return results, firstErr
}

// Increment inserts (txid, subtreeID) into seen_counter_subtrees (idempotent
// via the compound PK), counts distinct subtrees for the txid, and atomically
// transitions threshold_fired from 0 to 1 on the first call that reaches the
// threshold. Exactly one caller observes ThresholdReached=true.
//
// F-045: previously the read-then-update of threshold_fired could race when
// two concurrent callers both saw fired=0 with count >= threshold and each
// independently set fired=1, both reporting ThresholdReached=true. The fix
// flips the flag with a single conditional `UPDATE … RETURNING`-style
// statement (or, on SQLite, an UPDATE that filters on the prior value and
// then inspects RowsAffected) so the transition is observed by exactly one
// caller.
func (s *seenCounter) Increment(txid, subtreeID string) (*storepkg.IncrementResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Ensure the parent counter row exists (no-op if already there).
	qIns := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO seen_counters (txid) VALUES (%s)%s",
		s.d.placeholder(1), s.d.onConflictDoNothing,
	)
	if _, err = tx.ExecContext(ctx, qIns, txid); err != nil {
		return nil, fmt.Errorf("insert seen_counters: %w", err)
	}

	// Idempotent append of subtreeID.
	qSub := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO seen_counter_subtrees (txid, subtree_id) VALUES (%s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.onConflictDoNothing,
	)
	if _, err = tx.ExecContext(ctx, qSub, txid, subtreeID); err != nil {
		return nil, fmt.Errorf("insert seen_counter_subtrees: %w", err)
	}

	// Count distinct subtrees for this txid.
	qCount := fmt.Sprintf("SELECT COUNT(*) FROM seen_counter_subtrees WHERE txid = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
	var count int
	if err = tx.QueryRowContext(ctx, qCount, txid).Scan(&count); err != nil {
		return nil, fmt.Errorf("count subtrees: %w", err)
	}

	// Atomically attempt the 0 -> 1 transition. The WHERE clause guarantees
	// only one writer succeeds: any concurrent attempt sees threshold_fired
	// already set to 1 and matches zero rows.
	thresholdReached := false
	if count >= s.threshold {
		thresholdReached, err = s.tryFireThreshold(ctx, tx, txid)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit seen counter tx: %w", err)
	}
	return &storepkg.IncrementResult{NewCount: count, ThresholdReached: thresholdReached}, nil
}

// tryFireThreshold flips threshold_fired from 0 to 1 atomically. Returns true
// if THIS caller performed the transition; false if a concurrent caller had
// already fired (the row's prior value was already 1). Errors are surfaced —
// the previous implementation swallowed marker-write failures, masking real
// faults like a row-level lock timeout.
func (s *seenCounter) tryFireThreshold(ctx context.Context, tx *sql.Tx, txid string) (bool, error) {
	if isPostgres(s.d) {
		// Postgres: UPDATE … RETURNING. RETURNING emits a row only when the
		// WHERE matches, so the presence of a result row IS the false->true
		// transition signal.
		q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
			`UPDATE seen_counters
            SET threshold_fired = 1
            WHERE txid = %s AND threshold_fired = 0
            RETURNING 1`, s.d.placeholder(1),
		)
		var one int
		err := tx.QueryRowContext(ctx, q, txid).Scan(&one)
		if err != nil {
			if err == sql.ErrNoRows {
				return false, nil
			}
			return false, fmt.Errorf("fire threshold (postgres): %w", err)
		}
		return true, nil
	}

	// SQLite path. Modern SQLite (>= 3.35) supports RETURNING, but we keep
	// portability with older builds by inspecting RowsAffected: the WHERE
	// filter on threshold_fired = 0 makes the UPDATE itself the CAS, and
	// RowsAffected > 0 means we won the race.
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`UPDATE seen_counters
        SET threshold_fired = 1
        WHERE txid = %s AND threshold_fired = 0`, s.d.placeholder(1),
	)
	res, err := tx.ExecContext(ctx, q, txid)
	if err != nil {
		return false, fmt.Errorf("fire threshold (sqlite): %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected after fire threshold: %w", err)
	}
	return n > 0, nil
}

// BatchDelete removes the counters (and their per-subtree child rows) for
// txids — the mine-time cleanup: once a txid is in a block its propagation
// counter is dead weight. Missing txids are silently skipped (idempotent, so
// work-item redelivery is safe). Per-txid statements match this backend's
// tests-and-small-deployments posture (see BatchIncrement).
func (s *seenCounter) BatchDelete(txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qChildren := fmt.Sprintf("DELETE FROM seen_counter_subtrees WHERE txid = %s", s.d.placeholder(1))
	qParent := fmt.Sprintf("DELETE FROM seen_counters WHERE txid = %s", s.d.placeholder(1))

	// Children + parent go in ONE transaction per txid (no FK cascade in the
	// schema): unpaired deletes could interleave with a concurrent Increment
	// and leave orphan child rows that a recreated parent later counts as
	// phantom subtrees. Per-txid transactions (not one big one) preserve the
	// best-effort contract — a failure on one txid doesn't roll back the rest.
	var firstErr error
	for _, txid := range txids {
		if err := s.deleteOne(ctx, qChildren, qParent, txid); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// deleteOne atomically removes one txid's child rows and parent counter row.
func (s *seenCounter) deleteOne(ctx context.Context, qChildren, qParent, txid string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete seen counter tx for %s: %w", txid, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, qChildren, txid); err != nil {
		return fmt.Errorf("delete seen counter subtrees for %s: %w", txid, err)
	}
	if _, err := tx.ExecContext(ctx, qParent, txid); err != nil {
		return fmt.Errorf("delete seen counter for %s: %w", txid, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete seen counter for %s: %w", txid, err)
	}
	return nil
}
