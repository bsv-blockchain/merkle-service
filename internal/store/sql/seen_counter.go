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

// Increment inserts (txid, subtreeID) into seen_counter_subtrees (idempotent
// via the compound PK), counts distinct subtrees for the txid, and flips the
// threshold_fired flag atomically on the first call that reaches the threshold.
// The caller observes ThresholdReached=true on exactly one call.
func (s *seenCounter) Increment(txid, subtreeID string) (*storepkg.IncrementResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Ensure the parent counter row exists (no-op if already there).
	qIns := fmt.Sprintf("INSERT INTO seen_counters (txid) VALUES (%s)%s",
		s.d.placeholder(1), s.d.onConflictDoNothing)
	if _, err := tx.ExecContext(ctx, qIns, txid); err != nil {
		return nil, fmt.Errorf("insert seen_counters: %w", err)
	}

	// Idempotent append of subtreeID.
	qSub := fmt.Sprintf("INSERT INTO seen_counter_subtrees (txid, subtree_id) VALUES (%s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.onConflictDoNothing)
	if _, err := tx.ExecContext(ctx, qSub, txid, subtreeID); err != nil {
		return nil, fmt.Errorf("insert seen_counter_subtrees: %w", err)
	}

	// Count distinct subtrees for this txid.
	qCount := fmt.Sprintf("SELECT COUNT(*) FROM seen_counter_subtrees WHERE txid = %s", s.d.placeholder(1))
	var count int
	if err := tx.QueryRowContext(ctx, qCount, txid).Scan(&count); err != nil {
		return nil, fmt.Errorf("count subtrees: %w", err)
	}

	// Lock the counter row and check/flip the fired flag atomically.
	var lockQuery string
	if isPostgres(s.d) {
		lockQuery = fmt.Sprintf("SELECT threshold_fired FROM seen_counters WHERE txid = %s FOR UPDATE", s.d.placeholder(1))
	} else {
		lockQuery = fmt.Sprintf("SELECT threshold_fired FROM seen_counters WHERE txid = %s", s.d.placeholder(1))
	}
	var fired int
	if err := tx.QueryRowContext(ctx, lockQuery, txid).Scan(&fired); err != nil {
		return nil, fmt.Errorf("read fired flag: %w", err)
	}

	thresholdReached := false
	if fired == 0 && count >= s.threshold {
		qFire := fmt.Sprintf("UPDATE seen_counters SET threshold_fired = 1 WHERE txid = %s", s.d.placeholder(1))
		if _, err := tx.ExecContext(ctx, qFire, txid); err != nil {
			return nil, fmt.Errorf("set fired: %w", err)
		}
		thresholdReached = true
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &storepkg.IncrementResult{NewCount: count, ThresholdReached: thresholdReached}, nil
}
