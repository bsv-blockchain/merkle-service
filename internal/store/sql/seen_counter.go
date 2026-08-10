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

// BatchAddPeer applies AddPeer to each txid (SQL is tests/small deploys).
func (s *seenCounter) BatchAddPeer(txids []string, peerID string, weight int) (map[string]*storepkg.IncrementResult, error) {
	results := make(map[string]*storepkg.IncrementResult, len(txids))
	var firstErr error
	for _, txid := range txids {
		res, err := s.AddPeer(txid, peerID, weight)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("batch add peer %s: %w", txid, err)
			}
			continue
		}
		results[txid] = res
	}
	return results, firstErr
}

// AddPeer records peerID once per txid with observation-time weight; fires
// threshold via conditional UPDATE (F-045).
func (s *seenCounter) AddPeer(txid, peerID string, weight int) (*storepkg.IncrementResult, error) {
	if weight <= 0 || peerID == "" {
		return &storepkg.IncrementResult{NewCount: 0, ThresholdReached: false}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	qIns := fmt.Sprintf( //nolint:gosec // placeholders internal
		"INSERT INTO seen_counters (txid) VALUES (%s)%s",
		s.d.placeholder(1), s.d.onConflictDoNothing,
	)
	if _, err = tx.ExecContext(ctx, qIns, txid); err != nil {
		return nil, fmt.Errorf("insert seen_counters: %w", err)
	}

	qPeer := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholders, no user input
		"INSERT INTO seen_counter_peers (txid, peer_id, weight) VALUES (%s, %s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3), s.d.onConflictDoNothing,
	)
	res, err := tx.ExecContext(ctx, qPeer, txid, peerID, weight)
	if err != nil {
		return nil, fmt.Errorf("insert seen_counter_peers: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		qBump := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholders, no user input
			"UPDATE seen_counters SET score = score + %s WHERE txid = %s",
			s.d.placeholder(1), s.d.placeholder(2),
		)
		if _, err = tx.ExecContext(ctx, qBump, weight, txid); err != nil {
			return nil, fmt.Errorf("bump score: %w", err)
		}
	}

	qScore := fmt.Sprintf("SELECT score FROM seen_counters WHERE txid = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from dialect
	var score int
	if err = tx.QueryRowContext(ctx, qScore, txid).Scan(&score); err != nil {
		return nil, fmt.Errorf("read score: %w", err)
	}

	thresholdReached := false
	if score >= s.threshold {
		thresholdReached, err = s.tryFireThreshold(ctx, tx, txid)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit seen counter tx: %w", err)
	}
	return &storepkg.IncrementResult{NewCount: score, ThresholdReached: thresholdReached}, nil
}

func (s *seenCounter) tryFireThreshold(ctx context.Context, tx *sql.Tx, txid string) (bool, error) {
	if isPostgres(s.d) {
		q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholders, no user input
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

	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholders, no user input
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

// BatchDelete removes peer-weighted counters at mine time.
func (s *seenCounter) BatchDelete(txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qChildren := fmt.Sprintf("DELETE FROM seen_counter_peers WHERE txid = %s", s.d.placeholder(1))
	// Also clean legacy subtree table if present (pre-migration rows).
	qLegacy := fmt.Sprintf("DELETE FROM seen_counter_subtrees WHERE txid = %s", s.d.placeholder(1))
	qParent := fmt.Sprintf("DELETE FROM seen_counters WHERE txid = %s", s.d.placeholder(1))

	var firstErr error
	for _, txid := range txids {
		if err := s.deleteOne(ctx, qChildren, qLegacy, qParent, txid); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *seenCounter) deleteOne(ctx context.Context, qChildren, qLegacy, qParent, txid string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete seen counter tx for %s: %w", txid, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, qChildren, txid); err != nil {
		return fmt.Errorf("delete seen counter peers for %s: %w", txid, err)
	}
	// Best-effort legacy cleanup (table may not exist on fresh installs after 0009).
	_, _ = tx.ExecContext(ctx, qLegacy, txid)
	if _, err := tx.ExecContext(ctx, qParent, txid); err != nil {
		return fmt.Errorf("delete seen counter for %s: %w", txid, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete seen counter for %s: %w", txid, err)
	}
	return nil
}
