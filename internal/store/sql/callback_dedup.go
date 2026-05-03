package sql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type callbackDedup struct {
	db *sql.DB
	d  *dialect
}

var _ storepkg.CallbackDedupStore = (*callbackDedup)(nil)

func newCallbackDedup(db *sql.DB, d *dialect) *callbackDedup {
	return &callbackDedup{db: db, d: d}
}

func dedupKey(txid, callbackURL, statusType string) string {
	h := sha256.Sum256([]byte(txid + ":" + callbackURL + ":" + statusType))
	return hex.EncodeToString(h[:])
}

// Claim atomically reserves the dedup slot for (txid, callbackURL, statusType)
// via INSERT ... ON CONFLICT DO NOTHING RETURNING. RETURNING populates exactly
// one row when the INSERT actually wrote (we won the claim) and zero rows
// when the unique-key conflict suppressed the write (a prior or concurrent
// writer already claimed). Both Postgres and SQLite 3.35+ support this
// combination, so a single statement is the dedup primitive on both backends.
//
// A live row whose expires_at is in the past is treated as already-expired:
// the conflicting INSERT first overwrites the stale row (so the new TTL takes
// effect) and the caller is told they claimed it. This matches the prior
// Exists() filter behavior so a long-since-delivered tuple can still be
// re-claimed once its TTL elapses without waiting for the sweeper.
//
// Returns (true, nil) when the caller should deliver, (false, nil) on a live
// duplicate, and (_, err) on backend failure (caller should treat as
// transient and retry).
func (s *callbackDedup) Claim(txid, callbackURL, statusType string, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := dedupKey(txid, callbackURL, statusType)

	var expiresExpr string
	if ttl > 0 {
		expiresExpr = s.d.intervalSeconds(int(ttl.Seconds()))
	} else {
		expiresExpr = "NULL"
	}

	// Step 1: best-effort eviction of an expired row for this key. Done in a
	// separate statement so the subsequent INSERT can use the simple ON
	// CONFLICT DO NOTHING form on both Postgres and SQLite without juggling
	// dialect differences in the conflict-update predicate.
	delQ := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"DELETE FROM callback_dedup WHERE dedup_key = %s AND expires_at IS NOT NULL AND expires_at <= %s",
		s.d.placeholder(1), s.d.now)
	if _, err := s.db.ExecContext(ctx, delQ, key); err != nil {
		return false, fmt.Errorf("dedup expire-clean: %w", err)
	}

	// Step 2: atomic claim. RETURNING fires exactly when the INSERT inserts.
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO callback_dedup (dedup_key, expires_at) VALUES (%s, %s)
        ON CONFLICT (dedup_key) DO NOTHING
        RETURNING 1`,
		s.d.placeholder(1), expiresExpr)

	var sentinel int
	err := s.db.QueryRowContext(ctx, q, key).Scan(&sentinel)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		// Conflict: a live row already exists. Caller is a duplicate.
		return false, nil
	}
	return false, fmt.Errorf("dedup claim: %w", err)
}
