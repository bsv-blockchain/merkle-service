package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

// defaultCallbackURLRetention is the eviction window applied by the SQL
// callback URL registry when no explicit retention is configured. Mirrors the
// Aerospike sibling's 7-day default. See F-037 / issue #23.
const defaultCallbackURLRetention = 7 * 24 * time.Hour

// callbackURLRegistry stores the set of known callback URLs with a recency
// timestamp. `Add` upserts (url, now); `GetAll` filters URLs whose last_seen_at
// is within the retention window; the sweeper evicts older rows. Together
// these bound the registry to roughly the active-URL count and stop
// BLOCK_PROCESSED fan-out from broadcasting to long-since-expired tenants.
type callbackURLRegistry struct {
	db        *sql.DB
	d         *dialect
	retention time.Duration
}

var _ storepkg.CallbackURLRegistry = (*callbackURLRegistry)(nil)

func newCallbackURLRegistry(db *sql.DB, d *dialect, retention time.Duration) *callbackURLRegistry {
	if retention <= 0 {
		retention = defaultCallbackURLRetention
	}
	return &callbackURLRegistry{db: db, d: d, retention: retention}
}

func (r *callbackURLRegistry) Add(callbackURL, callbackToken string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// On conflict we must refresh last_seen_at — otherwise a URL added once
	// would expire even though it is being actively re-registered. We use a
	// dialect-portable UPSERT shape (ON CONFLICT ... DO UPDATE) which both
	// PostgreSQL and SQLite (>= 3.24) support. The token is also refreshed
	// so a rotation in arcade's cfg.CallbackToken converges within one
	// /watch round-trip.
	// A fresh /watch also clears the circuit breaker: a tenant that comes back
	// online re-enables its URL (failure_count -> 0, disabled_at -> NULL) within
	// one round-trip.
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO callback_urls (callback_url, last_seen_at, callback_token) VALUES (%s, %s, %s) "+
			"ON CONFLICT (callback_url) DO UPDATE SET last_seen_at = %s, callback_token = EXCLUDED.callback_token, "+
			"failure_count = 0, disabled_at = NULL",
		r.d.placeholder(1), r.d.now, r.d.placeholder(2), r.d.now,
	)
	_, err := r.db.ExecContext(ctx, q, callbackURL, callbackToken)
	return err
}

// RecordFailure increments the URL's failure counter and disables it once the
// counter reaches threshold. Returns whether the URL is now disabled. A
// non-positive threshold or an unknown URL is a no-op.
func (r *callbackURLRegistry) RecordFailure(callbackURL string, threshold int) (bool, error) {
	if threshold <= 0 {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Increment, and set disabled_at on the transition to >= threshold (leave it
	// set once tripped). disabled_at uses the driver's now expression.
	upd := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"UPDATE callback_urls SET failure_count = failure_count + 1, "+
			"disabled_at = CASE WHEN failure_count + 1 >= %s AND disabled_at IS NULL THEN %s ELSE disabled_at END "+
			"WHERE callback_url = %s",
		r.d.placeholder(1), r.d.now, r.d.placeholder(2),
	)
	if _, err := r.db.ExecContext(ctx, upd, threshold, callbackURL); err != nil {
		return false, err
	}

	// Report disabled state from the durable failure_count so the result is
	// portable across dialects (avoids scanning a boolean / nullable timestamp).
	sel := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"SELECT failure_count FROM callback_urls WHERE callback_url = %s",
		r.d.placeholder(1),
	)
	var failureCount int
	switch err := r.db.QueryRowContext(ctx, sel, callbackURL).Scan(&failureCount); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	return failureCount >= threshold, nil
}

func (r *callbackURLRegistry) GetAll() ([]storepkg.CallbackEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// last_seen_at IS NULL covers rows inserted before migration 0002 — they
	// are treated as fresh until the next Add() (which stamps last_seen_at)
	// or the next sweeper tick (which uses the same NULL-tolerant predicate).
	cutoff := -int(r.retention / time.Second)
	// disabled_at IS NULL excludes URLs whose circuit breaker has tripped, so
	// BLOCK_PROCESSED / STUMP fan-out stops targeting a dead endpoint.
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"SELECT callback_url, callback_token FROM callback_urls "+
			"WHERE (last_seen_at IS NULL OR last_seen_at >= %s) AND disabled_at IS NULL "+
			"ORDER BY callback_url",
		r.d.intervalSeconds(cutoff),
	)

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer ensureRowsClosed(rows)
	var out []storepkg.CallbackEntry
	for rows.Next() {
		var entry storepkg.CallbackEntry
		if err := rows.Scan(&entry.URL, &entry.Token); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}
