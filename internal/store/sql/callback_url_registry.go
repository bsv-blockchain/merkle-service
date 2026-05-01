package sql

import (
	"context"
	"database/sql"
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

func (r *callbackURLRegistry) Add(callbackURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// On conflict we must refresh last_seen_at — otherwise a URL added once
	// would expire even though it is being actively re-registered. We use a
	// dialect-portable UPSERT shape (ON CONFLICT ... DO UPDATE) which both
	// PostgreSQL and SQLite (>= 3.24) support.
	q := fmt.Sprintf(
		"INSERT INTO callback_urls (callback_url, last_seen_at) VALUES (%s, %s) "+
			"ON CONFLICT (callback_url) DO UPDATE SET last_seen_at = %s",
		r.d.placeholder(1), r.d.now, r.d.now)
	_, err := r.db.ExecContext(ctx, q, callbackURL)
	return err
}

func (r *callbackURLRegistry) GetAll() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// last_seen_at IS NULL covers rows inserted before migration 0002 — they
	// are treated as fresh until the next Add() (which stamps last_seen_at)
	// or the next sweeper tick (which uses the same NULL-tolerant predicate).
	cutoff := -int(r.retention / time.Second)
	q := fmt.Sprintf(
		"SELECT callback_url FROM callback_urls "+
			"WHERE last_seen_at IS NULL OR last_seen_at >= %s "+
			"ORDER BY callback_url",
		r.d.intervalSeconds(cutoff))

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer ensureRowsClosed(rows)
	var out []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
