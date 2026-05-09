package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

// dataHubRegistry stores the set of DataHub URLs the block processor has
// successfully fetched from. /reprocess uses GetAll to build a candidate
// list when no caller-supplied DataHub is available. Same recency model as
// callback_urls — rows whose last_seen_at falls outside the retention window
// are evicted by the sweeper.
type dataHubRegistry struct {
	db        *sql.DB
	d         *dialect
	retention time.Duration
}

var _ storepkg.DataHubRegistry = (*dataHubRegistry)(nil)

func newDataHubRegistry(db *sql.DB, d *dialect, retention time.Duration) *dataHubRegistry {
	if retention <= 0 {
		retention = defaultCallbackURLRetention
	}
	return &dataHubRegistry{db: db, d: d, retention: retention}
}

func (r *dataHubRegistry) Add(dataHubURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO datahub_urls (datahub_url, last_seen_at) VALUES (%s, %s) "+
			"ON CONFLICT (datahub_url) DO UPDATE SET last_seen_at = %s",
		r.d.placeholder(1), r.d.now, r.d.now)
	_, err := r.db.ExecContext(ctx, q, dataHubURL)
	return err
}

func (r *dataHubRegistry) GetAll() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cutoff := -int(r.retention / time.Second)
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"SELECT datahub_url FROM datahub_urls "+
			"WHERE last_seen_at IS NULL OR last_seen_at >= %s "+
			"ORDER BY datahub_url",
		r.d.intervalSeconds(cutoff))

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer ensureRowsClosed(rows)
	var out []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, err
		}
		out = append(out, url)
	}
	return out, rows.Err()
}
