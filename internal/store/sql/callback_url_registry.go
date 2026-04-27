package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type callbackURLRegistry struct {
	db *sql.DB
	d  *dialect
}

var _ storepkg.CallbackURLRegistry = (*callbackURLRegistry)(nil)

func newCallbackURLRegistry(db *sql.DB, d *dialect) *callbackURLRegistry {
	return &callbackURLRegistry{db: db, d: d}
}

func (r *callbackURLRegistry) Add(callbackURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf("INSERT INTO callback_urls (callback_url) VALUES (%s)%s",
		r.d.placeholder(1), r.d.onConflictDoNothing)
	_, err := r.db.ExecContext(ctx, q, callbackURL)
	return err
}

func (r *callbackURLRegistry) GetAll() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := r.db.QueryContext(ctx, "SELECT callback_url FROM callback_urls ORDER BY callback_url")
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
