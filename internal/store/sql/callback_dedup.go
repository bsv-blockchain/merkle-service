package sql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

func (s *callbackDedup) Exists(txid, callbackURL, statusType string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := dedupKey(txid, callbackURL, statusType)
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"SELECT 1 FROM callback_dedup WHERE dedup_key = %s AND (expires_at IS NULL OR expires_at > %s)",
		s.d.placeholder(1), s.d.now)
	row := s.db.QueryRowContext(ctx, q, key)
	var x int
	err := row.Scan(&x)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *callbackDedup) Record(txid, callbackURL, statusType string, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := dedupKey(txid, callbackURL, statusType)
	var expiresExpr string
	if ttl > 0 {
		expiresExpr = s.d.intervalSeconds(int(ttl.Seconds()))
	} else {
		expiresExpr = "NULL"
	}
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		`INSERT INTO callback_dedup (dedup_key, expires_at) VALUES (%s, %s)
        ON CONFLICT (dedup_key) DO UPDATE SET expires_at = EXCLUDED.expires_at`,
		s.d.placeholder(1), expiresExpr)
	_, err := s.db.ExecContext(ctx, q, key)
	return err
}
