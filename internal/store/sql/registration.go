package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type registrationStore struct {
	db *sql.DB
	d  *dialect
}

var _ storepkg.RegistrationStore = (*registrationStore)(nil)

func newRegistrationStore(db *sql.DB, d *dialect) *registrationStore {
	return &registrationStore{db: db, d: d}
}

func (s *registrationStore) Add(txid, callbackURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertReg := fmt.Sprintf("INSERT INTO registrations (txid) VALUES (%s)%s",
		s.d.placeholder(1), s.d.onConflictDoNothing)
	if _, err := tx.ExecContext(ctx, insertReg, txid); err != nil {
		return fmt.Errorf("insert registration: %w", err)
	}
	insertURL := fmt.Sprintf("INSERT INTO registration_urls (txid, callback_url) VALUES (%s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.onConflictDoNothing)
	if _, err := tx.ExecContext(ctx, insertURL, txid, callbackURL); err != nil {
		return fmt.Errorf("insert registration url: %w", err)
	}
	return tx.Commit()
}

func (s *registrationStore) Get(txid string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf("SELECT callback_url FROM registration_urls WHERE txid = %s ORDER BY callback_url", s.d.placeholder(1))
	rows, err := s.db.QueryContext(ctx, q, txid)
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

func (s *registrationStore) BatchGet(txids []string) (map[string][]string, error) {
	if len(txids) == 0 {
		return map[string][]string{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	placeholders := make([]string, len(txids))
	args := make([]interface{}, len(txids))
	for i, t := range txids {
		placeholders[i] = s.d.placeholder(i + 1)
		args[i] = t
	}
	q := fmt.Sprintf("SELECT txid, callback_url FROM registration_urls WHERE txid IN (%s)",
		strings.Join(placeholders, ", "))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer ensureRowsClosed(rows)

	result := map[string][]string{}
	for rows.Next() {
		var txid, url string
		if err := rows.Scan(&txid, &url); err != nil {
			return nil, err
		}
		result[txid] = append(result[txid], url)
	}
	return result, rows.Err()
}

func (s *registrationStore) UpdateTTL(txid string, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf("UPDATE registrations SET expires_at = %s WHERE txid = %s",
		s.d.intervalSeconds(int(ttl.Seconds())), s.d.placeholder(1))
	_, err := s.db.ExecContext(ctx, q, txid)
	return err
}

func (s *registrationStore) BatchUpdateTTL(txids []string, ttl time.Duration) error {
	if len(txids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	placeholders := make([]string, len(txids))
	args := make([]interface{}, len(txids))
	for i, t := range txids {
		placeholders[i] = s.d.placeholder(i + 1)
		args[i] = t
	}
	q := fmt.Sprintf("UPDATE registrations SET expires_at = %s WHERE txid IN (%s)",
		s.d.intervalSeconds(int(ttl.Seconds())), strings.Join(placeholders, ", "))
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}
