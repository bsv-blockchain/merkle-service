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
	// maxCallbacksPerTxID caps the number of distinct callback URLs allowed
	// per txid. 0 disables the cap (legacy unbounded behavior — F-050).
	maxCallbacksPerTxID int
}

var _ storepkg.RegistrationStore = (*registrationStore)(nil)

func newRegistrationStore(db *sql.DB, d *dialect, maxCallbacksPerTxID int) *registrationStore {
	if maxCallbacksPerTxID < 0 {
		maxCallbacksPerTxID = 0
	}
	return &registrationStore{db: db, d: d, maxCallbacksPerTxID: maxCallbacksPerTxID}
}

// Add registers callbackURL for txid. Re-adding an already-known URL is a
// no-op (set semantics, idempotent). When maxCallbacksPerTxID > 0, exceeding
// the cap returns store.ErrMaxCallbacksPerTxIDExceeded — the API layer maps
// this to HTTP 429.
//
// Concurrency: on Postgres we acquire a SELECT ... FOR UPDATE on the parent
// `registrations` row before counting and inserting, so two concurrent Add
// calls for the same txid can't both observe count == max-1 and both succeed.
// On SQLite the database serializes writers globally (BEGIN IMMEDIATE in the
// driver), so the count-then-insert pair is already atomic without an
// explicit lock.
func (s *registrationStore) Add(txid, callbackURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	insertReg := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO registrations (txid) VALUES (%s)%s",
		s.d.placeholder(1), s.d.onConflictDoNothing)
	if _, err := tx.ExecContext(ctx, insertReg, txid); err != nil {
		return fmt.Errorf("insert registration: %w", err)
	}

	if s.maxCallbacksPerTxID > 0 {
		// Lock the parent row (Postgres) so the count + insert below race
		// safely against concurrent registrations for the same txid.
		if isPostgres(s.d) {
			lockQ := fmt.Sprintf("SELECT 1 FROM registrations WHERE txid = %s FOR UPDATE", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
			var dummy int
			if err := tx.QueryRowContext(ctx, lockQ, txid).Scan(&dummy); err != nil {
				return fmt.Errorf("lock registration: %w", err)
			}
		}

		// Idempotency probe: if the URL is already registered, re-adding is
		// a no-op regardless of the cap. Otherwise enforce the limit.
		probeQ := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
			"SELECT 1 FROM registration_urls WHERE txid = %s AND callback_url = %s",
			s.d.placeholder(1), s.d.placeholder(2))
		var exists int
		switch err := tx.QueryRowContext(ctx, probeQ, txid, callbackURL).Scan(&exists); err {
		case nil:
			// URL already present — commit so the registrations row sticks
			// (preserving prior behavior where the parent row is upserted).
			return tx.Commit()
		case sql.ErrNoRows:
			// fall through to count + insert
		default:
			return fmt.Errorf("probe registration url: %w", err)
		}

		countQ := fmt.Sprintf("SELECT COUNT(*) FROM registration_urls WHERE txid = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
		var count int
		if err := tx.QueryRowContext(ctx, countQ, txid).Scan(&count); err != nil {
			return fmt.Errorf("count registration urls: %w", err)
		}
		if count >= s.maxCallbacksPerTxID {
			return storepkg.ErrMaxCallbacksPerTxIDExceeded
		}
	}

	insertURL := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO registration_urls (txid, callback_url) VALUES (%s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.onConflictDoNothing)
	if _, err := tx.ExecContext(ctx, insertURL, txid, callbackURL); err != nil {
		return fmt.Errorf("insert registration url: %w", err)
	}
	return tx.Commit()
}

func (s *registrationStore) Get(txid string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf("SELECT callback_url FROM registration_urls WHERE txid = %s ORDER BY callback_url", s.d.placeholder(1)) //nolint:gosec // placeholder from internal function
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
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"SELECT txid, callback_url FROM registration_urls WHERE txid IN (%s)",
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
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"UPDATE registrations SET expires_at = %s WHERE txid = %s",
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
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"UPDATE registrations SET expires_at = %s WHERE txid IN (%s)",
		s.d.intervalSeconds(int(ttl.Seconds())), strings.Join(placeholders, ", "))
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}
