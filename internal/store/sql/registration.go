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

// Add registers callbackURL + callbackToken for txid. Re-adding an
// already-known URL refreshes the token (set semantics on URL, idempotent
// on the row). When maxCallbacksPerTxID > 0, exceeding the cap returns
// store.ErrMaxCallbacksPerTxIDExceeded — the API layer maps this to HTTP 429.
//
// Concurrency: on Postgres we acquire a SELECT ... FOR UPDATE on the parent
// `registrations` row before counting and inserting, so two concurrent Add
// calls for the same txid can't both observe count == max-1 and both succeed.
// On SQLite the database serializes writers globally (BEGIN IMMEDIATE in the
// driver), so the count-then-insert pair is already atomic without an
// explicit lock.
func (s *registrationStore) Add(txid, callbackURL, callbackToken string) error {
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
		// a no-op for the count check (regardless of the cap). The token is
		// still refreshed via the ON CONFLICT DO UPDATE in the INSERT below.
		probeQ := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
			"SELECT 1 FROM registration_urls WHERE txid = %s AND callback_url = %s",
			s.d.placeholder(1), s.d.placeholder(2))
		var exists int
		switch err := tx.QueryRowContext(ctx, probeQ, txid, callbackURL).Scan(&exists); err {
		case nil:
			// URL already present — refresh its token and commit. Without
			// this UPDATE a token rotation would never propagate.
			updateTokenQ := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
				"UPDATE registration_urls SET callback_token = %s WHERE txid = %s AND callback_url = %s",
				s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3))
			if _, updateErr := tx.ExecContext(ctx, updateTokenQ, callbackToken, txid, callbackURL); updateErr != nil {
				return fmt.Errorf("refresh callback token: %w", updateErr)
			}
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

	// Cap-disabled / new-row path: upsert the (txid, callback_url) row,
	// refreshing callback_token on conflict so a token rotation lands.
	insertURL := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO registration_urls (txid, callback_url, callback_token) VALUES (%s, %s, %s) "+
			"ON CONFLICT (txid, callback_url) DO UPDATE SET callback_token = EXCLUDED.callback_token",
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3))
	if _, err := tx.ExecContext(ctx, insertURL, txid, callbackURL, callbackToken); err != nil {
		return fmt.Errorf("insert registration url: %w", err)
	}
	return tx.Commit()
}

func (s *registrationStore) Get(txid string) ([]storepkg.CallbackEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"SELECT callback_url, callback_token FROM registration_urls WHERE txid = %s ORDER BY callback_url",
		s.d.placeholder(1))
	rows, err := s.db.QueryContext(ctx, q, txid)
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

// batchParamChunkSize bounds how many txids we bind per query. The Postgres
// extended wire protocol caps bind parameters at 65535 per statement, and a
// single block subtree can carry 2^17+ txids (issue surfaced when a 131k-leaf
// subtree was DLQ'd with "extended protocol limited to 65535 parameters").
// 10000 leaves comfortable headroom and still amortizes round-trip cost.
const batchParamChunkSize = 10000

func (s *registrationStore) BatchGet(txids []string) (map[string][]storepkg.CallbackEntry, error) {
	if len(txids) == 0 {
		return map[string][]storepkg.CallbackEntry{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := map[string][]storepkg.CallbackEntry{}
	for start := 0; start < len(txids); start += batchParamChunkSize {
		end := start + batchParamChunkSize
		if end > len(txids) {
			end = len(txids)
		}
		chunk := txids[start:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for i, t := range chunk {
			placeholders[i] = s.d.placeholder(i + 1)
			args[i] = t
		}
		q := fmt.Sprintf(
			"SELECT txid, callback_url, callback_token FROM registration_urls WHERE txid IN (%s)",
			strings.Join(placeholders, ", "))
		if err := s.batchGetChunk(ctx, q, args, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *registrationStore) batchGetChunk(ctx context.Context, q string, args []interface{}, result map[string][]storepkg.CallbackEntry) error {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer ensureRowsClosed(rows)
	for rows.Next() {
		var txid string
		var entry storepkg.CallbackEntry
		if err := rows.Scan(&txid, &entry.URL, &entry.Token); err != nil {
			return err
		}
		result[txid] = append(result[txid], entry)
	}
	return rows.Err()
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

	intervalExpr := s.d.intervalSeconds(int(ttl.Seconds()))
	for start := 0; start < len(txids); start += batchParamChunkSize {
		end := start + batchParamChunkSize
		if end > len(txids) {
			end = len(txids)
		}
		chunk := txids[start:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for i, t := range chunk {
			placeholders[i] = s.d.placeholder(i + 1)
			args[i] = t
		}
		q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
			"UPDATE registrations SET expires_at = %s WHERE txid IN (%s)",
			intervalExpr, strings.Join(placeholders, ", "))
		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}
	return nil
}
