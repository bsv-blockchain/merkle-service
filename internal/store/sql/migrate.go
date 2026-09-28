package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version int
	name    string
	body    string
}

// loadMigrations reads every migration from the embedded FS and returns them
// sorted by version. Files must be named NNNN_name.sql where NNNN parses as int.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	out := make([]migration, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(e.Name(), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("migration %q: expected NNNN_name.sql", e.Name())
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("migration %q: version not an integer: %w", e.Name(), err)
		}
		body, err := fs.ReadFile(migrationFS, "migrations/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{version: v, name: parts[1], body: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// sqliteMigrationBusyTimeoutMs is how long a SQLite start waits for another
// start's migration to finish before giving up. SQLite's busy wait does not
// observe ctx, so a cancelled start can wait up to this long. A variable so
// tests can shorten it.
var sqliteMigrationBusyTimeoutMs = 30000

// execQuerier is the part of *sql.DB, *sql.Conn and *sql.Tx the migration
// steps need, so SQLite can run them all on one locked connection.
type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// runMigrations applies every pending migration in order. Concurrent callers
// are serialized via pg_advisory_lock on PostgreSQL and a BEGIN IMMEDIATE
// transaction per migration on SQLite. Already-applied migrations are idempotent no-ops.
func runMigrations(ctx context.Context, db *sql.DB, d *dialect, logger *slog.Logger) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if len(migs) == 0 {
		return nil
	}

	if !isPostgres(d) {
		return migrateSQLite(ctx, db, d, migs, logger)
	}

	// Acquire a cross-process lock so simultaneous service starts don't
	// step on each other.
	release, err := acquirePostgresMigrationLock(ctx, db)
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer release()

	return applyPending(ctx, db, d, migs, logger, func(m migration) (bool, error) {
		return true, applyMigration(ctx, db, d, m)
	})
}

// migrateSQLite applies each pending migration in its own BEGIN IMMEDIATE
// transaction on one connection, re-checking schema_migrations inside it.
// IMMEDIATE takes the write lock before that check, so a concurrent start
// waits (busy_timeout) and then finds the migration already recorded, instead
// of both applying it (#43). Each migration still commits on its own, as before.
func migrateSQLite(ctx context.Context, db *sql.DB, d *dialect, migs []migration, logger *slog.Logger) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	// dirty is set when the connection may still hold a transaction or the
	// raised busy_timeout; it is then discarded instead of returned to the pool.
	dirty := false
	defer func() {
		if dirty {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}()

	// Wait for another start's migration rather than failing with SQLITE_BUSY,
	// then put the connection's own setting back.
	var prevTimeout int
	if err = conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&prevTimeout); err != nil {
		return fmt.Errorf("migration lock: read busy_timeout: %w", err)
	}
	if prevTimeout < sqliteMigrationBusyTimeoutMs {
		if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", sqliteMigrationBusyTimeoutMs)); err != nil {
			// A cancelled ctx can report an error after the pragma took effect.
			dirty = true
			return fmt.Errorf("migration lock: set busy_timeout: %w", err)
		}
		defer func() {
			if _, rerr := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA busy_timeout = %d", prevTimeout)); rerr != nil {
				dirty = true
			}
		}()
	}

	return applyPending(ctx, conn, d, migs, logger, func(m migration) (bool, error) {
		applied, done, err := applySQLiteMigration(ctx, conn, d, m)
		if !done {
			dirty = true
		}
		return applied, err
	})
}

// applySQLiteMigration applies m inside BEGIN IMMEDIATE unless another start
// recorded it first; applied reports which. done is false when the
// transaction may still be open.
func applySQLiteMigration(ctx context.Context, conn *sql.Conn, d *dialect, m migration) (applied, done bool, err error) {
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		// A cancelled ctx can report an error after BEGIN took effect, so the
		// transaction may be open.
		return false, false, fmt.Errorf("migration lock: %w", err)
	}
	finish := func(stmt string) bool {
		_, ferr := conn.ExecContext(context.Background(), stmt)
		return ferr == nil
	}

	recorded, err := queryAppliedVersions(ctx, conn)
	if err == nil {
		if _, ok := recorded[m.version]; ok {
			if _, err = conn.ExecContext(context.Background(), "COMMIT"); err != nil {
				return false, finish("ROLLBACK"), fmt.Errorf("commit after finding migration applied: %w", err)
			}
			return false, true, nil
		}
		err = applyStatements(ctx, conn, d, m)
	}
	if err != nil {
		return false, finish("ROLLBACK"), err
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return false, finish("ROLLBACK"), fmt.Errorf("commit migration: %w", err)
	}
	return true, true, nil
}

// applyPending bootstraps schema_migrations and applies each migration not yet
// recorded there, in order, using apply. apply reports false when it found
// the migration already applied by a concurrent start.
func applyPending(ctx context.Context, ex execQuerier, d *dialect, migs []migration, logger *slog.Logger,
	apply func(migration) (bool, error),
) error {
	// Ensure schema_migrations exists. It is the first statement of 0001_init.
	// We execute 0001 wholesale below so this is just a safety net for partial
	// previous runs.
	if _, err := ex.ExecContext(ctx, d.rewrite(`CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY, applied_at ${TIMESTAMPTZ} NOT NULL)`)); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}

	applied, err := queryAppliedVersions(ctx, ex)
	if err != nil {
		return err
	}

	for _, m := range migs {
		if _, done := applied[m.version]; done {
			continue
		}
		appliedNow, err := apply(m)
		if err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.version, m.name, err)
		}
		if appliedNow && logger != nil {
			logger.Info("applied SQL migration", "version", m.version, "name", m.name)
		}
	}
	return nil
}

func acquirePostgresMigrationLock(ctx context.Context, db *sql.DB) (release func(), err error) {
	// 0x6D726B6C657376 ("mrklesv") fits in a 64-bit advisory key.
	const key int64 = 0x6D726B6C657376
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return func() {
		_, _ = conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", key)
		_ = conn.Close()
	}, nil
}

func queryAppliedVersions(ctx context.Context, ex execQuerier) (map[int]struct{}, error) {
	rows, err := ex.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		// Table might not exist on the very first run; treat as empty.
		if isMissingTable(err) {
			return map[int]struct{}{}, nil
		}
		return nil, fmt.Errorf("select schema_migrations: %w", err)
	}
	defer ensureRowsClosed(rows)
	out := map[int]struct{}{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = struct{}{}
	}
	return out, rows.Err()
}

// applyMigration applies one migration in its own transaction.
func applyMigration(ctx context.Context, db *sql.DB, d *dialect, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := applyStatements(ctx, tx, d, m); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// applyStatements runs a migration's statements and records its version,
// inside whatever transaction ex belongs to.
func applyStatements(ctx context.Context, ex execQuerier, d *dialect, m migration) error {
	rewritten := d.rewrite(m.body)
	// Some drivers reject multiple DDL statements in a single Exec; split by
	// top-level semicolons. This is a deliberately naive split — our migration
	// bodies never embed `;` inside literals.
	for _, stmt := range splitStatements(rewritten) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := ex.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec stmt: %w\n---\n%s", err, stmt)
		}
	}
	// SQL built from internal placeholder functions, no user input.
	q := fmt.Sprintf(
		"INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
		d.placeholder(1), d.now,
	)
	if _, err := ex.ExecContext(ctx, q, m.version); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	return nil
}

func splitStatements(body string) []string {
	// Strip line comments so a trailing `-- foo;` doesn't confuse splitter.
	var clean strings.Builder
	for _, line := range strings.Split(body, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "--") {
			continue
		}
		clean.WriteString(line)
		clean.WriteByte('\n')
	}
	return strings.Split(clean.String(), ";")
}

func isMissingTable(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "no such table") ||
		strings.Contains(s, "does not exist") ||
		strings.Contains(s, "undefined_table")
}

// Sentinel for callers that want to distinguish migration errors — currently
// unused but kept so future work can wrap richer error types.
var (
	errMigrationFailed = errors.New("migration failed")
	_                  = errMigrationFailed
)
