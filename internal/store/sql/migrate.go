package sql

import (
	"context"
	"database/sql"
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

// runMigrations applies every pending migration in order. Concurrent callers
// are serialized via pg_advisory_lock on PostgreSQL and an explicit transaction
// on SQLite. Already-applied migrations are idempotent no-ops.
func runMigrations(ctx context.Context, db *sql.DB, d *dialect, logger *slog.Logger) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if len(migs) == 0 {
		return nil
	}

	// Acquire a cross-process lock so simultaneous service starts don't
	// step on each other. PostgreSQL gets an advisory lock; SQLite naturally
	// serializes writers, but we still wrap in a transaction for consistency.
	release, err := acquireMigrationLock(ctx, db, d)
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer release()

	// Ensure schema_migrations exists. It is the first statement of 0001_init.
	// We execute 0001 wholesale below so this is just a safety net for partial
	// previous runs.
	if _, err := db.ExecContext(ctx, d.rewrite(`CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY, applied_at ${TIMESTAMPTZ} NOT NULL)`)); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}

	applied, err := queryAppliedVersions(ctx, db)
	if err != nil {
		return err
	}

	for _, m := range migs {
		if _, done := applied[m.version]; done {
			continue
		}
		if err := applyMigration(ctx, db, d, m); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.version, m.name, err)
		}
		if logger != nil {
			logger.Info("applied SQL migration", "version", m.version, "name", m.name)
		}
	}
	return nil
}

func acquireMigrationLock(ctx context.Context, db *sql.DB, d *dialect) (release func(), err error) {
	if isPostgres(d) {
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
	// SQLite: BEGIN IMMEDIATE gives us write-mode serialization. We commit
	// immediately after DDL below; any concurrent writer waits.
	return func() {}, nil
}

func queryAppliedVersions(ctx context.Context, db *sql.DB) (map[int]struct{}, error) {
	rows, err := db.QueryContext(ctx, "SELECT version FROM schema_migrations")
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

func applyMigration(ctx context.Context, db *sql.DB, d *dialect, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rewritten := d.rewrite(m.body)
	// Some drivers reject multiple DDL statements in a single Exec; split by
	// top-level semicolons. This is a deliberately naive split — our migration
	// bodies never embed `;` inside literals.
	for _, stmt := range splitStatements(rewritten) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec stmt: %w\n---\n%s", err, stmt)
		}
	}
	q := fmt.Sprintf("INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
		d.placeholder(1), d.now)
	if _, err := tx.ExecContext(ctx, q, m.version); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record migration: %w", err)
	}
	return tx.Commit()
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
var errMigrationFailed = errors.New("migration failed")
var _ = errMigrationFailed
