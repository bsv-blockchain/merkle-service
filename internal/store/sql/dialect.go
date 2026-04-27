// Package sql implements the store.Registry backed by a SQL database. Supports
// PostgreSQL (production) and SQLite (tests, lightweight deployments).
package sql

import (
	"database/sql"
	"fmt"
	"strings"
)

// dialect captures the handful of per-driver differences we care about
// (placeholder style, type substitution, migration lock mechanism). Query
// strings are hand-written; this is intentionally small — not a mini-ORM.
type dialect struct {
	name string
	// placeholder returns the placeholder for the 1-based parameter position.
	// PostgreSQL uses $N, SQLite uses ? for every position.
	placeholder func(n int) string
	// now returns the driver's current-timestamp expression.
	now string
	// interval returns an expression for `now + N seconds`.
	intervalSeconds func(seconds int) string
	// onConflictDoNothing returns an INSERT suffix for idempotent insert.
	onConflictDoNothing string
	// rewrite substitutes dialect-specific types in DDL: ${BYTEA} → bytea/blob,
	// ${TIMESTAMPTZ} → timestamptz/datetime, ${AUTOINC} → bigserial/integer autoincrement,
	// ${IF_NOT_EXISTS_INDEX} → IF NOT EXISTS (pg & modern sqlite both support).
	rewrite func(stmt string) string
}

func postgresDialect() *dialect {
	return &dialect{
		name:                "postgres",
		placeholder:         func(n int) string { return fmt.Sprintf("$%d", n) },
		now:                 "now()",
		intervalSeconds:     func(s int) string { return fmt.Sprintf("now() + (%d * interval '1 second')", s) },
		onConflictDoNothing: " ON CONFLICT DO NOTHING",
		rewrite: func(stmt string) string {
			r := strings.NewReplacer(
				"${BYTEA}", "bytea",
				"${TIMESTAMPTZ}", "timestamptz",
				"${AUTOINC}", "bigserial",
				"${IF_NOT_EXISTS_INDEX}", "IF NOT EXISTS",
			)
			return r.Replace(stmt)
		},
	}
}

func sqliteDialect() *dialect {
	return &dialect{
		name:        "sqlite",
		placeholder: func(int) string { return "?" },
		now:         "CURRENT_TIMESTAMP",
		intervalSeconds: func(s int) string {
			sign := "+"
			if s < 0 {
				sign = "-"
				s = -s
			}
			return fmt.Sprintf("datetime(CURRENT_TIMESTAMP, '%s%d seconds')", sign, s)
		},
		onConflictDoNothing: " ON CONFLICT DO NOTHING",
		rewrite: func(stmt string) string {
			r := strings.NewReplacer(
				"${BYTEA}", "blob",
				"${TIMESTAMPTZ}", "datetime",
				"${AUTOINC}", "integer primary key autoincrement",
				"${IF_NOT_EXISTS_INDEX}", "IF NOT EXISTS",
			)
			return r.Replace(stmt)
		},
	}
}

// isPgx reports whether db is backed by a pgx-based driver (used to enable
// PostgreSQL-only features like pg_advisory_lock).
func isPostgres(d *dialect) bool { return d != nil && d.name == "postgres" }

// ensureRowsClosed is a tiny helper so callers can defer without stuttering.
func ensureRowsClosed(rows *sql.Rows) {
	if rows != nil {
		_ = rows.Close()
	}
}
