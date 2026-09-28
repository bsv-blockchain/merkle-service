package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// seenCountersTTL returns migration 0009_seen_counters_ttl, which adds a
// column, so applying it twice fails with "duplicate column name". The
// fixtures below undo exactly this migration.
func seenCountersTTL(t *testing.T) migration {
	t.Helper()
	migs, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migs {
		if m.version == 9 && m.name == "seen_counters_ttl.sql" {
			return m
		}
	}
	t.Fatal("migration 0009_seen_counters_ttl.sql not found")
	return migration{}
}

// openWith0009Pending opens dsn, applies every migration, and then undoes
// 0009_seen_counters_ttl, so that migration is pending again.
func openWith0009Pending(t *testing.T, dsn string) (*sql.DB, migration) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := runMigrations(context.Background(), db, sqliteDialect(), nil); err != nil {
		t.Fatal(err)
	}
	last := seenCountersTTL(t)
	// Undo 0009_seen_counters_ttl.
	for _, s := range []struct {
		q    string
		args []any
	}{
		{q: "DROP INDEX idx_seen_counters_expires_at"},
		{q: "ALTER TABLE seen_counters DROP COLUMN expires_at"},
		{q: "DELETE FROM schema_migrations WHERE version = ?", args: []any{last.version}},
	} {
		if _, err := db.ExecContext(context.Background(), s.q, s.args...); err != nil {
			t.Fatalf("%s: %v", s.q, err)
		}
	}
	return db, last
}

// A migration that another start recorded after this start's first look at
// schema_migrations is skipped, not applied a second time (#43). This is the
// check inside the write lock.
func TestSQLiteMigration_RecheckInsideLock(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/recheck.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err = runMigrations(context.Background(), db, sqliteDialect(), nil); err != nil {
		t.Fatal(err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// As if this start's earlier read had missed 0009.
	applied, done, err := applySQLiteMigration(context.Background(), conn, sqliteDialect(), seenCountersTTL(t))
	if err != nil {
		t.Fatalf("already-recorded migration was run again: %v", err)
	}
	if applied || !done {
		t.Fatalf("applied=%v done=%v, want false, true", applied, done)
	}
}

// The re-check must happen after the write lock is taken. While another start
// holds BEGIN IMMEDIATE and has applied 0009 without committing,
// this start has to wait, then see the committed row and skip the migration (#43).
func TestSQLiteMigration_RecheckAfterWaitingForLock(t *testing.T) {
	for name, suffix := range map[string]string{
		"rollback journal": "",
		"WAL":              "?_pragma=journal_mode(WAL)",
	} {
		t.Run(name, func(t *testing.T) {
			dsn := "file:" + t.TempDir() + "/wait.db" + suffix
			holderDB, last := openWith0009Pending(t, dsn)
			holder, err := holderDB.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = holder.Close() }()
			if _, err = holder.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
				t.Fatal(err)
			}
			if err = applyStatements(context.Background(), holder, sqliteDialect(), last); err != nil {
				t.Fatal(err)
			}

			db, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if _, err := conn.ExecContext(context.Background(), "PRAGMA busy_timeout = 10000"); err != nil {
				t.Fatal(err)
			}

			type outcome struct {
				applied, done bool
				err           error
			}
			res := make(chan outcome, 1)
			go func() {
				applied, done, err := applySQLiteMigration(context.Background(), conn, sqliteDialect(), last)
				res <- outcome{applied, done, err}
			}()

			select {
			case r := <-res:
				t.Fatalf("returned while another start held the write lock: applied=%v done=%v err=%v", r.applied, r.done, r.err)
			case <-time.After(300 * time.Millisecond):
			}
			if _, err := holder.ExecContext(context.Background(), "COMMIT"); err != nil {
				t.Fatalf("holder commit: %v", err)
			}

			select {
			case r := <-res:
				if r.err != nil {
					t.Fatalf("after the other start committed: %v", r.err)
				}
				if r.applied || !r.done {
					t.Fatalf("applied=%v done=%v, want false, true", r.applied, r.done)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("still waiting after the lock was released")
			}
		})
	}
}
