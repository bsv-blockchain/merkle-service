package sql

import (
	"database/sql"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestSQLiteRuntimeDSN(t *testing.T) {
	for _, tc := range []struct {
		name, dsn, lock string
		pragmas         []string
	}{
		{"bare", "file:db.sqlite", "immediate", []string{"busy_timeout(5000)"}},
		{"memory", ":memory:", "immediate", []string{"busy_timeout(5000)"}},
		{"wal", "file:db.sqlite?_pragma=journal_mode(WAL)", "immediate", []string{"journal_mode(WAL)", "busy_timeout(5000)"}},
		{"explicit", "file:db.sqlite?_pragma=busy_timeout(1200)&_txlock=deferred", "deferred", []string{"busy_timeout(1200)"}},
		{"zero", "file:db.sqlite?_pragma=BUSY_TIMEOUT%3D0&_txlock=exclusive", "exclusive", []string{"BUSY_TIMEOUT=0"}},
		{"schema", "file:db.sqlite?_pragma=main.busy_timeout(3000)", "immediate", []string{"main.busy_timeout(3000)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sqliteRuntimeDSN(tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			base, rawQuery, _ := strings.Cut(got, "?")
			originalBase, _, _ := strings.Cut(tc.dsn, "?")
			query, err := url.ParseQuery(rawQuery)
			if err != nil {
				t.Fatal(err)
			}
			if base != originalBase || query.Get("_txlock") != tc.lock || !reflect.DeepEqual(query["_pragma"], tc.pragmas) {
				t.Fatalf("unexpected DSN: %s", got)
			}
			again, err := sqliteRuntimeDSN(got)
			if err != nil || again != got {
				t.Fatalf("normalization not idempotent: %q, %v", again, err)
			}
		})
	}
	if _, err := sqliteRuntimeDSN("file:db.sqlite?secret=%invalid"); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("invalid query must fail without disclosing its contents")
	}
}

func TestSQLiteRuntimeDefaultsOnEveryConnection(t *testing.T) {
	db, _ := newRuntimeSQLite(t)
	var conns []*sql.Conn
	t.Cleanup(func() {
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	for i := 0; i < 3; i++ {
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		var timeout int
		if err := conn.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil {
			t.Fatal(err)
		}
		if timeout != 5000 {
			t.Fatalf("connection %d timeout=%d, want 5000", i, timeout)
		}
	}
}
