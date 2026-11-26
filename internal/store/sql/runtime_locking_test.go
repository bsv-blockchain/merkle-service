package sql

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// Use the public constructor with a bare DSN and multiple connections. The
// single-connection newTestDB helper hides contention between runtime stores.
func newRuntimeSQLite(t *testing.T) (*sql.DB, *registrationStore) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Store.SQL.Driver = "sqlite"
	cfg.Store.SQL.DSN = "file:" + t.TempDir() + "/runtime.db"
	cfg.Store.SQL.MaxOpenConns = 8
	cfg.Store.SQL.MaxIdleConns = 8
	cfg.Store.SQL.SweeperInterval = "1h"
	cfg.BlobStore.URL = "memory:"
	r, err := New(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	reg := r.Registration.(*registrationStore)
	return reg.db, reg
}

func TestSQLiteRuntimeWaitsForWriter(t *testing.T) {
	db, reg := newRuntimeSQLite(t)
	holder, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := holder.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if _, err = holder.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = holder.ExecContext(t.Context(), "ROLLBACK") }()

	done := make(chan error, 1)
	go func() { done <- reg.Add("waiting-registration", "http://callback.invalid", "") }()
	select {
	case earlyErr := <-done:
		t.Fatalf("registration returned while another writer held the lock: %v", earlyErr)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err = holder.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	select {
	case registrationErr := <-done:
		if registrationErr != nil {
			t.Fatal(registrationErr)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("registration did not resume after the writer released its lock")
	}
}

func TestSQLiteRuntimeRegistrationAndCleanup(t *testing.T) {
	db, reg := newRuntimeSQLite(t)
	for i := 0; i < 100; i++ {
		if _, err := db.ExecContext(t.Context(), "INSERT INTO registrations (txid, expires_at) VALUES (?, '2000-01-01 00:00:00')", fmt.Sprintf("expired-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	sw := newSweeper(db, sqliteDialect(), time.Hour, time.Hour, nil)
	start := make(chan struct{})
	errs := make(chan error, 5)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 25; i++ {
				if err := reg.Add(fmt.Sprintf("fresh-%d-%d", w, i), "http://callback.invalid", ""); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			if _, err := sw.sweepTableBatch(t.Context(), ttlTables[0], 1); err != nil {
				errs <- err
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var fresh, expired int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM registrations WHERE txid LIKE 'fresh-%'").Scan(&fresh); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM registrations WHERE txid LIKE 'expired-%'").Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if fresh != 100 || expired != 0 {
		t.Fatalf("fresh=%d expired=%d; want 100 fresh and no expired rows", fresh, expired)
	}
}
