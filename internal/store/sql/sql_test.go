package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

// newTestDB opens a fresh in-memory SQLite and runs migrations. Each test
// gets its own database — ":memory:" is shared across connections in some
// drivers, so we use a unique temp file per test.
func newTestDB(t *testing.T) (*sql.DB, *dialect) {
	t.Helper()
	tmp := t.TempDir() + "/test.db"
	db, err := sql.Open("sqlite", "file:"+tmp+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// Single connection keeps BEGIN IMMEDIATE behavior predictable in tests.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	d := sqliteDialect()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := runMigrations(context.Background(), db, d, logger); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return db, d
}

func TestRegistrationStore_IdempotentAdd(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d)

	for i := 0; i < 3; i++ {
		if err := s.Add("tx1", "http://cb1"); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	got, err := s.Get("tx1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 1 || got[0] != "http://cb1" {
		t.Fatalf("got %v, want [http://cb1]", got)
	}
}

func TestRegistrationStore_BatchGet(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d)
	if err := s.Add("a", "u1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("a", "u2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("b", "u3"); err != nil {
		t.Fatal(err)
	}

	got, err := s.BatchGet([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got["a"]) != 2 {
		t.Fatalf("a urls = %v, want 2", got["a"])
	}
	if len(got["b"]) != 1 {
		t.Fatalf("b urls = %v, want 1", got["b"])
	}
	if _, ok := got["c"]; ok {
		t.Fatalf("c should not be present")
	}
}

func TestCallbackDedup_RecordAndExists(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackDedup(db, d)

	ok, err := s.Exists("tx", "url", "MINED")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Exists=true before Record")
	}

	if err := s.Record("tx", "url", "MINED", 1*time.Hour); err != nil {
		t.Fatal(err)
	}
	ok, err = s.Exists("tx", "url", "MINED")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Exists=false after Record")
	}

	// Idempotent re-record.
	if err := s.Record("tx", "url", "MINED", 1*time.Hour); err != nil {
		t.Fatalf("re-record: %v", err)
	}
}

func TestCallbackDedup_TTLExpiry(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackDedup(db, d)

	// 1-second TTL so the check-clause can observe the expiry without
	// needing the sweeper goroutine to run.
	if err := s.Record("tx", "url", "MINED", 1*time.Second); err != nil {
		t.Fatal(err)
	}
	// Wait past the TTL. SQLite's CURRENT_TIMESTAMP has second precision so
	// we pad by 2s.
	time.Sleep(2100 * time.Millisecond)
	ok, err := s.Exists("tx", "url", "MINED")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Exists should be false after TTL elapses")
	}
}

func TestCallbackURLRegistry_AddGetAll(t *testing.T) {
	db, d := newTestDB(t)
	r := newCallbackURLRegistry(db, d, time.Hour)

	if err := r.Add("http://one"); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("http://two"); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("http://one"); err != nil {
		t.Fatal(err)
	} // duplicate

	all, err := r.GetAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("got %v, want 2 URLs", all)
	}
}

// TestCallbackURLRegistry_RetentionWindow is the regression test for F-037 /
// issue #23. Pre-fix the SQL registry stored every URL forever; post-fix
// `last_seen_at` is refreshed by Add and rows older than the retention window
// no longer appear in GetAll (and the sweeper deletes them).
func TestCallbackURLRegistry_RetentionWindow(t *testing.T) {
	db, d := newTestDB(t)
	r := newCallbackURLRegistry(db, d, time.Hour)

	if err := r.Add("http://recent"); err != nil {
		t.Fatal(err)
	}

	// Insert a stale row by hand — simulates a URL whose last Add was far
	// outside the retention window.
	stale := "http://stale"
	q := fmt.Sprintf(
		"INSERT INTO callback_urls (callback_url, last_seen_at) VALUES (%s, %s)",
		d.placeholder(1), d.intervalSeconds(-2*int(time.Hour/time.Second)))
	if _, err := db.Exec(q, stale); err != nil {
		t.Fatalf("seed stale row: %v", err)
	}

	all, err := r.GetAll()
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	for _, u := range all {
		if u == stale {
			t.Fatalf("GetAll returned stale URL %q (retention window not enforced)", u)
		}
	}
	if len(all) != 1 || all[0] != "http://recent" {
		t.Fatalf("expected only http://recent, got %v", all)
	}

	// Re-Add the stale URL: that should refresh last_seen_at and bring it
	// back into the active window.
	if err := r.Add(stale); err != nil {
		t.Fatalf("re-Add stale: %v", err)
	}
	all, err = r.GetAll()
	if err != nil {
		t.Fatalf("GetAll after refresh: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 URLs after refresh, got %v", all)
	}
}

// TestCallbackURLRegistry_SweeperEvicts confirms the sweeper deletes stale
// URLs (issue #23). Without this, the table would grow unboundedly even
// though GetAll filters — and a long-running deployment would still pay the
// row-count cost for every Add (full-table scan on the unique index).
func TestCallbackURLRegistry_SweeperEvicts(t *testing.T) {
	db, d := newTestDB(t)
	r := newCallbackURLRegistry(db, d, time.Hour)

	if err := r.Add("http://recent"); err != nil {
		t.Fatal(err)
	}
	stale := "http://ancient"
	q := fmt.Sprintf(
		"INSERT INTO callback_urls (callback_url, last_seen_at) VALUES (%s, %s)",
		d.placeholder(1), d.intervalSeconds(-2*int(time.Hour/time.Second)))
	if _, err := db.Exec(q, stale); err != nil {
		t.Fatalf("seed stale row: %v", err)
	}

	// Use a sweeper interval that won't tick during the test — we drive it
	// directly via sweepOnce so the test is deterministic.
	sw := newSweeper(db, d, time.Hour, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	sw.sweepOnce(context.Background())

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM callback_urls").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("after sweep expected 1 row in callback_urls, got %d", n)
	}
}

func TestSeenCounter_ThresholdFiresOnce(t *testing.T) {
	db, d := newTestDB(t)
	s := newSeenCounter(db, d, 3)

	var firedCount int
	// Increment with 5 distinct subtreeIDs.
	for i := 0; i < 5; i++ {
		res, err := s.Increment("tx", fmt.Sprintf("st%d", i))
		if err != nil {
			t.Fatalf("Increment %d: %v", i, err)
		}
		if res.ThresholdReached {
			firedCount++
		}
	}
	if firedCount != 1 {
		t.Fatalf("threshold fired %d times, want exactly 1", firedCount)
	}

	// Duplicate subtreeID shouldn't bump count further.
	res, err := s.Increment("tx", "st0")
	if err != nil {
		t.Fatal(err)
	}
	if res.ThresholdReached {
		t.Fatal("duplicate subtreeID should not refire threshold")
	}
}

func TestSubtreeCounter_InitAndDecrement(t *testing.T) {
	db, d := newTestDB(t)
	s := newSubtreeCounter(db, d, 600)

	if err := s.Init("blk", 3); err != nil {
		t.Fatal(err)
	}
	for want := 2; want >= 0; want-- {
		got, err := s.Decrement("blk")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("Decrement = %d, want %d", got, want)
		}
	}
}

func TestSubtreeCounter_ConcurrentDecrement(t *testing.T) {
	db, d := newTestDB(t)
	s := newSubtreeCounter(db, d, 600)

	const initial = 50
	if err := s.Init("blk", initial); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]int, initial)
	for i := 0; i < initial; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := s.Decrement("blk")
			if err != nil {
				t.Errorf("Decrement: %v", err)
				return
			}
			results[i] = v
		}(i)
	}
	wg.Wait()

	// Every result in [0, initial-1] must appear exactly once.
	seen := map[int]bool{}
	for _, r := range results {
		if seen[r] {
			t.Fatalf("duplicate result %d", r)
		}
		seen[r] = true
	}
	for want := 0; want < initial; want++ {
		if !seen[want] {
			t.Fatalf("missing result %d", want)
		}
	}
}

func TestCallbackAccumulator_RoundTrip(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackAccumulator(db, d, 600)

	if err := s.Append("blk", "u1", []string{"tx1", "tx2"}, 0, []byte{0xAA}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("blk", "u1", []string{"tx3"}, 1, []byte{0xBB}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("blk", "u2", []string{"tx4"}, 0, []byte{0xCC}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ReadAndDelete("blk")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d URLs, want 2", len(got))
	}
	if len(got["u1"].Entries) != 2 {
		t.Fatalf("u1 entries = %d, want 2", len(got["u1"].Entries))
	}
	if len(got["u2"].Entries) != 1 {
		t.Fatalf("u2 entries = %d, want 1", len(got["u2"].Entries))
	}

	// Second read returns empty.
	got2, err := s.ReadAndDelete("blk")
	if err != nil {
		t.Fatal(err)
	}
	if got2 != nil {
		t.Fatalf("second read should be nil, got %v", got2)
	}
}

func TestSweeper_DeletesExpiredRows(t *testing.T) {
	db, d := newTestDB(t)
	dedup := newCallbackDedup(db, d)

	// Record one entry with a short TTL (will expire) and one with a long TTL.
	if err := dedup.Record("tx-expired", "u", "MINED", 1*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := dedup.Record("tx-fresh", "u", "MINED", 1*time.Hour); err != nil {
		t.Fatal(err)
	}

	// Wait for the first entry to expire, then sweep.
	time.Sleep(2100 * time.Millisecond)
	sw := newSweeper(db, d, 10*time.Millisecond, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	sw.sweepOnce(context.Background())

	// Verify the expired row is gone at the row level (not just filtered by
	// the expires_at check). Count via SELECT so we don't rely on Exists'
	// own expiry filter.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM callback_dedup WHERE dedup_key = ?",
		dedupKey("tx-expired", "u", "MINED")).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expired row still present after sweep: count=%d", count)
	}

	// Fresh entry still exists.
	ok, err := dedup.Exists("tx-fresh", "u", "MINED")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("fresh entry was deleted")
	}
}

func TestSweeper_StopsOnClose(t *testing.T) {
	db, d := newTestDB(t)
	sw := newSweeper(db, d, 5*time.Millisecond, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	ctx, cancel := context.WithCancel(context.Background())
	var started atomic.Bool
	go func() {
		started.Store(true)
		sw.run(ctx)
	}()
	// Let it tick a few times.
	time.Sleep(50 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		sw.waitStopped()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("sweeper did not stop after cancel")
	}
}

// Ensure all SQL stores actually satisfy the exported interfaces.
func TestSQLBackend_InterfaceSatisfaction(t *testing.T) {
	db, d := newTestDB(t)
	var (
		_ storepkg.RegistrationStore        = newRegistrationStore(db, d)
		_ storepkg.CallbackDedupStore       = newCallbackDedup(db, d)
		_ storepkg.CallbackURLRegistry      = newCallbackURLRegistry(db, d, time.Hour)
		_ storepkg.CallbackAccumulatorStore = newCallbackAccumulator(db, d, 60)
		_ storepkg.SeenCounterStore         = newSeenCounter(db, d, 3)
		_ storepkg.SubtreeCounterStore      = newSubtreeCounter(db, d, 60)
	)
}

func TestMigrations_Idempotent(t *testing.T) {
	db, d := newTestDB(t)
	// Run migrations a second time — should be a no-op.
	if err := runMigrations(context.Background(), db, d, nil); err != nil {
		t.Fatalf("second migration run failed: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("schema_migrations empty after run")
	}
}
