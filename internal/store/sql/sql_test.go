package sql

import (
	"context"
	"database/sql"
	"errors"
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
	t.Cleanup(func() { _ = db.Close() })

	d := sqliteDialect()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := runMigrations(context.Background(), db, d, logger); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return db, d
}

func TestRegistrationStore_IdempotentAdd(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d, 0)

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
	s := newRegistrationStore(db, d, 0)
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

// TestRegistrationStore_MaxCallbacksPerTxID covers F-050: once the per-txid
// callback URL cap is reached, further Add calls return the sentinel error,
// the row count in registration_urls is exactly the cap, and existing rows
// are not mutated.
func TestRegistrationStore_MaxCallbacksPerTxID(t *testing.T) {
	db, d := newTestDB(t)
	const max = 3
	s := newRegistrationStore(db, d, max)

	// First `max` distinct URLs succeed.
	for i := 0; i < max; i++ {
		if err := s.Add("tx1", fmt.Sprintf("http://cb/%d", i)); err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
	}

	// (max+1)-th distinct URL is rejected with the sentinel.
	err := s.Add("tx1", "http://cb/overflow")
	if !errors.Is(err, storepkg.ErrMaxCallbacksPerTxIDExceeded) {
		t.Fatalf("expected ErrMaxCallbacksPerTxIDExceeded, got %v", err)
	}

	// DB count is exactly `max` — the rejected insert did not slip through.
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM registration_urls WHERE txid = ?", "tx1").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != max {
		t.Fatalf("registration_urls count = %d, want %d", count, max)
	}

	// A different txid is unaffected by the first txid's cap.
	if err := s.Add("tx2", "http://cb/tx2"); err != nil {
		t.Fatalf("unrelated txid Add: %v", err)
	}
}

// TestRegistrationStore_MaxCallbacksIdempotent verifies that re-registering
// an already-known URL is a no-op and never trips the cap, even when the
// txid is already at `max` distinct URLs.
func TestRegistrationStore_MaxCallbacksIdempotent(t *testing.T) {
	db, d := newTestDB(t)
	const max = 2
	s := newRegistrationStore(db, d, max)

	if err := s.Add("tx1", "http://cb/a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("tx1", "http://cb/b"); err != nil {
		t.Fatal(err)
	}

	// At cap: re-adding either existing URL must succeed (idempotent set add).
	for _, url := range []string{"http://cb/a", "http://cb/b"} {
		if err := s.Add("tx1", url); err != nil {
			t.Fatalf("idempotent re-add of %q: %v", url, err)
		}
	}

	// And a NEW URL still trips the cap.
	if err := s.Add("tx1", "http://cb/c"); !errors.Is(err, storepkg.ErrMaxCallbacksPerTxIDExceeded) {
		t.Fatalf("expected ErrMaxCallbacksPerTxIDExceeded, got %v", err)
	}

	// Count should still be exactly `max`.
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM registration_urls WHERE txid = ?", "tx1").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != max {
		t.Fatalf("registration_urls count = %d, want %d", count, max)
	}
}

// TestRegistrationStore_MaxCallbacksDisabled covers the legacy unbounded
// path: when maxCallbacksPerTxID is 0 the cap is disabled and arbitrarily
// many URLs may be registered without ever returning the sentinel.
func TestRegistrationStore_MaxCallbacksDisabled(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d, 0)

	for i := 0; i < 25; i++ {
		if err := s.Add("tx1", fmt.Sprintf("http://cb/%d", i)); err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
	}
	urls, err := s.Get("tx1")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 25 {
		t.Fatalf("got %d urls, want 25 (cap disabled)", len(urls))
	}
}

func TestCallbackDedup_ClaimIsAtomic(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackDedup(db, d)

	// First call wins the claim.
	claimed, err := s.Claim("tx", "url", "MINED", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("first Claim should succeed")
	}

	// Subsequent calls observe the existing claim and return false (duplicate).
	for i := 0; i < 3; i++ {
		claimed, err = s.Claim("tx", "url", "MINED", 1*time.Hour)
		if err != nil {
			t.Fatalf("repeat Claim error: %v", err)
		}
		if claimed {
			t.Fatalf("repeat Claim #%d should report duplicate", i)
		}
	}

	// Distinct tuple is independent.
	claimed, err = s.Claim("tx", "url", "STUMP", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("Claim for distinct statusType should succeed")
	}
}

func TestCallbackDedup_TTLExpiryAllowsReclaim(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackDedup(db, d)

	// 1-second TTL so we can observe expiry without waiting for the sweeper.
	claimed, err := s.Claim("tx", "url", "MINED", 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("initial Claim should succeed")
	}

	// Immediately re-claim → duplicate, still within TTL.
	claimed, err = s.Claim("tx", "url", "MINED", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("Claim within TTL should be marked duplicate")
	}

	// SQLite's CURRENT_TIMESTAMP has second precision; pad by 2s past TTL.
	time.Sleep(2100 * time.Millisecond)

	// Expired row should be evicted by Claim's pre-INSERT cleanup, so the
	// next claim wins again.
	claimed, err = s.Claim("tx", "url", "MINED", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("Claim after TTL expiry should succeed")
	}
}

func TestCallbackDedup_ConcurrentClaimsAtMostOneWins(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackDedup(db, d)

	const workers = 16
	var wg sync.WaitGroup
	wins := make(chan bool, workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			ok, err := s.Claim("race-tx", "race-url", "MINED", 1*time.Hour)
			if err != nil {
				t.Errorf("Claim error: %v", err)
				return
			}
			wins <- ok
		}()
	}
	wg.Wait()
	close(wins)

	winCount := 0
	for w := range wins {
		if w {
			winCount++
		}
	}
	if winCount != 1 {
		t.Fatalf("expected exactly 1 winning Claim across %d workers, got %d", workers, winCount)
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
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO callback_urls (callback_url, last_seen_at) VALUES (%s, %s)",
		d.placeholder(1), d.intervalSeconds(-2*int(time.Hour/time.Second)))
	if _, err := db.ExecContext(context.Background(), q, stale); err != nil {
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
	if err = r.Add(stale); err != nil {
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
	q := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholder functions, no user input
		"INSERT INTO callback_urls (callback_url, last_seen_at) VALUES (%s, %s)",
		d.placeholder(1), d.intervalSeconds(-2*int(time.Hour/time.Second)))
	if _, err := db.ExecContext(context.Background(), q, stale); err != nil {
		t.Fatalf("seed stale row: %v", err)
	}

	// Use a sweeper interval that won't tick during the test — we drive it
	// directly via sweepOnce so the test is deterministic.
	sw := newSweeper(db, d, time.Hour, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	sw.sweepOnce(context.Background())

	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM callback_urls").Scan(&n); err != nil {
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

// TestSeenCounter_ConcurrentThresholdFiresOnce is the F-045 regression test:
// many goroutines call Increment for the same txid past the threshold and
// exactly one must observe ThresholdReached=true. The previous
// read-then-update sequence allowed two concurrent callers to both see
// fired=0 and both report fired, producing duplicate SEEN_MULTIPLE_NODES
// callbacks. The fix folds the check + flip into a single conditional UPDATE
// so only one caller wins.
func TestSeenCounter_ConcurrentThresholdFiresOnce(t *testing.T) {
	tmp := t.TempDir() + "/concurrent.db"
	// Allow multiple connections so goroutines actually contend; SQLite
	// serializes writes via its file lock, which is exactly the property we
	// rely on for atomicity. Without WAL + multiple conns the test would just
	// queue every goroutine through one conn and never exercise the race.
	db, err := sql.Open("sqlite", "file:"+tmp+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })

	d := sqliteDialect()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := runMigrations(context.Background(), db, d, logger); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	const (
		threshold = 5
		workers   = 32
	)
	s := newSeenCounter(db, d, threshold)

	var firedCount int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := s.Increment("tx-race", fmt.Sprintf("st%d", i))
			if err != nil {
				t.Errorf("Increment %d: %v", i, err)
				return
			}
			if res.ThresholdReached {
				atomic.AddInt64(&firedCount, 1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt64(&firedCount); got != 1 {
		t.Fatalf("F-045 regression: threshold fired %d times across %d concurrent workers, want exactly 1", got, workers)
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

	// Claim one entry with a short TTL (will expire) and one with a long TTL.
	if claimed, err := dedup.Claim("tx-expired", "u", "MINED", 1*time.Second); err != nil || !claimed {
		t.Fatalf("claim tx-expired: claimed=%v err=%v", claimed, err)
	}
	if claimed, err := dedup.Claim("tx-fresh", "u", "MINED", 1*time.Hour); err != nil || !claimed {
		t.Fatalf("claim tx-fresh: claimed=%v err=%v", claimed, err)
	}

	// Wait for the first entry to expire, then sweep.
	time.Sleep(2100 * time.Millisecond)
	sw := newSweeper(db, d, 10*time.Millisecond, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	sw.sweepOnce(context.Background())

	// Verify the expired row is gone at the row level (not just filtered by
	// the expires_at check). Count via SELECT so we don't rely on Claim's
	// own expiry-cleanup behavior.
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM callback_dedup WHERE dedup_key = ?",
		dedupKey("tx-expired", "u", "MINED")).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expired row still present after sweep: count=%d", count)
	}

	// Fresh entry still claimed → second Claim returns duplicate.
	claimed, err := dedup.Claim("tx-fresh", "u", "MINED", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("fresh entry should still be claimed (duplicate Claim)")
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
		_ storepkg.RegistrationStore        = newRegistrationStore(db, d, 0)
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
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("schema_migrations empty after run")
	}
}
