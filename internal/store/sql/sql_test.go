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
		if err := s.Add("tx1", "http://cb1", ""); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	got, err := s.Get("tx1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 1 || got[0].URL != "http://cb1" {
		t.Fatalf("got %v, want [http://cb1]", got)
	}
}

// TestRegistrationStore_TokenRoundTrip verifies the new callback_token column
// is persisted and retrieved on /watch round-trip, including the
// "re-registration refreshes the token" semantics that let arcade rotate
// without bouncing every txid.
func TestRegistrationStore_TokenRoundTrip(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d, 0)

	if err := s.Add("tx1", "http://cb", "tok-v1"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := s.Get("tx1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 1 || got[0].URL != "http://cb" || got[0].Token != "tok-v1" {
		t.Fatalf("first round-trip: got %+v, want [{http://cb tok-v1}]", got)
	}

	// Re-register the same URL with a rotated token: Get should now return
	// the new token (refresh-on-conflict).
	if addErr := s.Add("tx1", "http://cb", "tok-v2"); addErr != nil {
		t.Fatalf("Add (rotation): %v", addErr)
	}
	got, err = s.Get("tx1")
	if err != nil {
		t.Fatalf("Get after rotation: %v", err)
	}
	if len(got) != 1 || got[0].Token != "tok-v2" {
		t.Fatalf("rotated round-trip: got %+v, want token tok-v2", got)
	}

	// Empty token is a valid value (back-compat: deployments where arcade
	// hasn't shipped the matching change). Get returns Token = "".
	if addErr := s.Add("tx2", "http://cb2", ""); addErr != nil {
		t.Fatalf("Add empty token: %v", addErr)
	}
	got, err = s.Get("tx2")
	if err != nil {
		t.Fatalf("Get empty token: %v", err)
	}
	if len(got) != 1 || got[0].Token != "" {
		t.Fatalf("empty-token round-trip: got %+v, want token \"\"", got)
	}
}

func TestRegistrationStore_BatchGet(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d, 0)
	if err := s.Add("a", "u1", "tok-a1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("a", "u2", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("b", "u3", "tok-b"); err != nil {
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
	// Locate u1 and verify token survived the batch fetch.
	foundToken := false
	for _, e := range got["a"] {
		if e.URL == "u1" && e.Token == "tok-a1" {
			foundToken = true
		}
	}
	if !foundToken {
		t.Fatalf("expected (u1, tok-a1) in got[\"a\"]: %+v", got["a"])
	}
	if got["b"][0].Token != "tok-b" {
		t.Fatalf("got[\"b\"][0].Token = %q, want tok-b", got["b"][0].Token)
	}
	if _, ok := got["c"]; ok {
		t.Fatalf("c should not be present")
	}
}

// TestRegistrationStore_BatchChunking is the regression test for the Postgres
// "extended protocol limited to 65535 parameters" failure. Subtrees can carry
// 2^17+ txids; before chunking, BatchGet/BatchUpdateTTL emitted one bind
// parameter per txid in a single statement and Postgres rejected the prepared
// statement, sending the subtree to DLQ after maxAttempts retries. The fix
// chunks both calls into batches of batchParamChunkSize. SQLite has no
// parameter cap, so this test exercises the chunking logic by passing
// 2*batchParamChunkSize+1 txids and verifying results merge correctly across
// chunks.
func TestRegistrationStore_BatchChunking(t *testing.T) {
	db, d := newTestDB(t)
	s := newRegistrationStore(db, d, 0)

	const total = 2*batchParamChunkSize + 1
	// Register half of the txids so BatchGet has both hits and misses to merge.
	registered := make(map[string]bool, total/2)
	for i := 0; i < total; i += 2 {
		txid := fmt.Sprintf("tx%06d", i)
		if err := s.Add(txid, fmt.Sprintf("http://cb/%d", i), fmt.Sprintf("tok-%d", i)); err != nil {
			t.Fatalf("Add %s: %v", txid, err)
		}
		registered[txid] = true
	}

	txids := make([]string, total)
	for i := 0; i < total; i++ {
		txids[i] = fmt.Sprintf("tx%06d", i)
	}

	got, err := s.BatchGet(txids)
	if err != nil {
		t.Fatalf("BatchGet across chunks: %v", err)
	}
	if len(got) != len(registered) {
		t.Fatalf("BatchGet returned %d txids, want %d", len(got), len(registered))
	}
	for txid := range registered {
		entries, ok := got[txid]
		if !ok {
			t.Fatalf("missing txid %s in BatchGet result", txid)
		}
		if len(entries) != 1 {
			t.Fatalf("txid %s: %d entries, want 1", txid, len(entries))
		}
	}

	// BatchUpdateTTL must also chunk without erroring out.
	if err := s.BatchUpdateTTL(txids, time.Hour); err != nil {
		t.Fatalf("BatchUpdateTTL across chunks: %v", err)
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
		if err := s.Add("tx1", fmt.Sprintf("http://cb/%d", i), ""); err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
	}

	// (max+1)-th distinct URL is rejected with the sentinel.
	err := s.Add("tx1", "http://cb/overflow", "")
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
	if err := s.Add("tx2", "http://cb/tx2", ""); err != nil {
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

	if err := s.Add("tx1", "http://cb/a", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("tx1", "http://cb/b", ""); err != nil {
		t.Fatal(err)
	}

	// At cap: re-adding either existing URL must succeed (idempotent set add).
	for _, url := range []string{"http://cb/a", "http://cb/b"} {
		if err := s.Add("tx1", url, ""); err != nil {
			t.Fatalf("idempotent re-add of %q: %v", url, err)
		}
	}

	// And a NEW URL still trips the cap.
	if err := s.Add("tx1", "http://cb/c", ""); !errors.Is(err, storepkg.ErrMaxCallbacksPerTxIDExceeded) {
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
		if err := s.Add("tx1", fmt.Sprintf("http://cb/%d", i), ""); err != nil {
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

	if err = s.Record("tx", "url", "MINED", 1*time.Hour); err != nil {
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

	if err := r.Add("http://one", "tok-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("http://two", ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("http://one", "tok-1-rotated"); err != nil {
		t.Fatal(err)
	} // duplicate URL — token refreshes

	all, err := r.GetAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("got %v, want 2 URLs", all)
	}
	// Verify token is preserved end-to-end and that re-Add rotates it.
	tokenByURL := map[string]string{}
	for _, e := range all {
		tokenByURL[e.URL] = e.Token
	}
	if tokenByURL["http://one"] != "tok-1-rotated" {
		t.Fatalf("expected http://one token tok-1-rotated, got %q", tokenByURL["http://one"])
	}
	if tokenByURL["http://two"] != "" {
		t.Fatalf("expected http://two token \"\", got %q", tokenByURL["http://two"])
	}
}

// TestCallbackURLRegistry_RetentionWindow is the regression test for F-037 /
// issue #23. Pre-fix the SQL registry stored every URL forever; post-fix
// `last_seen_at` is refreshed by Add and rows older than the retention window
// no longer appear in GetAll (and the sweeper deletes them).
func TestCallbackURLRegistry_RetentionWindow(t *testing.T) {
	db, d := newTestDB(t)
	r := newCallbackURLRegistry(db, d, time.Hour)

	if err := r.Add("http://recent", ""); err != nil {
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
	for _, e := range all {
		if e.URL == stale {
			t.Fatalf("GetAll returned stale URL %q (retention window not enforced)", e.URL)
		}
	}
	if len(all) != 1 || all[0].URL != "http://recent" {
		t.Fatalf("expected only http://recent, got %v", all)
	}

	// Re-Add the stale URL: that should refresh last_seen_at and bring it
	// back into the active window.
	if err = r.Add(stale, ""); err != nil {
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

	if err := r.Add("http://recent", ""); err != nil {
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

// TestSubtreeCounter_DecrementMissingRow verifies that Decrement returns
// (0, nil) — not sql.ErrNoRows — when the counter row doesn't exist. This is
// the production scenario where an Init never ran (or the sweeper purged the
// row before all subtrees finished). Propagating ErrNoRows from this path
// wedges the F-013 DLQ branch in subtree_worker.go: that branch republishes
// the work item to the DLQ topic and then calls Decrement to drive
// BLOCK_PROCESSED, but if Decrement keeps failing the message is never
// ack'd and the partition gets stuck in an infinite redelivery loop.
// Treating the missing row as "already drained" lets the DLQ branch ack and
// the partition advance.
func TestSubtreeCounter_DecrementMissingRow(t *testing.T) {
	db, d := newTestDB(t)
	s := newSubtreeCounter(db, d, 600)

	got, err := s.Decrement("nonexistent-block")
	if err != nil {
		t.Fatalf("Decrement on missing row should not return error, got: %v", err)
	}
	if got != 0 {
		t.Fatalf("Decrement on missing row = %d, want 0", got)
	}

	// Sanity check: real ErrNoRows must not leak through even after an
	// underlying failure has been observed once.
	if err := s.Init("real-blk", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Decrement("real-blk"); err != nil {
		t.Fatalf("Decrement on existing row: %v", err)
	}
	got, err = s.Decrement("still-nonexistent")
	if err != nil {
		t.Fatalf("Decrement on missing row should not return error, got: %v", err)
	}
	if got != 0 {
		t.Fatalf("Decrement on missing row = %d, want 0", got)
	}
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Decrement leaked sql.ErrNoRows for missing row")
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

// TestSubtreeCounter_ConcurrentDecrementMultiConn is the F-052 regression
// test. The previous implementation opened the SQLite transaction with the
// default deferred isolation, so two connections could both run the
// SELECT before either ran the UPDATE — each saw the same `remaining`
// value and wrote back the same decremented value, silently losing a
// decrement. The fix opens the txn with sql.LevelSerializable, which
// modernc.org/sqlite implements as BEGIN IMMEDIATE: the second connection
// blocks at BEGIN until the first commits, so the read+write pair is
// serialized.
//
// To actually exercise the race we configure the pool with multiple
// connections (the shared newTestDB helper pins MaxOpenConns to 1 and
// trivially serializes everything through the single conn).
func TestSubtreeCounter_ConcurrentDecrementMultiConn(t *testing.T) {
	tmp := t.TempDir() + "/subtree_race.db"
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

	s := newSubtreeCounter(db, d, 600)
	const initial = 64
	if err := s.Init("blk", initial); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]int, initial)
	start := make(chan struct{})
	for i := 0; i < initial; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			v, err := s.Decrement("blk")
			if err != nil {
				t.Errorf("Decrement: %v", err)
				return
			}
			results[i] = v
		}(i)
	}
	close(start)
	wg.Wait()

	// Every result in [0, initial-1] must appear exactly once. Pre-fix this
	// would fail with duplicates and missing values when two goroutines
	// raced through the SELECT-then-UPDATE.
	seen := map[int]bool{}
	for _, r := range results {
		if seen[r] {
			t.Fatalf("F-052 regression: duplicate Decrement result %d (lost a decrement)", r)
		}
		seen[r] = true
	}
	for want := 0; want < initial; want++ {
		if !seen[want] {
			t.Fatalf("F-052 regression: missing Decrement result %d", want)
		}
	}

	// Final stored value must be 0, not some larger value left behind by
	// clobbered writes.
	var remaining int
	if err := db.QueryRowContext(context.Background(), "SELECT remaining FROM subtree_counters WHERE block_hash = ?", "blk").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("final remaining = %d, want 0", remaining)
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
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM callback_dedup WHERE dedup_key = ?",
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
