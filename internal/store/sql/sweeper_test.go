package sql

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestSweeper_CascadesRegistrationChildren is the regression test for F-055
// (issue #17): when a parent row in `registrations` expires and is swept,
// the child rows in `registration_urls` for that txid must also be deleted
// so we don't leak an unbounded set of orphaned URLs.
func TestSweeper_CascadesRegistrationChildren(t *testing.T) {
	db, d := newTestDB(t)
	r := newRegistrationStore(db, d, 0)

	// Two txids: one we'll expire in the past, one fresh.
	if err := r.Add("tx-old", "http://old1", ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("tx-old", "http://old2", ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("tx-fresh", "http://fresh", ""); err != nil {
		t.Fatal(err)
	}

	// Push tx-old into the past, leave tx-fresh with no expiry.
	if err := r.UpdateTTL("tx-old", -1*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateTTL("tx-fresh", 1*time.Hour); err != nil {
		t.Fatal(err)
	}

	sw := newSweeper(db, d, time.Hour, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	sw.sweepOnce(context.Background())

	// Parent row should be gone.
	var parentCount int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM registrations WHERE txid = ?", "tx-old").Scan(&parentCount); err != nil {
		t.Fatal(err)
	}
	if parentCount != 0 {
		t.Fatalf("expired parent registrations row still present: count=%d", parentCount)
	}

	// And — the bug: child rows must also be gone.
	var orphanCount int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM registration_urls WHERE txid = ?", "tx-old").Scan(&orphanCount); err != nil {
		t.Fatal(err)
	}
	if orphanCount != 0 {
		t.Fatalf("orphan registration_urls rows after sweep: count=%d (want 0); F-055 regression",
			orphanCount)
	}

	// Fresh entry untouched.
	urls, err := r.Get("tx-fresh")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 || urls[0].URL != "http://fresh" {
		t.Fatalf("fresh registration mutated: got %v", urls)
	}
}

// TestSweeper_CascadesAccumulatorChildren is the matching regression for the
// callback_accumulator → callback_accumulator_entries pair.
func TestSweeper_CascadesAccumulatorChildren(t *testing.T) {
	db, d := newTestDB(t)
	// Use a -ve effective expiry by writing the parent's expires_at directly
	// in the past; Append's TTL is positive. We bypass Append for the expired
	// entry to avoid waiting on real time, then use a normal Append for fresh.
	a := newCallbackAccumulator(db, d, 600)
	if err := a.Append("blk-fresh", "http://u", []string{"tx1"}, 0, []byte{0xAA}); err != nil {
		t.Fatal(err)
	}

	// Insert an expired parent + entries directly to simulate what would
	// otherwise require waiting on real time. The shape mirrors what Append
	// would produce.
	if _, err := db.ExecContext(context.Background(),
		"INSERT INTO callback_accumulator (block_hash, expires_at) VALUES (?, datetime('now', '-1 hour'))",
		"blk-old"); err != nil {
		t.Fatal(err)
	}
	for i, txns := range [][]string{{"tx-old-1"}, {"tx-old-2", "tx-old-3"}} {
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO callback_accumulator_entries
                (block_hash, callback_url, subtree_index, txids_json, stump_data)
             VALUES (?, ?, ?, ?, ?)`,
			"blk-old", "http://u", i, mustJSON(txns), []byte{0xBB}); err != nil {
			t.Fatal(err)
		}
	}

	sw := newSweeper(db, d, time.Hour, time.Hour, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	sw.sweepOnce(context.Background())

	// Parent gone.
	var parentCount int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM callback_accumulator WHERE block_hash = ?", "blk-old").Scan(&parentCount); err != nil {
		t.Fatal(err)
	}
	if parentCount != 0 {
		t.Fatalf("expired callback_accumulator row still present: count=%d", parentCount)
	}

	// No orphan child rows.
	var orphanCount int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM callback_accumulator_entries WHERE block_hash = ?", "blk-old").Scan(&orphanCount); err != nil {
		t.Fatal(err)
	}
	if orphanCount != 0 {
		t.Fatalf("orphan callback_accumulator_entries rows after sweep: count=%d (want 0); F-055 regression",
			orphanCount)
	}

	// Fresh accumulator entries survived.
	got, err := a.ReadAndDelete("blk-fresh")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got["http://u"].Entries) != 1 {
		t.Fatalf("fresh accumulator mutated: got %v", got)
	}
}

// mustJSON is a tiny helper to build a JSON string for an array of txids.
// We construct it by hand to avoid pulling encoding/json into a test that's
// otherwise stdlib-light.
func mustJSON(items []string) string {
	var b []byte
	b = append(b, '[')
	for i, s := range items {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '"')
		b = append(b, s...)
		b = append(b, '"')
	}
	b = append(b, ']')
	return string(b)
}
