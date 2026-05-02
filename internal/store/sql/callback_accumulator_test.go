package sql

import (
	"sync"
	"testing"
)

// TestCallbackAccumulator_HappyPath asserts the basic Append → ReadAndDelete
// round trip in isolation (sibling to the existing TestCallbackAccumulator_RoundTrip
// in sql_test.go but exercising the single-Append path explicitly).
func TestCallbackAccumulator_HappyPath(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackAccumulator(db, d, 600)

	if err := s.Append("blk", "u", []string{"tx1"}, 0, []byte{0x01}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.ReadAndDelete("blk")
	if err != nil {
		t.Fatalf("ReadAndDelete: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d URLs, want 1", len(got))
	}
	acc, ok := got["u"]
	if !ok || len(acc.Entries) != 1 {
		t.Fatalf("got %v, want one entry under \"u\"", got)
	}
	if acc.Entries[0].SubtreeIndex != 0 || len(acc.Entries[0].TxIDs) != 1 || acc.Entries[0].TxIDs[0] != "tx1" {
		t.Fatalf("unexpected entry: %+v", acc.Entries[0])
	}

	// And the parent row is gone — a second read returns nil.
	got2, err := s.ReadAndDelete("blk")
	if err != nil {
		t.Fatalf("ReadAndDelete (second): %v", err)
	}
	if got2 != nil {
		t.Fatalf("second read should be nil, got %v", got2)
	}
}

// TestCallbackAccumulator_ReadAndDelete_Empty asserts the early-return branch
// (no parent row → nil, nil with no error and no orphan entries).
func TestCallbackAccumulator_ReadAndDelete_Empty(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackAccumulator(db, d, 600)

	got, err := s.ReadAndDelete("never-seen")
	if err != nil {
		t.Fatalf("ReadAndDelete on missing block: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil result, got %v", got)
	}
}

// TestCallbackAccumulator_NoLostEntries_F046 is the regression test for F-046:
// concurrent Appends interleaved with ReadAndDelete must never lose entries.
//
// Invariant:
//
//	(entries returned by ReadAndDelete) ∪ (entries still in DB after) ==
//	(all entries successfully Appended)
//
// The original bug was that under Postgres READ COMMITTED, an Append committing
// between a ReadAndDelete's SELECT and its DELETE on callback_accumulator_entries
// would have its row removed without ever being returned. SQLite serializes
// writers globally so a single-connection harness like newTestDB cannot itself
// reproduce the race — running this on Postgres would. The test still encodes
// the invariant so any future regression that can leak entries (on either
// backend) is caught, and it exercises the new lock + DELETE...RETURNING fast
// path under -race.
func TestCallbackAccumulator_NoLostEntries_F046(t *testing.T) {
	db, d := newTestDB(t)
	s := newCallbackAccumulator(db, d, 600)

	const blockHash = "blk-race"

	// Seed one entry so the ReadAndDelete has something to consume.
	if err := s.Append(blockHash, "uA", []string{"tx1"}, 0, []byte{0xAA}); err != nil {
		t.Fatalf("seed Append: %v", err)
	}

	// Track every entry we successfully append so we can compare against
	// what ReadAndDelete returns plus what's left in the DB afterwards.
	type want struct {
		url      string
		subtree  int
		firstTxn string
	}
	var (
		mu       sync.Mutex
		appended = []want{{url: "uA", subtree: 0, firstTxn: "tx1"}}
	)

	// Spawn N concurrent Appends and one ReadAndDelete. Every Append that
	// returns nil is a commit we must not lose.
	const concurrentAppends = 8
	var wg sync.WaitGroup

	var rdResult map[string]*struct{} // unused; we use the real result below
	_ = rdResult
	var rdEntries map[string][]int // url -> subtree indices returned

	wg.Add(1)
	go func() {
		defer wg.Done()
		got, err := s.ReadAndDelete(blockHash)
		if err != nil {
			t.Errorf("ReadAndDelete: %v", err)
			return
		}
		// Capture for assertions below; map access is safe — wg.Wait fences it.
		rdEntries = make(map[string][]int, len(got))
		for url, acc := range got {
			for _, e := range acc.Entries {
				rdEntries[url] = append(rdEntries[url], e.SubtreeIndex)
			}
		}
	}()

	for i := 0; i < concurrentAppends; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			url := "uB"
			tx := "txB" + itoa(i)
			subtree := i + 1
			if err := s.Append(blockHash, url, []string{tx}, subtree, []byte{byte(i)}); err != nil { //nolint:gosec // i is bounded test counter
				t.Errorf("concurrent Append %d: %v", i, err)
				return
			}
			mu.Lock()
			appended = append(appended, want{url: url, subtree: subtree, firstTxn: tx})
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// What remains in the DB after the (single) ReadAndDelete?
	leftover, err := s.ReadAndDelete(blockHash)
	if err != nil {
		t.Fatalf("post ReadAndDelete: %v", err)
	}

	// Build the union of (returned by first ReadAndDelete) ∪ (remaining in DB).
	type seenKey struct {
		url     string
		subtree int
	}
	seen := map[seenKey]int{}
	for url, indices := range rdEntries {
		for _, st := range indices {
			seen[seenKey{url, st}]++
		}
	}
	for url, acc := range leftover {
		for _, e := range acc.Entries {
			seen[seenKey{url, e.SubtreeIndex}]++
		}
	}

	// Every successfully appended entry must appear exactly once in the union.
	if len(seen) != len(appended) {
		t.Fatalf("union has %d entries, appended %d (lost or duplicated): seen=%v appended=%v",
			len(seen), len(appended), seen, appended)
	}
	for _, w := range appended {
		k := seenKey{w.url, w.subtree}
		c, ok := seen[k]
		if !ok {
			t.Errorf("entry %+v was silently dropped (not returned and not in DB)", w)
			continue
		}
		if c != 1 {
			t.Errorf("entry %+v appeared %d times (want 1)", w, c)
		}
	}
}

// itoa is a tiny no-import int-to-string used by the race regression test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
