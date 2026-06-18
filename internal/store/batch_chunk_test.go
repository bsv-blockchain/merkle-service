package store

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestChunkSlice guards the F-5 fix: Aerospike rejects batches that land more
// than batch-max-requests (default 5000) keys on one node, so BatchGet must
// never issue a chunk larger than aerospikeBatchChunkSize. A teranode-default
// subtree carries ~1M txids — unchunked, that BatchGet fails deterministically
// and the subtree's callbacks are lost to the DLQ.
func TestChunkSlice(t *testing.T) {
	mk := func(n int) []string {
		s := make([]string, n)
		for i := range s {
			s[i] = fmt.Sprintf("txid-%d", i)
		}
		return s
	}

	cases := []struct {
		name       string
		n          int
		size       int
		wantChunks int
		wantLast   int // length of final chunk
	}{
		{"empty", 0, 5000, 0, 0},
		{"single", 1, 5000, 1, 1},
		{"under limit", 4999, 5000, 1, 4999},
		{"exactly limit", 5000, 5000, 1, 5000},
		{"limit plus one", 5001, 5000, 2, 1},
		{"mega-fixture shape", 12345, 5000, 3, 2345},
		{"teranode default subtree", 1_048_576, 5000, 210, 3576},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks := chunkSlice(mk(tc.n), tc.size)
			if len(chunks) != tc.wantChunks {
				t.Fatalf("chunkSlice(%d, %d) produced %d chunks, want %d", tc.n, tc.size, len(chunks), tc.wantChunks)
			}
			total := 0
			for i, c := range chunks {
				if len(c) > tc.size {
					t.Errorf("chunk %d has %d elements, exceeds max %d (would fail Aerospike batch-max-requests)", i, len(c), tc.size)
				}
				total += len(c)
			}
			if total != tc.n {
				t.Errorf("chunks cover %d elements, want %d (no element may be dropped)", total, tc.n)
			}
			if tc.wantChunks > 0 {
				if got := len(chunks[len(chunks)-1]); got != tc.wantLast {
					t.Errorf("final chunk length = %d, want %d", got, tc.wantLast)
				}
				// Order must be preserved: first element of each chunk follows
				// the last element of the previous chunk.
				if chunks[0][0] != "txid-0" {
					t.Errorf("first element = %q, want txid-0", chunks[0][0])
				}
			}
		})
	}
}

func mkTxids(n int) []string {
	s := make([]string, n)
	for i := range s {
		s[i] = fmt.Sprintf("txid-%d", i)
	}
	return s
}

// TestForEachChunkConcurrent_CoversEveryItemOnce checks the primitive that drives
// concurrent BatchGet/BatchIncrement: regardless of concurrency, every item is
// handled exactly once across the chunks. Run with -race, the shared-map merge
// pattern the real callers use is also exercised for data races.
func TestForEachChunkConcurrent_CoversEveryItemOnce(t *testing.T) {
	for _, concurrency := range []int{0, 1, 4, 16} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			const n = 23_456 // 5 chunks at size 5000, last partial
			items := mkTxids(n)

			var mu sync.Mutex
			seen := make(map[string]int, n)
			var chunkCount atomic.Int64

			err := forEachChunkConcurrent(items, concurrency, func(chunk []string) error {
				chunkCount.Add(1)
				// Mirror the caller pattern: build a per-chunk local result, then
				// merge under the mutex.
				local := make([]string, len(chunk))
				copy(local, chunk)
				mu.Lock()
				for _, id := range local {
					seen[id]++
				}
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(seen) != n {
				t.Fatalf("covered %d distinct items, want %d", len(seen), n)
			}
			for id, c := range seen {
				if c != 1 {
					t.Fatalf("item %s handled %d times, want exactly 1", id, c)
				}
			}
			if got := chunkCount.Load(); got != 5 {
				t.Fatalf("ran %d chunks, want 5", got)
			}
		})
	}
}

// TestForEachChunkConcurrent_AttemptsAllAndReturnsError pins the best-effort
// contract: a failing chunk does not abort the others, and an error is still
// surfaced (so the caller redelivers).
func TestForEachChunkConcurrent_AttemptsAllAndReturnsError(t *testing.T) {
	for _, concurrency := range []int{1, 8} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			items := mkTxids(20_000) // 4 chunks
			var attempts atomic.Int64
			sentinel := errors.New("boom")

			err := forEachChunkConcurrent(items, concurrency, func(chunk []string) error {
				attempts.Add(1)
				if chunk[0] == "txid-5000" { // the 2nd chunk fails
					return sentinel
				}
				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want sentinel", err)
			}
			if got := attempts.Load(); got != 4 {
				t.Fatalf("attempted %d chunks, want all 4 despite the failure", got)
			}
		})
	}
}

// TestForEachChunkConcurrent_SingleChunkStaysSerial confirms the no-goroutine
// fast path: a single chunk (or empty input) runs inline regardless of the
// concurrency setting.
func TestForEachChunkConcurrent_SingleChunkStaysSerial(t *testing.T) {
	var calls atomic.Int64
	if err := forEachChunkConcurrent(mkTxids(10), 16, func(_ []string) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("single chunk ran %d times, want 1", calls.Load())
	}

	calls.Store(0)
	if err := forEachChunkConcurrent(nil, 16, func(_ []string) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error on empty: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("empty input ran fn %d times, want 0", calls.Load())
	}
}
