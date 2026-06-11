package store

import (
	"fmt"
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
