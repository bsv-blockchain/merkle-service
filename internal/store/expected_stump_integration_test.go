//go:build integration

package store_test

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// TestExpectedStump_Aerospike exercises the production store against a real
// cluster: idempotent re-adds (a re-driven subtree must not grow the set) and
// concurrent adds of distinct indices to the same (block, URL) — the property
// the design leans on, since different subtree workers append to the same
// record at once and Aerospike must apply each list append atomically.
func TestExpectedStump_Aerospike(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "expected_stumps")
	s := store.NewExpectedStumpStore(client, setName, 600, 3, 100, slog.Default())

	const block = "block-int"
	url := "https://a.example/cb"

	// Idempotent: re-adding index 4 five times leaves the set as {4}.
	for i := 0; i < 5; i++ {
		if err := s.AddSubtreeIndex(block, 4, []string{url}); err != nil {
			t.Fatalf("AddSubtreeIndex idempotent run %d: %v", i, err)
		}
	}
	got, err := s.GetSubtreeIndices(block, url)
	if err != nil {
		t.Fatalf("GetSubtreeIndices: %v", err)
	}
	if len(got) != 1 || got[0] != 4 {
		t.Fatalf("after idempotent re-adds: %v, want [4]", got)
	}

	// Concurrent distinct indices (0..49) into the SAME record — every one must
	// survive (no lost updates from concurrent list appends). Index 4 is already
	// present; add-unique keeps the union {0..49}.
	const n = 50
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := s.AddSubtreeIndex(block, idx, []string{url}); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent AddSubtreeIndex: %v", err)
	}

	got, err = s.GetSubtreeIndices(block, url)
	if err != nil {
		t.Fatalf("GetSubtreeIndices after concurrent adds: %v", err)
	}
	if len(got) != n {
		t.Fatalf("concurrent adds: got %d indices, want %d (lost update?)", len(got), n)
	}
	for i := 0; i < n; i++ {
		if got[i] != i {
			t.Fatalf("indices not the ascending set 0..%d: %v", n-1, got)
		}
	}

	// A multi-URL add records the index under each URL independently.
	urlB := "https://b.example/cb"
	if err := s.AddSubtreeIndex(block, 99, []string{url, urlB}); err != nil {
		t.Fatalf("multi-URL add: %v", err)
	}
	if got, _ := s.GetSubtreeIndices(block, urlB); len(got) != 1 || got[0] != 99 {
		t.Fatalf("urlB: %v, want [99]", got)
	}

	// An unmatched URL / unknown block reads empty, not an error.
	if got, err := s.GetSubtreeIndices(block, "https://none.example/cb"); err != nil || len(got) != 0 {
		t.Fatalf("unmatched URL: got %v err %v, want empty,nil", got, err)
	}
}
