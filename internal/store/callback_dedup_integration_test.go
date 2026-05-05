//go:build integration

package store_test

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// TestCallbackDedup_ClaimIsAtomic exercises the CREATE_ONLY-backed Claim
// against a real Aerospike: the first writer for a tuple wins, every other
// writer (including concurrent ones) observes claimed=false. This is the
// regression test for issue #29 — the previous Exists+Record pair allowed
// two concurrent workers to both observe "not exists" and double-deliver.
func TestCallbackDedup_ClaimIsAtomic(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "dedup_claim")
	s := store.NewCallbackDedupStore(client, setName, 3, 100, slog.Default())

	claimed, err := s.Claim("tx", "url", "MINED", time.Hour)
	if err != nil {
		t.Fatalf("first Claim error: %v", err)
	}
	if !claimed {
		t.Fatal("first Claim should win")
	}

	// Repeat claims for the same tuple are duplicates.
	for i := 0; i < 3; i++ {
		dup, err := s.Claim("tx", "url", "MINED", time.Hour)
		if err != nil {
			t.Fatalf("repeat Claim error: %v", err)
		}
		if dup {
			t.Fatalf("repeat Claim #%d should be marked duplicate", i)
		}
	}

	// Distinct tuples are independent claims.
	other, err := s.Claim("tx", "url", "STUMP", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !other {
		t.Fatal("Claim for distinct statusType should win")
	}
}

// TestCallbackDedup_ConcurrentClaimsAtMostOneWins is the explicit race
// regression: N goroutines race for the same tuple, only one is allowed to
// observe claimed=true. CREATE_ONLY surfaces the others as KEY_EXISTS_ERROR
// which we map to claimed=false.
func TestCallbackDedup_ConcurrentClaimsAtMostOneWins(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "dedup_race")
	s := store.NewCallbackDedupStore(client, setName, 3, 100, slog.Default())

	const workers = 32
	results := make(chan bool, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			ok, err := s.Claim("race-tx", "race-url", "MINED", time.Hour)
			if err != nil {
				t.Errorf("Claim error: %v", err)
				return
			}
			results <- ok
		}()
	}
	wg.Wait()
	close(results)

	wins := 0
	for r := range results {
		if r {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 winning Claim, got %d (across %d workers)", wins, workers)
	}
}
