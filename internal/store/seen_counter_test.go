package store

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func newSeenCounterTestStore(t *testing.T, threshold int) (SeenCounterStore, *AerospikeClient, string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	client, err := NewAerospikeClient("localhost", 3000, "merkle", 2, 50, logger)
	if err != nil {
		t.Skipf("Aerospike not available: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	setName := fmt.Sprintf("test_seen_%d", os.Getpid())
	return NewSeenCounterStore(client, setName, threshold, 2, 50, logger), client, setName
}

func TestSeenCounter_BasicIncrement(t *testing.T) {
	store, _, _ := newSeenCounterTestStore(t, 3)

	txid := fmt.Sprintf("tx-basic-%d", os.Getpid())
	for i := 1; i <= 5; i++ {
		res, err := store.Increment(txid, fmt.Sprintf("subtree-%d", i))
		if err != nil {
			t.Fatalf("Increment %d: %v", i, err)
		}
		if res.NewCount != i {
			t.Errorf("Increment %d: NewCount=%d, want %d", i, res.NewCount, i)
		}
	}
}

func TestSeenCounter_DuplicateSubtreeIsIdempotent(t *testing.T) {
	store, _, _ := newSeenCounterTestStore(t, 3)

	txid := fmt.Sprintf("tx-dup-%d", os.Getpid())
	for i := 0; i < 4; i++ {
		res, err := store.Increment(txid, "subtree-A")
		if err != nil {
			t.Fatalf("Increment %d: %v", i, err)
		}
		if res.NewCount != 1 {
			t.Errorf("Increment %d: NewCount=%d, want 1 (duplicate subtreeID)", i, res.NewCount)
		}
		if res.ThresholdReached {
			t.Errorf("Increment %d: threshold fired despite single distinct subtree", i)
		}
	}
}

func TestSeenCounter_ThresholdFiresExactlyOnce(t *testing.T) {
	store, _, _ := newSeenCounterTestStore(t, 3)

	txid := fmt.Sprintf("tx-thresh-%d", os.Getpid())
	fired := 0
	for i := 0; i < 8; i++ {
		res, err := store.Increment(txid, fmt.Sprintf("subtree-%d", i))
		if err != nil {
			t.Fatalf("Increment %d: %v", i, err)
		}
		if res.ThresholdReached {
			fired++
		}
	}
	if fired != 1 {
		t.Fatalf("ThresholdReached fired %d times across 8 distinct subtrees, want exactly 1", fired)
	}
}

// TestSeenCounter_ConcurrentThresholdFiresOnce is the F-045 regression test.
// Many goroutines call Increment for the same txid past the threshold and
// exactly one must observe ThresholdReached=true. The previous Get-then-Put
// sequence had a window where two concurrent observations could both see
// alreadyFired=false and both fire SEEN_MULTIPLE_NODES; the marker-write
// error was also silently dropped. The fix folds the read + check + set into
// a generation-checked CAS retry loop, so exactly one caller observes the
// false->true transition and any write failure is surfaced.
func TestSeenCounter_ConcurrentThresholdFiresOnce(t *testing.T) {
	const (
		threshold = 5
		workers   = 32
	)
	store, _, _ := newSeenCounterTestStore(t, threshold)

	txid := fmt.Sprintf("tx-race-%d", os.Getpid())

	var firedCount int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := store.Increment(txid, fmt.Sprintf("subtree-%d", i))
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

	got := atomic.LoadInt64(&firedCount)
	if got != 1 {
		t.Fatalf("F-045 regression: threshold fired %d times across %d concurrent workers, want exactly 1", got, workers)
	}
}

// TestSeenCounter_ConcurrentDistinctTxidsAllFire ensures the per-record CAS
// loop does not falsely serialize unrelated txids: each txid past its
// threshold should fire independently.
func TestSeenCounter_ConcurrentDistinctTxidsAllFire(t *testing.T) {
	const (
		threshold = 3
		txids     = 8
		perTxid   = 5
	)
	store, _, _ := newSeenCounterTestStore(t, threshold)

	pid := os.Getpid()
	firedPerTxid := make([]int64, txids)
	var wg sync.WaitGroup
	for i := 0; i < txids; i++ {
		for j := 0; j < perTxid; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				txid := fmt.Sprintf("tx-multi-%d-%d", pid, i)
				res, err := store.Increment(txid, fmt.Sprintf("subtree-%d", j))
				if err != nil {
					t.Errorf("Increment(%s,%d): %v", txid, j, err)
					return
				}
				if res.ThresholdReached {
					atomic.AddInt64(&firedPerTxid[i], 1)
				}
			}(i, j)
		}
	}
	wg.Wait()

	for i, n := range firedPerTxid {
		if n != 1 {
			t.Errorf("txid #%d fired %d times, want exactly 1", i, n)
		}
	}
}
