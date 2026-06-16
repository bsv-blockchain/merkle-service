//go:build integration

package store_test

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// Before/after throughput benchmarks for the P1 batching work
// (docs/kafka-throughput-review.md F-4 and F-8) against a REAL Aerospike.
//
// "Serial" sub-benchmarks drive the legacy call pattern (one store call per
// txid — what the pipeline did before); "batch" drives the new bulk APIs.
// Both APIs coexist on this branch, so the comparison runs on identical code.
//
// Run:
//
//	AEROSPIKE_HOST=localhost AEROSPIKE_PORT=3100 \
//	  go test -tags integration -run XXX -bench 'SeenCounter|UpdateTTL' \
//	  -benchtime 3x ./internal/store/
func benchAerospikeClient(b *testing.B) *store.AerospikeClient {
	b.Helper()
	host := os.Getenv("AEROSPIKE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 3000
	if p := os.Getenv("AEROSPIKE_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}
	ns := os.Getenv("AEROSPIKE_NAMESPACE")
	if ns == "" {
		ns = "merkle"
	}
	client, err := store.NewAerospikeClient(host, port, ns, 3, 100, slog.Default())
	if err != nil {
		b.Skipf("Aerospike not available on %s:%d: %v", host, port, err)
	}
	b.Cleanup(func() { client.Close() })
	return client
}

func benchTxids(prefix string, n int) []string {
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("%s-%064d", prefix, i)
	}
	return txids
}

// BenchmarkSeenCounter measures the SEEN-path counter update: the legacy
// 2-RTT-per-txid Increment loop vs the new BatchIncrement. High threshold so
// no callbacks fire — this is the steady-state path. Each iteration uses
// fresh txids so record state never carries between iterations.
func BenchmarkSeenCounter(b *testing.B) {
	client := benchAerospikeClient(b)
	setName := fmt.Sprintf("bench_seen_%d", time.Now().UnixNano())
	sc := store.NewSeenCounterStore(client, setName, 1_000_000, 3, 100, slog.Default())

	for _, size := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("serial/txids=%d", size), func(b *testing.B) {
			if size > 1000 {
				b.Skip("serial at >1000 txids takes ~10s/op; size 100/1000 establish the per-txid cost")
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				txids := benchTxids(fmt.Sprintf("ser%d-%d", size, i), size)
				for _, txid := range txids {
					if _, err := sc.Increment(txid, "subtree-bench"); err != nil {
						b.Fatalf("Increment: %v", err)
					}
				}
			}
			b.ReportMetric(float64(size)/b.Elapsed().Seconds()*float64(b.N), "txids/s")
		})
		b.Run(fmt.Sprintf("batch/txids=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				txids := benchTxids(fmt.Sprintf("bat%d-%d", size, i), size)
				results, err := sc.BatchIncrement(txids, "subtree-bench")
				if err != nil {
					b.Fatalf("BatchIncrement: %v", err)
				}
				if len(results) != size {
					b.Fatalf("got %d results, want %d", len(results), size)
				}
			}
			b.ReportMetric(float64(size)/b.Elapsed().Seconds()*float64(b.N), "txids/s")
		})
	}
}

// BenchmarkUpdateTTL measures the post-mine TTL refresh: the legacy
// one-Operate-per-txid UpdateTTL loop vs the new batched BatchUpdateTTL.
// Records are registered once in setup; TTL refresh is idempotent so
// iterations reuse them.
func BenchmarkUpdateTTL(b *testing.B) {
	client := benchAerospikeClient(b)
	setName := fmt.Sprintf("bench_ttl_%d", time.Now().UnixNano())
	regStore := store.NewRegistrationStore(client, setName, 3, 100, 0, slog.Default())

	const size = 1000
	txids := benchTxids("ttl", size)
	for _, txid := range txids {
		if err := regStore.Add(txid, "https://bench.example/cb", ""); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}

	b.Run(fmt.Sprintf("serial/txids=%d", size), func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, txid := range txids {
				if err := regStore.UpdateTTL(txid, 30*time.Minute); err != nil {
					b.Fatalf("UpdateTTL: %v", err)
				}
			}
		}
		b.ReportMetric(float64(size)/b.Elapsed().Seconds()*float64(b.N), "txids/s")
	})
	b.Run(fmt.Sprintf("batch/txids=%d", size), func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := regStore.BatchUpdateTTL(txids, 30*time.Minute); err != nil {
				b.Fatalf("BatchUpdateTTL: %v", err)
			}
		}
		b.ReportMetric(float64(size)/b.Elapsed().Seconds()*float64(b.N), "txids/s")
	})
}

// TestBatchGet_BeyondServerBatchLimit is the F-5 capability proof rather than
// a speed benchmark: a single whole-subtree BatchGet above the server's
// batch-max-requests (default 5000) failed deterministically before the
// chunking fix; 12,345 keys must now succeed in one call.
func TestBatchGet_BeyondServerBatchLimit(t *testing.T) {
	host := os.Getenv("AEROSPIKE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 3000
	if p := os.Getenv("AEROSPIKE_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}
	ns := os.Getenv("AEROSPIKE_NAMESPACE")
	if ns == "" {
		ns = "merkle"
	}
	client, err := store.NewAerospikeClient(host, port, ns, 3, 100, slog.Default())
	if err != nil {
		t.Skipf("Aerospike not available: %v", err)
	}
	defer client.Close()

	setName := fmt.Sprintf("bench_f5_%d", time.Now().UnixNano())
	regStore := store.NewRegistrationStore(client, setName, 3, 100, 0, slog.Default())

	const total = 12345 // > 2x the 5000-key server default
	txids := benchTxids("f5", total)
	// Register a sparse subset; BatchGet must return exactly those.
	for i := 0; i < total; i += 1000 {
		if err := regStore.Add(txids[i], "https://bench.example/cb", ""); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	got, err := regStore.BatchGet(txids)
	if err != nil {
		t.Fatalf("BatchGet over %d keys failed (pre-fix this exceeded batch-max-requests): %v", total, err)
	}
	want := (total + 999) / 1000
	if len(got) != want {
		t.Fatalf("BatchGet returned %d registered txids, want %d", len(got), want)
	}
}

// TestBatchIncrement_ConcurrentThresholdFiresOnce is the F-045 guarantee
// under the new batched path: several workers BatchIncrement the SAME txids
// for DIFFERENT subtrees concurrently, and for every txid exactly ONE caller
// across all workers and batches must observe ThresholdReached=true. This is
// the question a reviewer should ask of the two-phase design: phase 1's bulk
// append is not atomic with the threshold check, so the exactly-once property
// rests entirely on phase 2's generation-CAS — prove it.
func TestBatchIncrement_ConcurrentThresholdFiresOnce(t *testing.T) {
	host := os.Getenv("AEROSPIKE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 3000
	if p := os.Getenv("AEROSPIKE_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}
	ns := os.Getenv("AEROSPIKE_NAMESPACE")
	if ns == "" {
		ns = "merkle"
	}
	client, err := store.NewAerospikeClient(host, port, ns, 3, 100, slog.Default())
	if err != nil {
		t.Skipf("Aerospike not available: %v", err)
	}
	defer client.Close()

	const (
		workers   = 8 // concurrent subtree observations
		txidCount = 200
		threshold = 3
	)
	setName := fmt.Sprintf("bench_f045_%d", time.Now().UnixNano())
	sc := store.NewSeenCounterStore(client, setName, threshold, 3, 100, slog.Default())
	txids := benchTxids("f045", txidCount)

	type fireCount struct {
		mu    sync.Mutex
		fires map[string]int
	}
	fc := &fireCount{fires: make(map[string]int)}

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			// Every worker reports the same txids from a distinct subtree, all
			// racing through phase 1 appends and phase 2 CAS at once.
			results, err := sc.BatchIncrement(txids, fmt.Sprintf("subtree-%d", worker))
			if err != nil {
				errCh <- err
				return
			}
			fc.mu.Lock()
			defer fc.mu.Unlock()
			for txid, res := range results {
				if res.ThresholdReached {
					fc.fires[txid]++
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("BatchIncrement: %v", err)
	}

	// workers(8) >= threshold(3): every txid must have crossed, and exactly once.
	for _, txid := range txids {
		if got := fc.fires[txid]; got != 1 {
			t.Errorf("txid %s: ThresholdReached observed %d times across concurrent batches, want exactly 1 (F-045)", txid, got)
		}
	}
}

// TestBatchIncrement_CountIsListSizeAndIdempotent pins the read-back-collapse
// change: BatchIncrement now derives NewCount from the list-append op's returned
// size instead of a second BatchGet. This isolates that semantics — a distinct
// subtree bumps the unique-subtree count by one, and a REPEAT of an
// already-recorded subtree (AddUnique|NoFail) leaves it unchanged. Threshold is
// set high so phase 2 never runs and only the phase-1 count path is exercised.
func TestBatchIncrement_CountIsListSizeAndIdempotent(t *testing.T) {
	host := os.Getenv("AEROSPIKE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 3000
	if p := os.Getenv("AEROSPIKE_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}
	ns := os.Getenv("AEROSPIKE_NAMESPACE")
	if ns == "" {
		ns = "merkle"
	}
	client, err := store.NewAerospikeClient(host, port, ns, 3, 100, slog.Default())
	if err != nil {
		t.Skipf("Aerospike not available: %v", err)
	}
	defer client.Close()

	const threshold = 100 // high: keep phase 2 (threshold firing) out of the picture
	setName := fmt.Sprintf("bench_count_%d", time.Now().UnixNano())
	sc := store.NewSeenCounterStore(client, setName, threshold, 3, 100, slog.Default())
	txids := benchTxids("count", 50)

	assertCounts := func(stage string, results map[string]*store.IncrementResult, want int) {
		t.Helper()
		if len(results) != len(txids) {
			t.Fatalf("%s: got %d results, want %d", stage, len(results), len(txids))
		}
		for _, txid := range txids {
			res, ok := results[txid]
			if !ok {
				t.Fatalf("%s: missing result for %s", stage, txid)
			}
			if res.NewCount != want {
				t.Errorf("%s: txid %s NewCount=%d, want %d", stage, txid, res.NewCount, want)
			}
			if res.ThresholdReached {
				t.Errorf("%s: txid %s fired below threshold", stage, txid)
			}
		}
	}

	// First distinct subtree -> count 1.
	r1, err := sc.BatchIncrement(txids, "subtree-A")
	if err != nil {
		t.Fatalf("BatchIncrement A: %v", err)
	}
	assertCounts("subtree-A", r1, 1)

	// Second distinct subtree -> count 2.
	r2, err := sc.BatchIncrement(txids, "subtree-B")
	if err != nil {
		t.Fatalf("BatchIncrement B: %v", err)
	}
	assertCounts("subtree-B", r2, 2)

	// Repeat of subtree-A (AddUnique|NoFail) -> count stays 2, proving the size
	// returned by the append reflects the deduplicated list, not a blind +1.
	r3, err := sc.BatchIncrement(txids, "subtree-A")
	if err != nil {
		t.Fatalf("BatchIncrement A repeat: %v", err)
	}
	assertCounts("subtree-A repeat", r3, 2)
}
