//go:build integration

package store_test

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

func newSubtreeCounterStore(t *testing.T) store.SubtreeCounterStore {
	t.Helper()
	client := newAerospikeClient(t)
	return store.NewSubtreeCounterStore(client, "subtree_counter_test", 60, 3, 100, slog.Default())
}

func TestSubtreeCounterStore_InitAndDecrement(t *testing.T) {
	s := newSubtreeCounterStore(t)
	blockHash := fmt.Sprintf("block-init-dec-%d", time.Now().UnixNano())

	// Stash BlockProcessedData at Init: it must round-trip through the store and
	// surface only on the final (remaining == 0) decrement.
	want := &store.BlockProcessedData{
		MerkleRoot:    "aabbcc",
		SubtreeCount:  3,
		SubtreeHashes: []string{"deadbeef", "feedface"},
		CoinbaseBUMP:  "0102030405",
	}
	if err := s.Init(blockHash, 3, want); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	remaining, data, err := s.Decrement(blockHash)
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if remaining != 2 {
		t.Errorf("expected 2, got %d", remaining)
	}
	if data != nil {
		t.Errorf("expected nil block data on intermediate decrement, got %+v", data)
	}

	remaining, data, err = s.Decrement(blockHash)
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if remaining != 1 {
		t.Errorf("expected 1, got %d", remaining)
	}
	if data != nil {
		t.Errorf("expected nil block data on intermediate decrement, got %+v", data)
	}

	remaining, data, err = s.Decrement(blockHash)
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if remaining != 0 {
		t.Errorf("expected 0, got %d", remaining)
	}
	// The nil check short-circuits the field comparisons, so data is never
	// dereferenced when nil.
	if data == nil || data.MerkleRoot != want.MerkleRoot || data.SubtreeCount != want.SubtreeCount ||
		data.CoinbaseBUMP != want.CoinbaseBUMP || len(data.SubtreeHashes) != len(want.SubtreeHashes) {
		t.Fatalf("block data round-trip mismatch on final decrement: got %+v, want %+v", data, want)
	}
}

func TestSubtreeCounterStore_ConcurrentDecrements(t *testing.T) {
	s := newSubtreeCounterStore(t)
	blockHash := fmt.Sprintf("block-concurrent-%d", time.Now().UnixNano())
	count := 50

	if err := s.Init(blockHash, count, nil); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan int, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			remaining, _, err := s.Decrement(blockHash)
			if err != nil {
				t.Errorf("Decrement failed: %v", err)
				return
			}
			results <- remaining
		}()
	}
	wg.Wait()
	close(results)

	// Exactly one goroutine should have seen remaining == 0.
	zeroCount := 0
	for r := range results {
		if r == 0 {
			zeroCount++
		}
	}
	if zeroCount != 1 {
		t.Errorf("expected exactly 1 goroutine to see remaining=0, got %d", zeroCount)
	}
}
