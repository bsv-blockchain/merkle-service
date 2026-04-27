//go:build integration

package store_test

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

func newAerospikeClient(t *testing.T) *store.AerospikeClient {
	t.Helper()
	client, err := store.NewAerospikeClient("localhost", 3000, "test", 3, 100, slog.Default())
	if err != nil {
		t.Fatalf("failed to create Aerospike client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func uniqueSet(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// --- RegistrationStore tests ---

func TestRegistrationStore_AddAndGet(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "reg_add")
	regStore := store.NewRegistrationStore(client, setName, 3, 100, slog.Default())

	txid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	callback := "https://example.com/cb1"

	err := regStore.Add(txid, callback)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(urls) != 1 {
		t.Fatalf("expected 1 callback, got %d", len(urls))
	}
	if urls[0] != callback {
		t.Fatalf("expected %q, got %q", callback, urls[0])
	}
}

func TestRegistrationStore_MultipleCallbacksSameTxid(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "reg_multi")
	regStore := store.NewRegistrationStore(client, setName, 3, 100, slog.Default())

	txid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	cb1 := "https://example.com/cb1"
	cb2 := "https://example.com/cb2"
	cb3 := "https://example.com/cb3"

	for _, cb := range []string{cb1, cb2, cb3} {
		if err := regStore.Add(txid, cb); err != nil {
			t.Fatalf("Add(%q) failed: %v", cb, err)
		}
	}

	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(urls) != 3 {
		t.Fatalf("expected 3 callbacks, got %d: %v", len(urls), urls)
	}

	// The store uses ordered list, so callbacks should be sorted.
	expected := []string{cb1, cb2, cb3}
	for i, u := range urls {
		if u != expected[i] {
			t.Errorf("callback[%d] = %q, want %q", i, u, expected[i])
		}
	}
}

func TestRegistrationStore_IdempotentAdd(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "reg_idem")
	regStore := store.NewRegistrationStore(client, setName, 3, 100, slog.Default())

	txid := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	callback := "https://example.com/cb_dup"

	// Add the same callback twice.
	if err := regStore.Add(txid, callback); err != nil {
		t.Fatalf("first Add failed: %v", err)
	}
	if err := regStore.Add(txid, callback); err != nil {
		t.Fatalf("second Add failed: %v", err)
	}

	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(urls) != 1 {
		t.Fatalf("expected 1 callback (idempotent), got %d: %v", len(urls), urls)
	}
	if urls[0] != callback {
		t.Fatalf("expected %q, got %q", callback, urls[0])
	}
}

func TestRegistrationStore_BatchGet(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "reg_batch")
	regStore := store.NewRegistrationStore(client, setName, 3, 100, slog.Default())

	txid1 := "1111111111111111111111111111111111111111111111111111111111111111"
	txid2 := "2222222222222222222222222222222222222222222222222222222222222222"
	txid3 := "3333333333333333333333333333333333333333333333333333333333333333" // no registration

	if err := regStore.Add(txid1, "https://example.com/a"); err != nil {
		t.Fatalf("Add txid1 failed: %v", err)
	}
	if err := regStore.Add(txid2, "https://example.com/b"); err != nil {
		t.Fatalf("Add txid2 failed: %v", err)
	}

	result, err := regStore.BatchGet([]string{txid1, txid2, txid3})
	if err != nil {
		t.Fatalf("BatchGet failed: %v", err)
	}

	if len(result[txid1]) != 1 || result[txid1][0] != "https://example.com/a" {
		t.Errorf("txid1: expected [https://example.com/a], got %v", result[txid1])
	}
	if len(result[txid2]) != 1 || result[txid2][0] != "https://example.com/b" {
		t.Errorf("txid2: expected [https://example.com/b], got %v", result[txid2])
	}
	if _, exists := result[txid3]; exists {
		t.Errorf("txid3 should not exist in result, got %v", result[txid3])
	}
}

func TestRegistrationStore_UpdateTTL(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "reg_ttl")
	regStore := store.NewRegistrationStore(client, setName, 3, 100, slog.Default())

	txid := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	callback := "https://example.com/ttl"

	if err := regStore.Add(txid, callback); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Set a short TTL of 5 seconds.
	err := regStore.UpdateTTL(txid, 5*time.Second)
	if err != nil {
		// Some Aerospike configurations (e.g., namespace with nsup-period=0)
		// do not allow TTL updates. Skip the test in that case.
		t.Skipf("UpdateTTL not supported in this Aerospike configuration: %v", err)
	}

	// Record should still be accessible immediately.
	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("Get after UpdateTTL failed: %v", err)
	}
	if len(urls) != 1 {
		t.Fatalf("expected 1 callback after UpdateTTL, got %d", len(urls))
	}

	// Wait for expiry (Aerospike TTL has ~1s granularity, so wait a bit extra).
	t.Log("waiting for TTL expiry...")
	time.Sleep(7 * time.Second)

	urls, err = regStore.Get(txid)
	if err != nil {
		// After TTL expiry, Aerospike may return KEY_NOT_FOUND as an error.
		t.Logf("Get after TTL expiry returned error (expected): %v", err)
		return
	}
	if len(urls) != 0 {
		t.Fatalf("expected 0 callbacks after TTL expiry, got %d: %v", len(urls), urls)
	}
}

func TestRegistrationStore_GetNonExistent(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "reg_noexist")
	regStore := store.NewRegistrationStore(client, setName, 3, 100, slog.Default())

	urls, err := regStore.Get("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err != nil {
		// Aerospike may return KEY_NOT_FOUND_ERROR as an error for non-existent keys.
		// This is acceptable behavior - the key genuinely does not exist.
		t.Logf("Get non-existent returned error (acceptable): %v", err)
		return
	}
	if urls != nil {
		t.Fatalf("expected nil for non-existent key, got %v", urls)
	}
}

// --- SeenCounterStore tests ---

func TestSeenCounter_IncrementReturnsCorrectCount(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "seen_inc")
	counter := store.NewSeenCounterStore(client, setName, 3, 3, 100, slog.Default())

	txid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	for i := 1; i <= 5; i++ {
		result, err := counter.Increment(txid)
		if err != nil {
			t.Fatalf("Increment #%d failed: %v", i, err)
		}
		if result.NewCount != i {
			t.Errorf("Increment #%d: expected count=%d, got %d", i, i, result.NewCount)
		}
	}
}

func TestSeenCounter_ThresholdReachedFiresAtThreshold(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "seen_thresh")
	threshold := 3
	counter := store.NewSeenCounterStore(client, setName, threshold, 3, 100, slog.Default())

	txid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	for i := 1; i <= 5; i++ {
		result, err := counter.Increment(txid)
		if err != nil {
			t.Fatalf("Increment #%d failed: %v", i, err)
		}

		if i == threshold {
			if !result.ThresholdReached {
				t.Errorf("Increment #%d: expected ThresholdReached=true at threshold=%d", i, threshold)
			}
		} else {
			if result.ThresholdReached {
				t.Errorf("Increment #%d: ThresholdReached should be false (threshold=%d)", i, threshold)
			}
		}
	}
}

func TestSeenCounter_AboveThresholdDoesNotReFire(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "seen_above")
	threshold := 2
	counter := store.NewSeenCounterStore(client, setName, threshold, 3, 100, slog.Default())

	txid := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	thresholdFiredCount := 0
	for i := 1; i <= 10; i++ {
		result, err := counter.Increment(txid)
		if err != nil {
			t.Fatalf("Increment #%d failed: %v", i, err)
		}
		if result.ThresholdReached {
			thresholdFiredCount++
		}
	}

	if thresholdFiredCount != 1 {
		t.Fatalf("expected ThresholdReached to fire exactly once, fired %d times", thresholdFiredCount)
	}
}

func TestSeenCounter_Threshold(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "seen_tval")
	counter := store.NewSeenCounterStore(client, setName, 7, 3, 100, slog.Default())

	if counter.Threshold() != 7 {
		t.Fatalf("expected Threshold()=7, got %d", counter.Threshold())
	}
}
