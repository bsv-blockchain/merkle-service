package store

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
)

func newAccumulatorTestStore(t *testing.T) CallbackAccumulatorStore {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	client, err := NewAerospikeClient("localhost", 3000, "merkle", 2, 50, logger)
	if err != nil {
		t.Skipf("Aerospike not available: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	setName := fmt.Sprintf("test_accum_%d", os.Getpid())
	return NewCallbackAccumulatorStore(client, setName, 60, 2, 50, logger)
}

func TestCallbackAccumulatorStore_AppendSingle(t *testing.T) {
	store := newAccumulatorTestStore(t)

	stumpData := []byte{0x01, 0x02, 0x03}
	err := store.Append("block1", "http://example.com/cb", []string{"txid1", "txid2"}, 0, stumpData)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	result, err := store.ReadAndDelete("block1")
	if err != nil {
		t.Fatalf("ReadAndDelete failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 URL entry, got %d", len(result))
	}

	acc := result["http://example.com/cb"]
	if acc == nil {
		t.Fatal("expected entry for callback URL")
	}
	if len(acc.Entries) != 1 {
		t.Fatalf("expected 1 entry (one subtree append), got %d", len(acc.Entries))
	}
	if len(acc.Entries[0].TxIDs) != 2 {
		t.Errorf("expected 2 txids in entry, got %d", len(acc.Entries[0].TxIDs))
	}
	if acc.Entries[0].TxIDs[0] != "txid1" {
		t.Errorf("expected first txid=txid1, got %s", acc.Entries[0].TxIDs[0])
	}
}

func TestCallbackAccumulatorStore_AppendMultipleSameURL(t *testing.T) {
	store := newAccumulatorTestStore(t)

	stumpA := []byte{0x01}
	stumpB := []byte{0x02}
	if err := store.Append("block2", "http://example.com/cb", []string{"txid1"}, 0, stumpA); err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}
	if err := store.Append("block2", "http://example.com/cb", []string{"txid2", "txid3"}, 1, stumpB); err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}

	result, err := store.ReadAndDelete("block2")
	if err != nil {
		t.Fatalf("ReadAndDelete failed: %v", err)
	}

	acc := result["http://example.com/cb"]
	if acc == nil {
		t.Fatal("expected entry for callback URL")
	}
	if len(acc.Entries) != 2 {
		t.Errorf("expected 2 entries (two subtree appends), got %d", len(acc.Entries))
	}
}

func TestCallbackAccumulatorStore_AppendDifferentURLs(t *testing.T) {
	store := newAccumulatorTestStore(t)

	stump := []byte{0x01}
	if err := store.Append("block3", "http://a.com/cb", []string{"txid1"}, 0, stump); err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}
	if err := store.Append("block3", "http://b.com/cb", []string{"txid2"}, 0, stump); err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}

	result, err := store.ReadAndDelete("block3")
	if err != nil {
		t.Fatalf("ReadAndDelete failed: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 URL entries, got %d", len(result))
	}
	if result["http://a.com/cb"] == nil || result["http://b.com/cb"] == nil {
		t.Error("missing expected callback URLs in result")
	}
}

func TestCallbackAccumulatorStore_ReadAndDeleteRemovesRecord(t *testing.T) {
	store := newAccumulatorTestStore(t)

	stump := []byte{0x01}
	if err := store.Append("block4", "http://example.com/cb", []string{"txid1"}, 0, stump); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// First read should return data.
	result, err := store.ReadAndDelete("block4")
	if err != nil {
		t.Fatalf("ReadAndDelete failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}

	// Second read should return nil (record deleted).
	result2, err := store.ReadAndDelete("block4")
	if err != nil {
		t.Fatalf("second ReadAndDelete failed: %v", err)
	}
	if result2 != nil && len(result2) != 0 {
		t.Errorf("expected empty result after delete, got %d entries", len(result2))
	}
}

func TestCallbackAccumulatorStore_ReadNonexistent(t *testing.T) {
	store := newAccumulatorTestStore(t)

	result, err := store.ReadAndDelete("nonexistent-block")
	if err != nil {
		t.Fatalf("ReadAndDelete failed: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for nonexistent block, got %v", result)
	}
}
