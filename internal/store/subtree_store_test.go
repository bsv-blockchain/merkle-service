package store

import (
	"bytes"
	"io"
	"log/slog"
	"sync"
	"testing"
)

func TestSubtreeStore_StoreAndGet(t *testing.T) {
	blob := NewMemoryBlobStore()
	ss := NewSubtreeStore(blob, 1, slog.Default())

	data := []byte("test subtree data")
	err := ss.StoreSubtree("subtree-1", data, 100)
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}

	got, err := ss.GetSubtree("subtree-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %s, want %s", got, data)
	}
}

func TestSubtreeStore_DAHExpiry(t *testing.T) {
	blob := NewMemoryBlobStore()
	ss := NewSubtreeStore(blob, 1, slog.Default())

	// Store at block height 100, DAH = 101
	err := ss.StoreSubtree("subtree-1", []byte("data"), 100)
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}

	// Should exist before DAH
	_, err = ss.GetSubtree("subtree-1")
	if err != nil {
		t.Fatalf("should exist before DAH: %v", err)
	}

	// Advance past DAH
	ss.SetCurrentBlockHeight(101)

	// Should be pruned
	_, err = ss.GetSubtree("subtree-1")
	if err == nil {
		t.Fatal("should be pruned after DAH")
	}
}

func TestSubtreeStore_StreamingIO(t *testing.T) {
	blob := NewMemoryBlobStore()
	ss := NewSubtreeStore(blob, 1, slog.Default())

	data := []byte("large subtree data for streaming")
	r := bytes.NewReader(data)
	err := ss.StoreSubtreeFromReader("subtree-stream", r, int64(len(data)), 100)
	if err != nil {
		t.Fatalf("store from reader failed: %v", err)
	}

	reader, err := ss.GetSubtreeReader("subtree-stream")
	if err != nil {
		t.Fatalf("get reader failed: %v", err)
	}
	defer reader.Close()

	got, _ := io.ReadAll(reader)
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch")
	}
}

func TestSubtreeStore_Delete(t *testing.T) {
	blob := NewMemoryBlobStore()
	ss := NewSubtreeStore(blob, 1, slog.Default())

	err := ss.StoreSubtree("subtree-del", []byte("data"), 100)
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}

	err = ss.DeleteSubtree("subtree-del")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = ss.GetSubtree("subtree-del")
	if err == nil {
		t.Fatal("should not exist after delete")
	}
}

func TestConcurrentBlobStore_DeduplicatesGets(t *testing.T) {
	blob := NewMemoryBlobStore()
	err := blob.Set("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	concurrent := NewConcurrentBlobStore(blob)

	var wg sync.WaitGroup
	results := make([][]byte, 10)
	errors := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errors[idx] = concurrent.Get("key1")
		}(i)
	}

	wg.Wait()

	for i := 0; i < 10; i++ {
		if errors[i] != nil {
			t.Fatalf("concurrent get %d failed: %v", i, errors[i])
		}
		if !bytes.Equal(results[i], []byte("value1")) {
			t.Fatalf("concurrent get %d: data mismatch", i)
		}
	}
}
