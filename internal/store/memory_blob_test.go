package store

import (
	"bytes"
	"testing"
)

func TestMemoryBlobStore_SetGet(t *testing.T) {
	s := NewMemoryBlobStore()
	err := s.Set("k1", []byte("v1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	v, err := s.Get("k1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("expected v1, got %s", v)
	}
}

func TestMemoryBlobStore_GetNotFound(t *testing.T) {
	s := NewMemoryBlobStore()
	_, err := s.Get("missing")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestMemoryBlobStore_Delete(t *testing.T) {
	s := NewMemoryBlobStore()
	err := s.Set("k1", []byte("v1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	err = s.Del("k1")
	if err != nil {
		t.Fatalf("del failed: %v", err)
	}

	_, err = s.Get("k1")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

// TestMemoryBlobStore_DelDoesNotCancelSchedule pins that the in-memory store
// honors the same schedule contract as FileBlobStore (see
// TestFileBlobStore_ScheduleIsNotCancelable): delete-at-height schedules are
// append-only and survive Del, so a Del + re-store does not rescue the blob
// from its scheduled prune.
func TestMemoryBlobStore_DelDoesNotCancelSchedule(t *testing.T) {
	s := NewMemoryBlobStore()
	if err := s.Set("k1", []byte("v1"), WithDeleteAtHeight(4)); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := s.Del("k1"); err != nil {
		t.Fatalf("del failed: %v", err)
	}
	// Re-store the same key without a DAH; the earlier schedule still fires.
	if err := s.Set("k1", []byte("v1")); err != nil {
		t.Fatalf("re-set failed: %v", err)
	}

	s.SetCurrentBlockHeight(4)

	if _, err := s.Get("k1"); err == nil {
		t.Fatal("schedule must fire at height 4 despite Del+re-store")
	}
}

func TestMemoryBlobStore_DAH(t *testing.T) {
	s := NewMemoryBlobStore()
	err := s.Set("k1", []byte("v1"), WithDeleteAtHeight(10))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	// Before height
	_, err = s.Get("k1")
	if err != nil {
		t.Fatalf("should exist before DAH: %v", err)
	}

	// At height
	s.SetCurrentBlockHeight(10)
	_, err = s.Get("k1")
	if err == nil {
		t.Fatal("should be pruned at DAH")
	}
}
