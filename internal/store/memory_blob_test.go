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
