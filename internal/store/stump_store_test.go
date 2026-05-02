package store

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func newTestStumpStore(t *testing.T, dahOffset uint64) (StumpStore, *MemoryBlobStore) {
	t.Helper()
	blob := NewMemoryBlobStore()
	return NewStumpStore(blob, dahOffset, slog.New(slog.NewTextHandler(io.Discard, nil))), blob
}

func TestStumpStore_PutGetRoundTrip(t *testing.T) {
	ss, _ := newTestStumpStore(t, 0)

	data := []byte{0x01, 0x02, 0xDE, 0xAD, 0xBE, 0xEF}
	ref, err := ss.Put(data, 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ref == "" {
		t.Fatal("expected non-empty ref")
	}

	got, err := ss.Get(ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("round-trip mismatch: got %x want %x", got, data)
	}
}

func TestStumpStore_ContentAddressed(t *testing.T) {
	ss, _ := newTestStumpStore(t, 0)

	data := []byte("the-same-stump-bytes")
	ref1, err := ss.Put(data, 100)
	if err != nil {
		t.Fatalf("Put #1: %v", err)
	}
	ref2, err := ss.Put(data, 200)
	if err != nil {
		t.Fatalf("Put #2: %v", err)
	}
	if ref1 != ref2 {
		t.Errorf("identical bytes should yield identical refs: %s != %s", ref1, ref2)
	}

	// Putting different bytes yields a different ref.
	ref3, err := ss.Put([]byte("different-bytes"), 0)
	if err != nil {
		t.Fatalf("Put #3: %v", err)
	}
	if ref3 == ref1 {
		t.Errorf("distinct bytes yielded identical refs: %s", ref3)
	}
}

func TestStumpStore_GetMissingReturnsSentinel(t *testing.T) {
	ss, _ := newTestStumpStore(t, 0)

	_, err := ss.Get("nonexistent-ref")
	if err == nil {
		t.Fatal("expected error for missing ref")
	}
	if !errors.Is(err, ErrStumpNotFound) {
		t.Errorf("expected ErrStumpNotFound, got %v", err)
	}

	// Empty ref should also be treated as not found — never silently resolve
	// to an arbitrary blob.
	if _, err := ss.Get(""); !errors.Is(err, ErrStumpNotFound) {
		t.Errorf("expected ErrStumpNotFound for empty ref, got %v", err)
	}
}

func TestStumpStore_DeleteAtHeight(t *testing.T) {
	ss, blob := newTestStumpStore(t, 3)

	ref, err := ss.Put([]byte("pruneable"), 100)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Before reaching DAH, the blob is readable.
	if _, err = ss.Get(ref); err != nil {
		t.Fatalf("unexpected error before DAH: %v", err)
	}

	// Advance height past DAH (100 + 3). The memory blob store prunes on
	// SetCurrentBlockHeight; after that Get should report not found.
	blob.SetCurrentBlockHeight(103)
	_, err = ss.Get(ref)
	if !errors.Is(err, ErrStumpNotFound) {
		t.Errorf("expected ErrStumpNotFound after DAH, got %v", err)
	}
}

func TestStumpStore_DeleteIsIdempotent(t *testing.T) {
	ss, _ := newTestStumpStore(t, 0)

	ref, err := ss.Put([]byte("to-delete"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ss.Delete(ref); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	// Second delete of an already-missing blob should not error — callers may
	// share a ref across callback URLs and race on cleanup.
	if err := ss.Delete(ref); err != nil {
		t.Errorf("second Delete should be a no-op, got %v", err)
	}
	// Deleting an empty ref is a no-op (defensive).
	if err := ss.Delete(""); err != nil {
		t.Errorf("Delete(\"\") should be a no-op, got %v", err)
	}
}

func TestStumpRefFor_Deterministic(t *testing.T) {
	a := StumpRefFor([]byte("abc"))
	b := StumpRefFor([]byte("abc"))
	c := StumpRefFor([]byte("abd"))
	if a != b {
		t.Errorf("same input produced different refs: %s vs %s", a, b)
	}
	if a == c {
		t.Errorf("different input produced the same ref: %s", a)
	}
	if len(a) != 64 {
		t.Errorf("expected 64-char hex sha256 ref, got %d chars: %s", len(a), a)
	}
}
