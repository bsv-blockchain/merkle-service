package cache

import (
	"strings"
	"testing"
)

// newTestImprovedCache creates a small ImprovedCache suitable for unit tests.
func newTestImprovedCache(t *testing.T) *ImprovedCache {
	t.Helper()

	c, err := New(BucketsCount*int(ChunkSize), Unallocated)
	if err != nil {
		t.Fatalf("failed to create test cache: %v", err)
	}

	return c
}

func TestSetMulti_LengthMismatchReturnsError(t *testing.T) {
	c := newTestImprovedCache(t)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("1"), []byte("2")}

	err := c.SetMulti(keys, values)
	if err == nil {
		t.Fatal("expected error for mismatched keys/values lengths, got nil")
	}

	if !strings.Contains(err.Error(), "SetMulti: keys/values length mismatch") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestSetMulti_EqualLengthsSucceeds(t *testing.T) {
	c := newTestImprovedCache(t)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}

	if err := c.SetMulti(keys, values); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

func TestSetMulti_EmptySlicesSucceeds(t *testing.T) {
	c := newTestImprovedCache(t)

	if err := c.SetMulti(nil, nil); err != nil {
		t.Fatalf("expected success for empty slices, got: %v", err)
	}
}

func TestSetMultiKeysSingleValue_ZeroKeySizeReturnsError(t *testing.T) {
	c := newTestImprovedCache(t)

	err := c.SetMultiKeysSingleValue([][]byte{[]byte("ab")}, []byte("v"), 0)
	if err == nil {
		t.Fatal("expected error for keySize == 0, got nil")
	}

	if !strings.Contains(err.Error(), "keySize must be > 0") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestSetMultiKeysSingleValue_NegativeKeySizeReturnsError(t *testing.T) {
	c := newTestImprovedCache(t)

	err := c.SetMultiKeysSingleValue([][]byte{[]byte("ab")}, []byte("v"), -1)
	if err == nil {
		t.Fatal("expected error for negative keySize, got nil")
	}

	if !strings.Contains(err.Error(), "keySize must be > 0") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestSetMultiKeysSingleValueAppended_ZeroKeySizeReturnsError(t *testing.T) {
	c := newTestImprovedCache(t)

	err := c.SetMultiKeysSingleValueAppended([]byte("aabb"), []byte("v"), 0)
	if err == nil {
		t.Fatal("expected error for keySize == 0, got nil")
	}

	if !strings.Contains(err.Error(), "keySize must be > 0") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestSetMultiKeysSingleValueAppended_NegativeKeySizeReturnsError(t *testing.T) {
	c := newTestImprovedCache(t)

	err := c.SetMultiKeysSingleValueAppended([]byte("aabb"), []byte("v"), -2)
	if err == nil {
		t.Fatal("expected error for negative keySize, got nil")
	}

	if !strings.Contains(err.Error(), "keySize must be > 0") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
