package store

import (
	"bytes"
	"path/filepath"
	"testing"
)

// TestFileBlobStore_SetCreatesParentDir guards the production fix where
// prefixed keys like "stump/<sha256>" must land on disk even when the
// subdirectory does not yet exist.
func TestFileBlobStore_SetCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	key := "stump/3292be80a8cd32bc53582b666a1f13564259281a256a6b40aae0bc83c4d50a4d"
	payload := []byte("stump-bytes")

	if err := bs.Set(key, payload); err != nil {
		t.Fatalf("Set with nested key: %v", err)
	}

	got, err := bs.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %q want %q", got, payload)
	}

	// The parent directory should exist on disk.
	if _, err := filepath.Abs(filepath.Join(dir, "stump")); err != nil {
		t.Errorf("expected stump/ subdirectory: %v", err)
	}
}
