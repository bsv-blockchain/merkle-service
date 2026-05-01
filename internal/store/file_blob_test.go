package store

import (
	"bytes"
	"path/filepath"
	"strings"
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

// TestNewBlobStoreFromURL pins the factory's scheme handling. F-040 was a
// silent fallback to memory for any non-"file://" URL — a typo or an
// object-store URL that the operator believed was supported would lose
// subtree/STUMP blobs on restart with no operator-visible signal. The cases
// below cover every documented branch and the error path that previously
// produced the silent fallback.
func TestNewBlobStoreFromURL(t *testing.T) {
	t.Run("file scheme returns file-backed store", func(t *testing.T) {
		dir := t.TempDir()
		bs, err := NewBlobStoreFromURL("file://" + dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if bs == nil {
			t.Fatal("nil store")
		}
		if _, ok := bs.(*FileBlobStore); !ok {
			t.Errorf("expected *FileBlobStore, got %T", bs)
		}
	})

	t.Run("memory: scheme returns in-memory store", func(t *testing.T) {
		bs, err := NewBlobStoreFromURL("memory:")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := bs.(*MemoryBlobStore); !ok {
			t.Errorf("expected *MemoryBlobStore, got %T", bs)
		}
	})

	t.Run("memory:// authority form also accepted", func(t *testing.T) {
		bs, err := NewBlobStoreFromURL("memory://")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := bs.(*MemoryBlobStore); !ok {
			t.Errorf("expected *MemoryBlobStore, got %T", bs)
		}
	})

	t.Run("s3 scheme rejected with helpful error", func(t *testing.T) {
		bs, err := NewBlobStoreFromURL("s3://bucket/key")
		if err == nil {
			t.Fatalf("expected error, got store %T", bs)
		}
		if bs != nil {
			t.Errorf("expected nil store on error, got %T", bs)
		}
		msg := err.Error()
		if !strings.Contains(msg, `"s3"`) {
			t.Errorf("error %q should name the rejected scheme", msg)
		}
		if !strings.Contains(msg, "unsupported") {
			t.Errorf("error %q should mention it is unsupported", msg)
		}
		// Guard against credential leakage in the diagnostic.
		if strings.Contains(msg, "bucket") || strings.Contains(msg, "key") {
			t.Errorf("error %q should not echo the URL path/host", msg)
		}
	})

	t.Run("gs and azure schemes also rejected", func(t *testing.T) {
		for _, raw := range []string{"gs://b/k", "azure://acct/c", "http://blob:8080"} {
			if _, err := NewBlobStoreFromURL(raw); err == nil {
				t.Errorf("expected error for %q, got nil", raw)
			}
		}
	})

	t.Run("typo does not silently fall back", func(t *testing.T) {
		// Previously this would have returned a memory store with no error.
		bs, err := NewBlobStoreFromURL("flie:///tmp/foo")
		if err == nil {
			t.Fatalf("expected error for typo, got store %T", bs)
		}
		if !strings.Contains(err.Error(), `"flie"`) {
			t.Errorf("error %q should name the typo'd scheme", err.Error())
		}
	})

	t.Run("empty URL is rejected (tightened semantics)", func(t *testing.T) {
		// The factory used to return a memory store for an empty URL. We now
		// require operators to opt in explicitly via "memory:" so a missing
		// config field can't silently produce a non-durable store.
		if _, err := NewBlobStoreFromURL(""); err == nil {
			t.Error("expected error for empty URL, got nil")
		}
		if _, err := NewBlobStoreFromURL("   "); err == nil {
			t.Error("expected error for whitespace-only URL, got nil")
		}
	})

	t.Run("non-URL garbage is rejected without echoing full input", func(t *testing.T) {
		secret := "supersecretpasswordvaluethatshouldnotleak"
		_, err := NewBlobStoreFromURL(secret)
		if err == nil {
			t.Fatal("expected error for non-URL input")
		}
		if strings.Contains(err.Error(), "shouldnotleak") {
			t.Errorf("error %q leaked the trailing portion of the input", err.Error())
		}
	})
}
