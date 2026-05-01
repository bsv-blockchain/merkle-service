package store

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// NewBlobStoreFromURL creates a BlobStore from a URL string.
//
// Exactly two forms are accepted:
//
//   - "file://<path>" — durable on-disk store rooted at <path>. The path is
//     created if it does not exist. This is the only durable backend.
//   - "memory:" or "memory://" — in-memory store. Suitable for tests and
//     ephemeral single-process deployments only; blobs are lost on restart
//     and are not visible to other replicas.
//
// Any other scheme (including object-store URLs such as s3://, gs://, or
// azure://, and typos such as "flie://") returns an error so that operator
// misconfiguration fails loudly at startup instead of silently degrading to
// an in-memory store that loses subtree/STUMP blobs and breaks inter-replica
// sharing. An empty URL is also rejected for the same reason — callers who
// want an in-memory store must request it explicitly via "memory:".
func NewBlobStoreFromURL(rawURL string) (BlobStore, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil, fmt.Errorf(`blob-store URL is empty (expected "file://<path>" or "memory:")`)
	}

	// Accept the explicit in-memory form. Both "memory:" (opaque URI) and
	// "memory://" (authority form) are accepted for operator convenience.
	if trimmed == "memory:" || trimmed == "memory://" {
		return NewMemoryBlobStore(), nil
	}

	if strings.HasPrefix(trimmed, "file://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return nil, fmt.Errorf("parsing blob store URL: %w", err)
		}
		dir := u.Host + u.Path
		if dir == "" {
			dir = u.Path
		}
		return NewFileBlobStore(dir)
	}

	// Anything else — including s3://, gs://, azure://, http://, and typos
	// like "flie://" — is rejected. We surface the scheme but not the full
	// URL to avoid logging credentials embedded in the userinfo component.
	scheme := schemeOf(trimmed)
	return nil, fmt.Errorf(`unsupported blob-store scheme %q (expected "file" or "memory")`, scheme)
}

// schemeOf returns the URL scheme for rawURL, or the raw value (truncated)
// when the input does not look like a URL. Used purely for error messages,
// so it never returns userinfo or path components.
func schemeOf(rawURL string) string {
	if i := strings.Index(rawURL, ":"); i > 0 {
		return rawURL[:i]
	}
	// No scheme delimiter — the operator passed something that is not a URL
	// at all. Echo back a short prefix so the error is actionable without
	// risking secret leakage.
	const maxLen = 16
	if len(rawURL) > maxLen {
		return rawURL[:maxLen] + "..."
	}
	return rawURL
}

// FileBlobStore implements BlobStore using the local filesystem.
type FileBlobStore struct {
	dir    string
	mu     sync.RWMutex
	dah    map[string]uint64 // delete-at-height per key
	height uint64
}

// NewFileBlobStore creates a new file-based blob store rooted at dir.
// The directory is created if it doesn't exist.
func NewFileBlobStore(dir string) (*FileBlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating blob store directory %s: %w", dir, err)
	}
	return &FileBlobStore{
		dir: dir,
		dah: make(map[string]uint64),
	}, nil
}

func (f *FileBlobStore) path(key string) string {
	return filepath.Join(f.dir, key)
}

func (f *FileBlobStore) Set(key string, data []byte, opts ...BlobOption) error {
	o := &blobOptions{}
	for _, opt := range opts {
		opt(o)
	}

	path := f.path(key)
	// Keys may contain path separators (e.g. "stump/<sha256>") to namespace
	// different blob categories. os.WriteFile does not create parents, so
	// ensure the containing directory exists before writing.
	if parent := filepath.Dir(path); parent != "" && parent != f.dir {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("creating blob parent dir %s: %w", parent, err)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("writing blob %s: %w", key, err)
	}

	if o.deleteAtHeight > 0 {
		f.mu.Lock()
		f.dah[key] = o.deleteAtHeight
		f.mu.Unlock()
	}

	return nil
}

func (f *FileBlobStore) SetFromReader(key string, r io.Reader, size int64, opts ...BlobOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading blob data: %w", err)
	}
	return f.Set(key, data, opts...)
}

func (f *FileBlobStore) Get(key string) ([]byte, error) {
	data, err := os.ReadFile(f.path(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
		}
		return nil, fmt.Errorf("reading blob %s: %w", key, err)
	}
	return data, nil
}

func (f *FileBlobStore) GetIoReader(key string) (io.ReadCloser, error) {
	data, err := f.Get(key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *FileBlobStore) Del(key string) error {
	f.mu.Lock()
	delete(f.dah, key)
	f.mu.Unlock()

	err := os.Remove(f.path(key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting blob %s: %w", key, err)
	}
	return nil
}

func (f *FileBlobStore) SetCurrentBlockHeight(height uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.height = height

	// Prune entries whose DAH has been reached.
	for key, dah := range f.dah {
		if height >= dah {
			_ = os.Remove(f.path(key))
			delete(f.dah, key)
		}
	}
}
