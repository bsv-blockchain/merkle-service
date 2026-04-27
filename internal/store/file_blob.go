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
// Supports "file://" for local filesystem storage.
// Falls back to in-memory store for unrecognized schemes.
func NewBlobStoreFromURL(rawURL string) (BlobStore, error) {
	if strings.HasPrefix(rawURL, "file://") {
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("parsing blob store URL: %w", err)
		}
		dir := u.Host + u.Path
		if dir == "" {
			dir = u.Path
		}
		return NewFileBlobStore(dir)
	}
	// Default to memory store for unknown/empty URLs.
	return NewMemoryBlobStore(), nil
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
