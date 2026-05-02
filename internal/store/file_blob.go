package store

import (
	"bytes"
	"errors"
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
	dir     string
	rootAbs string // absolute, cleaned form of dir for traversal checks
	mu      sync.RWMutex
	dah     map[string]uint64 // delete-at-height per key
	height  uint64
}

// ErrBlobKeyEscapesRoot is returned when a blob key resolves to a filesystem
// path outside the configured root directory. F-038 (issue #24): an
// attacker-controlled subtree/STUMP key could otherwise read, write, or
// delete files anywhere the service has filesystem permission.
var ErrBlobKeyEscapesRoot = errors.New("blob key escapes root directory")

// NewFileBlobStore creates a new file-based blob store rooted at dir.
// The directory is created if it doesn't exist.
func NewFileBlobStore(dir string) (*FileBlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // 0755 is intentional: data dir needs group read
		return nil, fmt.Errorf("creating blob store directory %s: %w", dir, err)
	}
	rootAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolving blob store directory %s: %w", dir, err)
	}
	return &FileBlobStore{
		dir:     dir,
		rootAbs: rootAbs,
		dah:     make(map[string]uint64),
	}, nil
}

// resolvePath validates key and returns the absolute path it maps to inside
// f.dir. F-038: keys arrive from network-facing producers (subtree hashes,
// STUMP refs) so we must reject any shape that could escape f.dir before
// calling into the filesystem.
//
// The validation is layered:
//  1. Reject the obviously-malicious shapes at entry — empty, absolute,
//     leading slash, OS-specific separators (Windows '\\'), and any segment
//     equal to ".." — so the error message names the offending key without
//     leaking the resolved root.
//  2. Re-check after filepath.Clean+Join via filepath.Rel so we also catch
//     anything sneakier that happens to clean to a sibling directory (for
//     example via OS-specific path quirks or future refactors that change
//     how keys are composed).
func (f *FileBlobStore) resolvePath(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty key", ErrBlobKeyEscapesRoot)
	}
	if filepath.IsAbs(key) || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("%w: %q is absolute", ErrBlobKeyEscapesRoot, key)
	}
	// Reject Windows-style separators on every platform: blob keys are
	// produced by content addressing (sha256 hex) plus optional "<bucket>/"
	// prefixes, so a backslash is always anomalous and might bypass
	// filepath.Clean on non-Windows hosts.
	if strings.ContainsRune(key, '\\') {
		return "", fmt.Errorf("%w: %q contains backslash", ErrBlobKeyEscapesRoot, key)
	}
	// Walk segments and reject any "..". Splitting by "/" works for the
	// forward-slash-namespaced keys this store accepts; the IsAbs check
	// above already rejects anything starting at root.
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: %q contains parent segment", ErrBlobKeyEscapesRoot, key)
		}
	}

	cleaned := filepath.Clean(key)
	joined := filepath.Join(f.dir, cleaned)

	abs, err := filepath.Abs(joined)
	if err != nil {
		return "", fmt.Errorf("resolving blob path %q: %w", key, err)
	}
	rel, err := filepath.Rel(f.rootAbs, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: %q", ErrBlobKeyEscapesRoot, key)
	}
	return abs, nil
}

func (f *FileBlobStore) Set(key string, data []byte, opts ...BlobOption) error {
	o := &blobOptions{}
	for _, opt := range opts {
		opt(o)
	}

	path, err := f.resolvePath(key)
	if err != nil {
		return err
	}
	// Keys may contain path separators (e.g. "stump/<sha256>") to namespace
	// different blob categories. os.WriteFile does not create parents, so
	// ensure the containing directory exists before writing.
	if parent := filepath.Dir(path); parent != "" && parent != f.rootAbs {
		if err := os.MkdirAll(parent, 0o755); err != nil { //nolint:gosec // 0755 intentional for data dirs
			return fmt.Errorf("creating blob parent dir %s: %w", parent, err)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec // 0644 intentional for data blobs
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
	path, err := f.resolvePath(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is resolved through resolvePath which validates against rootAbs
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
	path, err := f.resolvePath(key)
	if err != nil {
		return err
	}

	f.mu.Lock()
	delete(f.dah, key)
	f.mu.Unlock()

	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting blob %s: %w", key, err)
	}
	return nil
}

func (f *FileBlobStore) SetCurrentBlockHeight(height uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.height = height

	// Prune entries whose DAH has been reached. Keys in f.dah were already
	// validated by Set, but re-validate defensively so a future bug that
	// writes to f.dah outside Set can't trigger a traversal during pruning.
	for key, dah := range f.dah {
		if height >= dah {
			if path, err := f.resolvePath(key); err == nil {
				_ = os.Remove(path)
			}
			delete(f.dah, key)
		}
	}
}
