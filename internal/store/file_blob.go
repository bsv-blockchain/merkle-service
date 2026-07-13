package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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

// dahDirName is the reserved directory (under the store root) holding the
// on-disk delete-at-height manifests. It is not valid blob-key space:
// resolvePath rejects keys that would land inside it.
//
// Layout: <root>/.dah/<height>/<owner>.list — one blob key per line. Each
// process appends to its own <owner>.list (so concurrent writers never share
// a file), and ANY process that learns a new block height prunes every
// manifest at or below that height. Keeping the bookkeeping on the volume —
// instead of in a per-process map — is what makes pruning survive restarts
// and work across the fetcher/worker/block-processor split, where the
// process that writes a blob is usually not the one that learns the height
// (this is how the shared subtree PVC previously filled to 100%: every
// process held a private map and none of them ever pruned another's blobs).
const dahDirName = ".dah"

// FileBlobStore implements BlobStore using the local filesystem.
type FileBlobStore struct {
	dir     string
	rootAbs string // absolute, cleaned form of dir for traversal checks
	owner   string // per-process manifest filename stem (hostname-pid)
	mu      sync.Mutex
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
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return &FileBlobStore{
		dir:     dir,
		rootAbs: rootAbs,
		owner:   fmt.Sprintf("%s-%d", hostname, os.Getpid()),
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
	// Reject control characters. DAH manifests are line-oriented (one key per
	// line), so a key containing "\n" would persist as TWO manifest lines —
	// forging a delete schedule for a different in-root blob. No legitimate
	// key (hex hashes, optional "<bucket>/" prefix) contains control
	// characters, so reject the whole class rather than just newlines.
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("%w: %q contains control character", ErrBlobKeyEscapesRoot, key)
		}
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
	// The DAH manifest area is bookkeeping, not blob space: a key that lands
	// inside it could forge delete schedules for arbitrary blobs. Check the
	// CLEANED key — raw shapes like "./.dah/x" or ".dah//x" only reveal their
	// destination after Clean (the key is already known to be relative with no
	// ".." segments, so Clean's output is exactly where Join will land).
	if cleaned == dahDirName || strings.HasPrefix(cleaned, dahDirName+"/") {
		return "", fmt.Errorf("%w: %q is reserved bookkeeping space", ErrBlobKeyEscapesRoot, key)
	}
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
		if err := f.ScheduleDelete(key, o.deleteAtHeight); err != nil {
			return fmt.Errorf("recording delete-at-height for blob %s: %w", key, err)
		}
	}

	return nil
}

// ScheduleDelete durably records that key's blob should be deleted once the
// chain reaches height, without touching the blob bytes. The record is an
// append to this process's manifest file under <root>/.dah/<height>/ — see
// dahDirName for the layout and why this lives on disk. Schedules are
// append-only and not cancellable (manifests may belong to other processes);
// firing on an already-deleted or re-stored key is harmless because blobs
// are content-addressed.
func (f *FileBlobStore) ScheduleDelete(key string, height uint64) error {
	// Callers pass keys that already went through resolvePath, but validate
	// again so no future call path can smuggle an unvalidated key into a
	// manifest.
	if _, err := f.resolvePath(key); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	manifestDir := filepath.Join(f.rootAbs, dahDirName, strconv.FormatUint(height, 10))
	manifest := filepath.Join(manifestDir, f.owner+".list")

	// A concurrent pruner can remove the height directory between MkdirAll
	// and OpenFile; one retry closes that window (the second MkdirAll runs
	// after the removal has finished).
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := os.MkdirAll(manifestDir, 0o755); err != nil { //nolint:gosec // 0755 intentional for data dirs
			lastErr = err
			continue
		}
		fh, err := os.OpenFile(manifest, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // path is store-internal bookkeeping
		if err != nil {
			lastErr = err
			continue
		}
		// Leading newline is torn-write armor: appends are not fsynced, so a
		// crash can leave a partial line with no terminator. A bare "key\n"
		// append would then fuse with that fragment into one corrupt key,
		// silently unscheduling BOTH blobs. Starting with "\n" seals any torn
		// fragment onto its own line so only the crashed append's key is lost
		// (its blob falls through to the age sweeper); the resulting blank
		// lines are skipped by the prune scan.
		_, werr := fh.WriteString("\n" + key + "\n")
		cerr := fh.Close()
		if werr != nil {
			lastErr = werr
			continue
		}
		if cerr != nil {
			lastErr = cerr
			continue
		}
		return nil
	}
	return lastErr
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

	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting blob %s: %w", key, err)
	}
	return nil
}

// SetCurrentBlockHeight prunes every blob scheduled for deletion at or below
// height, reading the schedules from the on-disk manifests (see dahDirName).
// It is safe to call from any process sharing the volume and safe to call
// concurrently: blob removal ignores already-missing files, and a manifest
// directory that received a concurrent append is left in place for the next
// call. Manifest keys are re-validated against the store root before any
// filesystem delete, so a forged manifest cannot reach outside the store
// (same F-038 posture as the old in-memory map path).
func (f *FileBlobStore) SetCurrentBlockHeight(height uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	dahRoot := filepath.Join(f.rootAbs, dahDirName)
	dirs, err := os.ReadDir(dahRoot)
	if err != nil {
		return // no schedules recorded (or unreadable — nothing safe to do)
	}

	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		h, err := strconv.ParseUint(d.Name(), 10, 64)
		if err != nil || h > height {
			continue
		}

		heightDir := filepath.Join(dahRoot, d.Name())
		manifests, err := os.ReadDir(heightDir)
		if err != nil {
			continue
		}
		for _, m := range manifests {
			if m.IsDir() || !strings.HasSuffix(m.Name(), ".list") {
				continue
			}
			manifestPath := filepath.Join(heightDir, m.Name())
			data, err := os.ReadFile(manifestPath) //nolint:gosec // path is store-internal bookkeeping
			if err != nil {
				continue
			}
			// Only act on newline-terminated entries. A manifest that does not
			// end in "\n" was torn mid-append (every complete append ends with
			// "\n"), so the trailing fragment is a truncated key — acting on it
			// could remove a blob that was never scheduled. Dropping it loses
			// only the crashed append's schedule; that blob falls through to
			// the age sweeper, same as the torn-write posture in ScheduleDelete.
			entries := string(data)
			if !strings.HasSuffix(entries, "\n") {
				if i := strings.LastIndexByte(entries, '\n'); i >= 0 {
					entries = entries[:i+1]
				} else {
					entries = ""
				}
			}
			for _, key := range strings.Split(entries, "\n") {
				if key == "" {
					continue
				}
				if path, err := f.resolvePath(key); err == nil {
					_ = os.Remove(path)
				}
			}
			_ = os.Remove(manifestPath)
		}
		// Succeeds only when empty; a concurrent append keeps the directory
		// alive and the next prune pass picks it up.
		_ = os.Remove(heightDir)
	}
}
