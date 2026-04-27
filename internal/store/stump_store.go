package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// stumpKeyPrefix namespaces STUMP blobs so they cannot collide with subtree
// keys when the same BlobStore backs both.
const stumpKeyPrefix = "stump/"

// ErrStumpNotFound is returned when a STUMP reference resolves to no blob —
// typically because the blob was pruned by DAH. Callers should treat this as
// a permanent delivery failure (route to DLQ) rather than retrying.
var ErrStumpNotFound = errors.New("stump not found")

// blobStumpStore wraps a BlobStore to provide content-addressed STUMP storage
// with delete-at-height pruning. Content addressing gives automatic dedup
// across retries, re-orgs, and any other path that rebuilds an identical STUMP.
type blobStumpStore struct {
	store     BlobStore
	dahOffset uint64
	logger    *slog.Logger
}

var _ StumpStore = (*blobStumpStore)(nil)

func NewStumpStore(store BlobStore, dahOffset uint64, logger *slog.Logger) StumpStore {
	return &blobStumpStore{
		store:     store,
		dahOffset: dahOffset,
		logger:    logger,
	}
}

// Put stores the STUMP bytes and returns a content-addressed reference that
// Get can use later. If blockHeight > 0 the blob is scheduled for deletion at
// blockHeight + dahOffset; otherwise it is kept until explicitly deleted.
func (s *blobStumpStore) Put(data []byte, blockHeight uint64) (string, error) {
	ref := StumpRefFor(data)
	key := stumpKeyPrefix + ref

	var opts []BlobOption
	if blockHeight > 0 && s.dahOffset > 0 {
		opts = append(opts, WithDeleteAtHeight(blockHeight+s.dahOffset))
	}

	if err := s.store.Set(key, data, opts...); err != nil {
		return "", fmt.Errorf("failed to store stump %s: %w", ref, err)
	}
	return ref, nil
}

// Get retrieves STUMP bytes for the given reference. Returns ErrStumpNotFound
// (wrapped) if the blob is absent so callers can distinguish expired/pruned
// blobs from transient I/O failures.
func (s *blobStumpStore) Get(ref string) ([]byte, error) {
	if ref == "" {
		return nil, ErrStumpNotFound
	}
	data, err := s.store.Get(stumpKeyPrefix + ref)
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrStumpNotFound, ref)
		}
		return nil, fmt.Errorf("failed to get stump %s: %w", ref, err)
	}
	return data, nil
}

// Delete removes the STUMP for the given reference. Missing blobs are not an
// error — this method is idempotent so it is safe to call after successful
// delivery without coordinating across callback URLs that share the same ref.
func (s *blobStumpStore) Delete(ref string) error {
	if ref == "" {
		return nil
	}
	if err := s.store.Del(stumpKeyPrefix + ref); err != nil {
		return fmt.Errorf("failed to delete stump %s: %w", ref, err)
	}
	return nil
}

// StumpRefFor returns the content-addressed reference for the given STUMP
// bytes. Exposed so producers and consumers can agree on the ref format
// without needing a StumpStore instance.
func StumpRefFor(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// TrimStumpRefPrefix strips the internal key prefix if a caller accidentally
// passes a full blob key instead of a raw reference.
func TrimStumpRefPrefix(ref string) string {
	return strings.TrimPrefix(ref, stumpKeyPrefix)
}
