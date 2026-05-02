package store

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// ErrBlobNotFound is returned by BlobStore.Get / GetIoReader when the key is absent.
// Callers should use errors.Is to distinguish missing blobs from transient I/O errors
// so they can decide between retrying (transient) and giving up (permanent).
var ErrBlobNotFound = errors.New("blob not found")

// BlobStore defines the interface for subtree blob storage, mirroring Teranode's stores/blob.Store.
// When integrating with Teranode, this can be implemented by a wrapper around blob.Store.
type BlobStore interface {
	Set(key string, data []byte, opts ...BlobOption) error
	SetFromReader(key string, r io.Reader, size int64, opts ...BlobOption) error
	Get(key string) ([]byte, error)
	GetIoReader(key string) (io.ReadCloser, error)
	Del(key string) error
	SetCurrentBlockHeight(height uint64)
}

// BlobOption is a functional option for blob operations.
type BlobOption func(*blobOptions)

type blobOptions struct {
	deleteAtHeight uint64
}

// WithDeleteAtHeight sets the block height at which the blob should be deleted.
func WithDeleteAtHeight(height uint64) BlobOption {
	return func(o *blobOptions) {
		o.deleteAtHeight = height
	}
}

// blobSubtreeStore manages subtree storage and retrieval via a BlobStore backend.
type blobSubtreeStore struct {
	store         BlobStore
	dahOffset     uint64
	logger        *slog.Logger
	mu            sync.RWMutex
	currentHeight uint64
}

var _ SubtreeStore = (*blobSubtreeStore)(nil)

func NewSubtreeStore(store BlobStore, dahOffset uint64, logger *slog.Logger) SubtreeStore {
	return &blobSubtreeStore{
		store:     store,
		dahOffset: dahOffset,
		logger:    logger,
	}
}

// StoreSubtree stores subtree data with delete-at-height.
// If blockHeight is 0 (unknown, e.g. during realtime subtree processing),
// the blob is stored without a DAH and won't be auto-pruned until
// re-stored with a real block height during block processing.
func (s *blobSubtreeStore) StoreSubtree(id string, data []byte, blockHeight uint64) error {
	var opts []BlobOption
	if blockHeight > 0 {
		dah := blockHeight + s.dahOffset
		opts = append(opts, WithDeleteAtHeight(dah))
		s.logger.Info("stored subtree", "id", id, "dah", dah)
	} else {
		s.logger.Info("stored subtree", "id", id, "dah", "none")
	}
	err := s.store.Set(id, data, opts...)
	if err != nil {
		return fmt.Errorf("failed to store subtree %s: %w", id, err)
	}
	return nil
}

// StoreSubtreeFromReader stores subtree data from a reader with delete-at-height.
func (s *blobSubtreeStore) StoreSubtreeFromReader(id string, r io.Reader, size int64, blockHeight uint64) error {
	dah := blockHeight + s.dahOffset
	err := s.store.SetFromReader(id, r, size, WithDeleteAtHeight(dah))
	if err != nil {
		return fmt.Errorf("failed to store subtree %s from reader: %w", id, err)
	}
	s.logger.Debug("stored subtree from reader", "id", id, "dah", dah)
	return nil
}

// GetSubtree retrieves subtree data by ID.
func (s *blobSubtreeStore) GetSubtree(id string) ([]byte, error) {
	data, err := s.store.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get subtree %s: %w", id, err)
	}
	return data, nil
}

// GetSubtreeReader retrieves a reader for subtree data by ID.
func (s *blobSubtreeStore) GetSubtreeReader(id string) (io.ReadCloser, error) {
	r, err := s.store.GetIoReader(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get subtree reader %s: %w", id, err)
	}
	return r, nil
}

// DeleteSubtree removes a subtree from the store.
func (s *blobSubtreeStore) DeleteSubtree(id string) error {
	err := s.store.Del(id)
	if err != nil {
		return fmt.Errorf("failed to delete subtree %s: %w", id, err)
	}
	return nil
}

// SetCurrentBlockHeight updates the current block height for DAH pruning.
func (s *blobSubtreeStore) SetCurrentBlockHeight(height uint64) {
	s.mu.Lock()
	s.currentHeight = height
	s.mu.Unlock()
	s.store.SetCurrentBlockHeight(height)
}

// ConcurrentBlobStore wraps a BlobStore and deduplicates concurrent fetches for the same key.
type ConcurrentBlobStore struct {
	store    BlobStore
	inflight map[string]*call
	mu       sync.Mutex
}

type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func NewConcurrentBlobStore(store BlobStore) *ConcurrentBlobStore {
	return &ConcurrentBlobStore{
		store:    store,
		inflight: make(map[string]*call),
	}
}

// Get retrieves data, deduplicating concurrent requests for the same key.
func (c *ConcurrentBlobStore) Get(key string) ([]byte, error) {
	c.mu.Lock()
	if call, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}

	cl := &call{}
	cl.wg.Add(1)
	c.inflight[key] = cl
	c.mu.Unlock()

	cl.val, cl.err = c.store.Get(key)
	cl.wg.Done()

	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()

	return cl.val, cl.err
}

// Delegate other methods directly to the underlying store.
func (c *ConcurrentBlobStore) Set(key string, data []byte, opts ...BlobOption) error {
	return c.store.Set(key, data, opts...)
}

func (c *ConcurrentBlobStore) SetFromReader(key string, r io.Reader, size int64, opts ...BlobOption) error {
	return c.store.SetFromReader(key, r, size, opts...)
}

func (c *ConcurrentBlobStore) GetIoReader(key string) (io.ReadCloser, error) {
	return c.store.GetIoReader(key)
}

func (c *ConcurrentBlobStore) Del(key string) error {
	return c.store.Del(key)
}

func (c *ConcurrentBlobStore) SetCurrentBlockHeight(height uint64) {
	c.store.SetCurrentBlockHeight(height)
}
