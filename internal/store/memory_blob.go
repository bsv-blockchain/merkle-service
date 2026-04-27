package store

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// MemoryBlobStore is an in-memory implementation of BlobStore for testing.
type MemoryBlobStore struct {
	mu     sync.RWMutex
	data   map[string][]byte
	dah    map[string]uint64 // delete-at-height per key
	height uint64
}

func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{
		data: make(map[string][]byte),
		dah:  make(map[string]uint64),
	}
}

func (m *MemoryBlobStore) Set(key string, data []byte, opts ...BlobOption) error {
	o := &blobOptions{}
	for _, opt := range opts {
		opt(o)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	if o.deleteAtHeight > 0 {
		m.dah[key] = o.deleteAtHeight
	}
	return nil
}

func (m *MemoryBlobStore) SetFromReader(key string, r io.Reader, size int64, opts ...BlobOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return m.Set(key, data, opts...)
}

func (m *MemoryBlobStore) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *MemoryBlobStore) GetIoReader(key string) (io.ReadCloser, error) {
	data, err := m.Get(key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemoryBlobStore) Del(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	delete(m.dah, key)
	return nil
}

func (m *MemoryBlobStore) SetCurrentBlockHeight(height uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.height = height
	// Prune entries whose DAH has been reached
	for key, dah := range m.dah {
		if height >= dah {
			delete(m.data, key)
			delete(m.dah, key)
		}
	}
}
