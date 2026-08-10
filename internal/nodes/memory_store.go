package nodes

import (
	"sync"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// MemoryBlockAttributionStore is an in-process BlockAttributionStore for tests.
type MemoryBlockAttributionStore struct {
	mu   sync.Mutex
	data map[string]store.BlockAttribution
}

func NewMemoryBlockAttributionStore() *MemoryBlockAttributionStore {
	return &MemoryBlockAttributionStore{data: make(map[string]store.BlockAttribution)}
}

func (s *MemoryBlockAttributionStore) TryAttribute(hash, prevHash, peerID string, height uint32) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.data[hash]; ok {
		return existing.PeerID, false, nil
	}
	s.data[hash] = store.BlockAttribution{
		Hash:     hash,
		PrevHash: prevHash,
		Height:   height,
		PeerID:   peerID,
	}
	return peerID, true, nil
}

func (s *MemoryBlockAttributionStore) ListAll() ([]store.BlockAttribution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.BlockAttribution, 0, len(s.data))
	for _, a := range s.data {
		out = append(out, a)
	}
	return out, nil
}

func (s *MemoryBlockAttributionStore) DeleteHashes(hashes []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hashes {
		delete(s.data, h)
	}
	return nil
}

// MemorySubtreeAttributionStore is an in-process SubtreeAttributionStore for tests.
type MemorySubtreeAttributionStore struct {
	mu   sync.Mutex
	data map[string]string // hash → peerID
}

func NewMemorySubtreeAttributionStore() *MemorySubtreeAttributionStore {
	return &MemorySubtreeAttributionStore{data: make(map[string]string)}
}

func (s *MemorySubtreeAttributionStore) TryAttribute(subtreeHash, peerID string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.data[subtreeHash]; ok {
		return existing, false, nil
	}
	s.data[subtreeHash] = peerID
	return peerID, true, nil
}
