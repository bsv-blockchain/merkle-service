package main

import (
	"sync"
	"time"
)

// CallbackEntry represents a single received callback.
type CallbackEntry struct {
	Timestamp time.Time
	Body      map[string]interface{}
	RawJSON   string
}

// CallbackStore is a thread-safe ring buffer for received callbacks.
type CallbackStore struct {
	mu      sync.RWMutex
	entries []CallbackEntry
	maxSize int
}

// NewCallbackStore creates a new CallbackStore with the given capacity.
func NewCallbackStore(maxSize int) *CallbackStore {
	return &CallbackStore{
		entries: make([]CallbackEntry, 0, maxSize),
		maxSize: maxSize,
	}
}

// Add stores a new callback entry, evicting the oldest if at capacity.
func (s *CallbackStore) Add(entry CallbackEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.entries) >= s.maxSize {
		s.entries = s.entries[1:]
	}
	s.entries = append(s.entries, entry)
}

// GetAll returns all entries in reverse chronological order.
func (s *CallbackStore) GetAll() []CallbackEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]CallbackEntry, len(s.entries))
	for i, e := range s.entries {
		result[len(s.entries)-1-i] = e
	}
	return result
}

// Count returns the number of stored entries.
func (s *CallbackStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Clear removes all entries.
func (s *CallbackStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = s.entries[:0]
}

// TrackedTxid represents a txid registered through the dashboard.
type TrackedTxid struct {
	Txid         string
	RegisteredAt time.Time
	CallbackURLs []string
}

// TxidTracker tracks txids registered through the dashboard.
type TxidTracker struct {
	mu    sync.RWMutex
	txids []TrackedTxid
	index map[string]int // txid -> index in txids slice
}

// NewTxidTracker creates a new TxidTracker.
func NewTxidTracker() *TxidTracker {
	return &TxidTracker{
		index: make(map[string]int),
	}
}

// Add adds a txid to the tracker.
func (t *TxidTracker) Add(txid string, callbackURLs []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if idx, exists := t.index[txid]; exists {
		t.txids[idx].CallbackURLs = callbackURLs
		return
	}

	t.index[txid] = len(t.txids)
	t.txids = append(t.txids, TrackedTxid{
		Txid:         txid,
		RegisteredAt: time.Now(),
		CallbackURLs: callbackURLs,
	})
}

// UpdateCallbackURLs updates the callback URLs for a tracked txid.
func (t *TxidTracker) UpdateCallbackURLs(txid string, urls []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if idx, exists := t.index[txid]; exists {
		t.txids[idx].CallbackURLs = urls
	}
}

// GetAll returns all tracked txids.
func (t *TxidTracker) GetAll() []TrackedTxid {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]TrackedTxid, len(t.txids))
	copy(result, t.txids)
	return result
}

// Count returns the number of tracked txids.
func (t *TxidTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.txids)
}
