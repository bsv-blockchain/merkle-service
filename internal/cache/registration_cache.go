package cache

import (
	"encoding/hex"
	"log/slog"
	"sync"
)

// RegistrationCache wraps ImprovedCache for deduplicating registration lookups.
// It caches whether a txid has been checked against Aerospike recently.
type RegistrationCache struct {
	cache  *ImprovedCache
	logger *slog.Logger
	mu     sync.RWMutex
}

// NewRegistrationCache creates a new registration cache.
// maxMB is the maximum memory in megabytes for the cache.
func NewRegistrationCache(maxMB int, logger *slog.Logger) (*RegistrationCache, error) {
	maxBytes := maxMB * 1024 * 1024
	cache, err := New(maxBytes, Unallocated)
	if err != nil {
		return nil, err
	}
	return &RegistrationCache{
		cache:  cache,
		logger: logger,
	}, nil
}

// cacheValue represents a cached registration lookup result.
// 0 = not registered, 1 = registered
type cacheValue byte

const (
	notRegistered cacheValue = 0
	registered    cacheValue = 1
)

// SetRegistered marks a txid as having been checked and found registered.
func (rc *RegistrationCache) SetRegistered(txid string) error {
	key, err := hex.DecodeString(txid)
	if err != nil {
		return err
	}
	return rc.cache.Set(key, []byte{byte(registered)})
}

// SetNotRegistered marks a txid as having been checked and found not registered.
func (rc *RegistrationCache) SetNotRegistered(txid string) error {
	key, err := hex.DecodeString(txid)
	if err != nil {
		return err
	}
	return rc.cache.Set(key, []byte{byte(notRegistered)})
}

// SetMultiRegistered marks multiple txids as registered in batch.
func (rc *RegistrationCache) SetMultiRegistered(txids []string) error {
	keys := make([][]byte, 0, len(txids))
	values := make([][]byte, 0, len(txids))
	for _, txid := range txids {
		key, err := hex.DecodeString(txid)
		if err != nil {
			continue
		}
		keys = append(keys, key)
		values = append(values, []byte{byte(registered)})
	}
	if len(keys) == 0 {
		return nil
	}
	return rc.cache.SetMulti(keys, values)
}

// SetMultiNotRegistered marks multiple txids as not registered in batch.
func (rc *RegistrationCache) SetMultiNotRegistered(txids []string) error {
	keys := make([][]byte, 0, len(txids))
	values := make([][]byte, 0, len(txids))
	for _, txid := range txids {
		key, err := hex.DecodeString(txid)
		if err != nil {
			continue
		}
		keys = append(keys, key)
		values = append(values, []byte{byte(notRegistered)})
	}
	if len(keys) == 0 {
		return nil
	}
	return rc.cache.SetMulti(keys, values)
}

// GetCached checks if a txid lookup result is cached.
// Returns (isRegistered, isCached).
// If isCached is false, the caller should query Aerospike.
func (rc *RegistrationCache) GetCached(txid string) (isRegistered bool, isCached bool) {
	key, err := hex.DecodeString(txid)
	if err != nil {
		return false, false
	}
	var dest []byte
	if err := rc.cache.Get(&dest, key); err != nil {
		return false, false
	}
	if len(dest) == 0 {
		return false, false
	}
	return dest[0] == byte(registered), true
}

// FilterUncached takes a list of txids and returns only those NOT in the cache.
// Also returns the cached results for those that ARE cached.
func (rc *RegistrationCache) FilterUncached(txids []string) (uncached []string, cachedRegistered []string) {
	for _, txid := range txids {
		isReg, isCached := rc.GetCached(txid)
		if !isCached {
			uncached = append(uncached, txid)
		} else if isReg {
			cachedRegistered = append(cachedRegistered, txid)
		}
	}
	return
}

// GetStats returns cache statistics.
func (rc *RegistrationCache) GetStats() *Stats {
	s := &Stats{}
	rc.cache.UpdateStats(s)
	return s
}
