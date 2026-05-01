package cache

import (
	"encoding/hex"
	"log/slog"
	"sync"
)

// RegistrationCache wraps ImprovedCache for deduplicating registration lookups.
//
// Only positive results ("this txid is registered") are cached. Negative
// results ("not registered") are intentionally NOT cached because the
// registration store is mutated out-of-band by the API server's /watch
// handler, which has no way to invalidate this in-process cache — and in
// the microservices deployment topology (api-server, subtree-fetcher and
// block-processor as separate processes) cannot reach the cache at all.
//
// Caching negatives previously caused F-020: a txid observed by the
// subtree-fetcher before it was registered would be cached as
// "not registered" with no TTL and remain hidden until the cache
// wrapped around, suppressing callbacks for any later /watch.
//
// The performance loss vs caching negatives is bounded: every uncached
// txid still flows through a single batched BatchGet against the
// backing store. The hot positive-hit path (cachedRegistered in
// FilterUncached) is unchanged.
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
// Only `registered` is ever stored; see the package-level comment on
// RegistrationCache for why negatives are not cached.
type cacheValue byte

const (
	registered cacheValue = 1
)

// SetRegistered marks a txid as having been checked and found registered.
func (rc *RegistrationCache) SetRegistered(txid string) error {
	key, err := hex.DecodeString(txid)
	if err != nil {
		return err
	}
	return rc.cache.Set(key, []byte{byte(registered)})
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

// Invalidate removes any cached entry for txid. Safe to call for txids
// that were never cached. Provided so an in-process /watch handler can
// drop a stale entry as defence in depth — note that the canonical fix
// for F-020 is "do not cache negatives", which this cache enforces by
// construction (there is no SetNotRegistered).
func (rc *RegistrationCache) Invalidate(txid string) error {
	key, err := hex.DecodeString(txid)
	if err != nil {
		return err
	}
	rc.cache.Del(key)
	return nil
}

// GetCached checks if a txid lookup result is cached.
// Returns (isRegistered, isCached).
//
// Because negatives are never cached, isCached==true always implies
// isRegistered==true. The two-value signature is preserved so callers
// can reason about cache hits explicitly. If isCached is false, the
// caller must query the backing store.
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
//
// Because negatives are never cached, every cached entry is positive,
// and cachedRegistered will contain every txid that hits the cache.
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
