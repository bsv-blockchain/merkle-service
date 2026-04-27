package cache

import (
	"container/list"
	"sync"
)

// DedupCache is a thread-safe bounded LRU set for tracking processed message hashes.
// It supports Add (returns false if already present) and Contains operations,
// with oldest-entry eviction when the cache reaches capacity.
type DedupCache struct {
	mu       sync.Mutex
	maxSize  int
	items    map[string]*list.Element
	eviction *list.List
}

// NewDedupCache creates a new DedupCache with the given maximum size.
func NewDedupCache(maxSize int) *DedupCache {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &DedupCache{
		maxSize:  maxSize,
		items:    make(map[string]*list.Element, maxSize),
		eviction: list.New(),
	}
}

// Add inserts a key into the cache. Returns false if the key was already present
// (and promotes it to most-recently-used). Returns true if the key was newly added.
func (c *DedupCache) Add(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.eviction.MoveToFront(elem)
		return false
	}

	// Evict oldest if at capacity.
	for c.eviction.Len() >= c.maxSize {
		back := c.eviction.Back()
		if back == nil {
			break
		}
		evictedKey := back.Value.(string)
		c.eviction.Remove(back)
		delete(c.items, evictedKey)
	}

	elem := c.eviction.PushFront(key)
	c.items[key] = elem
	return true
}

// Contains checks if a key exists in the cache without modifying LRU order.
func (c *DedupCache) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[key]
	return ok
}

// Len returns the number of entries in the cache.
func (c *DedupCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
