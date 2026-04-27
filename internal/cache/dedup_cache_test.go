package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestDedupCache_AddAndContains(t *testing.T) {
	c := NewDedupCache(10)

	if c.Contains("a") {
		t.Error("empty cache should not contain 'a'")
	}

	if !c.Add("a") {
		t.Error("Add('a') should return true for new key")
	}

	if !c.Contains("a") {
		t.Error("cache should contain 'a' after Add")
	}
}

func TestDedupCache_DuplicateDetection(t *testing.T) {
	c := NewDedupCache(10)

	if !c.Add("x") {
		t.Error("first Add should return true")
	}
	if c.Add("x") {
		t.Error("second Add should return false (duplicate)")
	}
	if c.Add("x") {
		t.Error("third Add should return false (duplicate)")
	}
}

func TestDedupCache_EvictionAtCapacity(t *testing.T) {
	c := NewDedupCache(3)

	c.Add("a")
	c.Add("b")
	c.Add("c")

	if c.Len() != 3 {
		t.Fatalf("expected len 3, got %d", c.Len())
	}

	// Adding a 4th item should evict "a" (oldest)
	c.Add("d")

	if c.Len() != 3 {
		t.Fatalf("expected len 3 after eviction, got %d", c.Len())
	}
	if c.Contains("a") {
		t.Error("'a' should have been evicted")
	}
	if !c.Contains("b") {
		t.Error("'b' should still be present")
	}
	if !c.Contains("d") {
		t.Error("'d' should be present")
	}
}

func TestDedupCache_EvictionOrder(t *testing.T) {
	c := NewDedupCache(3)

	c.Add("a")
	c.Add("b")
	c.Add("c")

	// Re-add "a" (promotes it to most recent)
	c.Add("a") // returns false, but promotes

	// Now add "d" — should evict "b" (oldest non-promoted)
	c.Add("d")

	if c.Contains("b") {
		t.Error("'b' should have been evicted (oldest)")
	}
	if !c.Contains("a") {
		t.Error("'a' should still be present (was promoted)")
	}
	if !c.Contains("c") {
		t.Error("'c' should still be present")
	}
	if !c.Contains("d") {
		t.Error("'d' should be present")
	}
}

func TestDedupCache_ConcurrentAccess(t *testing.T) {
	c := NewDedupCache(1000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			c.Add(key)
			c.Contains(key)
			c.Add(key)
		}(i)
	}

	wg.Wait()

	if c.Len() != 100 {
		t.Errorf("expected 100 unique keys, got %d", c.Len())
	}
}

func TestDedupCache_ZeroMaxSize(t *testing.T) {
	c := NewDedupCache(0)
	c.Add("a")
	if c.Len() != 1 {
		t.Error("cache with maxSize 0 should default to 1")
	}
	// Adding second item evicts first
	c.Add("b")
	if c.Contains("a") {
		t.Error("'a' should be evicted")
	}
	if !c.Contains("b") {
		t.Error("'b' should be present")
	}
}
