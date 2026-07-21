package nodes

import (
	"fmt"
	"testing"
)

func TestChain_FirstSeenWins(t *testing.T) {
	c := NewChain(100)
	first, _ := c.Record(BlockAttr{Hash: "h1", PrevHash: "h0", Height: 1, PeerID: "alice"})
	if !first {
		t.Fatal("expected first seen")
	}
	second, _ := c.Record(BlockAttr{Hash: "h1", PrevHash: "h0", Height: 1, PeerID: "bob"})
	if second {
		t.Fatal("duplicate hash must not re-attribute")
	}
	if c.Weight("alice") != 1 {
		t.Fatalf("alice weight=%d", c.Weight("alice"))
	}
	if c.Weight("bob") != 0 {
		t.Fatalf("bob should not own h1, weight=%d", c.Weight("bob"))
	}
}

func TestChain_ExtendsTipAndWeights(t *testing.T) {
	c := NewChain(100)
	c.Record(BlockAttr{Hash: "h1", PrevHash: "genesis", Height: 1, PeerID: "a"})
	c.Record(BlockAttr{Hash: "h2", PrevHash: "h1", Height: 2, PeerID: "b"})
	c.Record(BlockAttr{Hash: "h3", PrevHash: "h2", Height: 3, PeerID: "a"})

	if c.TipHash() != "h3" {
		t.Fatalf("tip=%s want h3", c.TipHash())
	}
	w := c.Weights()
	if w["a"] != 2 || w["b"] != 1 {
		t.Fatalf("weights=%v", w)
	}
	if c.Ready() {
		t.Fatal("window of 3 should not be ready for W=100")
	}
}

func TestChain_ReadyWhenWindowFull(t *testing.T) {
	const W = 5
	c := NewChain(W)
	prev := "genesis"
	for i := 1; i <= W; i++ {
		h := fmt.Sprintf("h%d", i)
		c.Record(BlockAttr{Hash: h, PrevHash: prev, Height: uint32(i), PeerID: "miner"})
		prev = h
	}
	if !c.Ready() {
		t.Fatal("expected ready after W continuous blocks")
	}
	if c.Weight("miner") != W {
		t.Fatalf("weight=%d want %d", c.Weight("miner"), W)
	}
}

func TestChain_ReorgDropsOrphansFromWindow(t *testing.T) {
	// Build main chain: g -> a1 -> a2 -> a3
	c := NewChain(10)
	c.Record(BlockAttr{Hash: "a1", PrevHash: "g", Height: 1, PeerID: "alice"})
	c.Record(BlockAttr{Hash: "a2", PrevHash: "a1", Height: 2, PeerID: "alice"})
	c.Record(BlockAttr{Hash: "a3", PrevHash: "a2", Height: 3, PeerID: "alice"})

	// Side branch from a1: b2 (height 2) then b3, b4 (height 4 wins tip)
	c.Record(BlockAttr{Hash: "b2", PrevHash: "a1", Height: 2, PeerID: "bob"})
	c.Record(BlockAttr{Hash: "b3", PrevHash: "b2", Height: 3, PeerID: "bob"})
	first, orphaned := c.Record(BlockAttr{Hash: "b4", PrevHash: "b3", Height: 4, PeerID: "bob"})
	if !first {
		t.Fatal("b4 should be first seen")
	}
	if c.TipHash() != "b4" {
		t.Fatalf("tip=%s want b4", c.TipHash())
	}

	// a2 and a3 should be off the tip path window.
	orphanedSet := map[string]bool{}
	for _, h := range orphaned {
		orphanedSet[h] = true
	}
	if !orphanedSet["a2"] || !orphanedSet["a3"] {
		t.Fatalf("expected a2,a3 orphaned from tip path, got %v", orphaned)
	}

	w := c.Weights()
	// Window tip path: b4,b3,b2,a1 → bob=3, alice=1
	if w["bob"] != 3 {
		t.Fatalf("bob weight=%d want 3, weights=%v", w["bob"], w)
	}
	if w["alice"] != 1 {
		t.Fatalf("alice weight=%d want 1, weights=%v", w["alice"], w)
	}
}

func TestChain_GapStopsWindow(t *testing.T) {
	c := NewChain(5)
	// Missing parent of h2: walk stops, window len=1
	c.Record(BlockAttr{Hash: "h2", PrevHash: "missing", Height: 2, PeerID: "a"})
	if len(c.Window()) != 1 {
		t.Fatalf("window len=%d want 1", len(c.Window()))
	}
	if c.Ready() {
		t.Fatal("should not be ready with a gap")
	}
}

func TestChain_LoadRestoresTip(t *testing.T) {
	attrs := []BlockAttr{
		{Hash: "h1", PrevHash: "g", Height: 1, PeerID: "a"},
		{Hash: "h2", PrevHash: "h1", Height: 2, PeerID: "b"},
	}
	c := NewChain(100)
	c.Load(attrs)
	if c.TipHash() != "h2" {
		t.Fatalf("tip=%s", c.TipHash())
	}
	if c.Weight("b") != 1 || c.Weight("a") != 1 {
		t.Fatalf("weights=%v", c.Weights())
	}
}

func TestChain_PruneBelowKeepsTipPath(t *testing.T) {
	c := NewChain(3)
	prev := "g"
	for i := 1; i <= 10; i++ {
		h := fmt.Sprintf("h%d", i)
		c.Record(BlockAttr{Hash: h, PrevHash: prev, Height: uint32(i), PeerID: "m"})
		prev = h
	}
	// Side old orphan far below tip
	c.Record(BlockAttr{Hash: "old", PrevHash: "x", Height: 1, PeerID: "other"})
	removed := c.PruneBelow(4)
	if removed < 1 {
		t.Fatalf("expected prune of old block, removed=%d", removed)
	}
	if _, ok := c.blocks["old"]; ok {
		t.Fatal("old should be pruned")
	}
	// Tip path blocks within retain depth stay
	if c.TipHash() != "h10" {
		t.Fatalf("tip=%s", c.TipHash())
	}
	if len(c.Window()) != 3 {
		t.Fatalf("window=%d", len(c.Window()))
	}
}
