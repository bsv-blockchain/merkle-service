// Package nodes maintains the set of "mining nodes" used for SEEN_MULTIPLE_NODES
// confidence scoring. A node is a peer that first-announced at least one of the
// last W blocks on our lightweight tip path derived from P2P block announcements.
package nodes

import (
	"sort"
)

// BlockAttr is a first-seen attribution of a block hash to a peer.
type BlockAttr struct {
	Hash     string
	PrevHash string
	Height   uint32
	PeerID   string
}

// Chain is a pure in-memory model of attributed blocks, a tip chosen by
// maximum height, and a sliding window of the last W blocks on the tip path.
//
// Performance: tip path and weight map are recomputed only on mutation
// (Record/Load/Prune), never on Weight/Ready reads — those are O(1) map lookups
// against the cached snapshot. This matters because Weight is called on every
// subtree that reaches scoring at multi-million-tx scale.
type Chain struct {
	window int
	blocks map[string]BlockAttr // hash → attr
	tip    string

	// Cached after each mutation. windowPath is tip-first, capped at window.
	windowPath []string
	weights    map[string]int
	ready      bool
}

// NewChain creates an empty chain with the given window size W.
// W <= 0 defaults to 100.
func NewChain(window int) *Chain {
	if window <= 0 {
		window = 100
	}
	return &Chain{
		window:  window,
		blocks:  make(map[string]BlockAttr),
		weights: make(map[string]int),
	}
}

// Load replaces chain state from a full set of attributions (e.g. after
// loading from a shared store). Tip is recomputed as the max-height block.
func (c *Chain) Load(attrs []BlockAttr) {
	c.blocks = make(map[string]BlockAttr, len(attrs))
	for _, a := range attrs {
		if a.Hash == "" {
			continue
		}
		c.blocks[a.Hash] = a
	}
	c.recomputeTip()
	c.rebuildCaches()
}

// Record adds a first-seen block attribution. If hash is already known, the
// existing attribution is kept and FirstSeen is false. Otherwise the block is
// stored, the tip may move, and Orphaned lists hashes that left the tip path
// (informational; attributions are retained for side-branch reconnection).
func (c *Chain) Record(attr BlockAttr) (firstSeen bool, orphaned []string) {
	if attr.Hash == "" {
		return false, nil
	}
	if _, exists := c.blocks[attr.Hash]; exists {
		return false, nil
	}

	oldPath := c.windowPathSet()
	c.blocks[attr.Hash] = attr
	c.recomputeTip()
	c.rebuildCaches()
	newPath := c.windowPathSet()

	for h := range oldPath {
		if !newPath[h] {
			orphaned = append(orphaned, h)
		}
	}
	sort.Strings(orphaned)
	return true, orphaned
}

// Window returns up to W block attributions on the current tip path,
// tip-first order (index 0 is the tip).
func (c *Chain) Window() []BlockAttr {
	out := make([]BlockAttr, 0, len(c.windowPath))
	for _, h := range c.windowPath {
		out = append(out, c.blocks[h])
	}
	return out
}

// Weights returns peerID → number of blocks in the current window.
// The returned map must not be mutated by the caller (shared cache).
func (c *Chain) Weights() map[string]int {
	return c.weights
}

// Weight returns the window count for peerID (0 if unknown / not in window).
// O(1) after cache rebuild.
func (c *Chain) Weight(peerID string) int {
	if peerID == "" {
		return 0
	}
	return c.weights[peerID]
}

// Ready is true when the tip path has at least W known blocks. O(1).
func (c *Chain) Ready() bool {
	return c.ready
}

// TipHash returns the current tip block hash, or empty if none.
func (c *Chain) TipHash() string { return c.tip }

// WindowSize returns W.
func (c *Chain) WindowSize() int { return c.window }

// Len returns the number of attributed blocks retained.
func (c *Chain) Len() int { return len(c.blocks) }

// All returns a copy of every attributed block (for persistence snapshots).
func (c *Chain) All() []BlockAttr {
	out := make([]BlockAttr, 0, len(c.blocks))
	for _, a := range c.blocks {
		out = append(out, a)
	}
	return out
}

// PruneBelow removes attributions with height strictly less than
// tipHeight - retainDepth, keeping enough history for short reorgs.
// retainDepth defaults to 2*window when <= 0.
func (c *Chain) PruneBelow(retainDepth int) int {
	if c.tip == "" {
		return 0
	}
	if retainDepth <= 0 {
		retainDepth = 2 * c.window
	}
	tip := c.blocks[c.tip]
	if tip.Height < uint32(retainDepth) {
		return 0
	}
	minHeight := tip.Height - uint32(retainDepth)
	keep := c.windowPathSet()
	// Also keep full tip path beyond window for parent walks.
	for _, h := range c.tipPathUncapped() {
		keep[h] = true
	}
	removed := 0
	for h, a := range c.blocks {
		if keep[h] {
			continue
		}
		if a.Height < minHeight {
			delete(c.blocks, h)
			removed++
		}
	}
	if removed > 0 {
		c.rebuildCaches()
	}
	return removed
}

func (c *Chain) recomputeTip() {
	if len(c.blocks) == 0 {
		c.tip = ""
		return
	}
	var best BlockAttr
	found := false
	for _, a := range c.blocks {
		if !found || a.Height > best.Height || (a.Height == best.Height && a.Hash < best.Hash) {
			best = a
			found = true
		}
	}
	c.tip = best.Hash
}

// rebuildCaches refreshes windowPath, weights, ready. Call after any mutation.
func (c *Chain) rebuildCaches() {
	path := c.tipPathUncapped()
	if len(path) > c.window {
		path = path[:c.window]
	}
	// Own the slice so later walks don't share backing arrays unexpectedly.
	c.windowPath = append([]string(nil), path...)

	w := make(map[string]int, 8)
	for _, h := range c.windowPath {
		a := c.blocks[h]
		if a.PeerID == "" {
			continue
		}
		w[a.PeerID]++
	}
	c.weights = w
	c.ready = len(c.windowPath) >= c.window
}

// tipPathUncapped walks prevHash links from tip, returning hashes tip-first.
func (c *Chain) tipPathUncapped() []string {
	if c.tip == "" {
		return nil
	}
	max := c.window * 4
	if max < 256 {
		max = 256
	}
	seen := make(map[string]struct{}, max)
	path := make([]string, 0, c.window)
	h := c.tip
	for i := 0; i < max && h != ""; i++ {
		if _, ok := seen[h]; ok {
			break
		}
		a, ok := c.blocks[h]
		if !ok {
			break
		}
		seen[h] = struct{}{}
		path = append(path, h)
		h = a.PrevHash
	}
	return path
}

func (c *Chain) windowPathSet() map[string]bool {
	set := make(map[string]bool, len(c.windowPath))
	for _, h := range c.windowPath {
		set[h] = true
	}
	return set
}
