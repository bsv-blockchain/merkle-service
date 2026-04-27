package stump

import (
	"encoding/binary"
	"sort"
)

// STUMP represents a Subtree Unified Merkle Path in BRC-0074 BUMP format.
type STUMP struct {
	BlockHeight uint64
	TreeHeight  uint8
	Paths       []PathLevel
}

// PathLevel represents one level of the merkle tree path.
type PathLevel struct {
	Leaves []Leaf
}

// Leaf represents a node at a given level in the merkle path.
type Leaf struct {
	Offset    uint64
	Hash      []byte // 32 bytes
	TxID      bool   // true if this is a registered txid (flag=0x02)
	Duplicate bool   // true if hash should be duplicated from the working hash (flag=0x01)
}

// Build constructs a STUMP for registered txids within a subtree merkle tree.
//
// leaves contains the leaf hashes (txid hashes) in their original order.
// internalNodes contains the internal merkle tree nodes as returned by
// go-subtree's BuildMerkleTreeStoreFromBytes (level-by-level, NOT including leaves).
// registeredIndices maps leaf index to the txid at that position.
//
// The resulting STUMP follows BRC-0074 BUMP format where the subtree root
// replaces the block merkle root.
func Build(blockHeight uint64, leaves [][]byte, internalNodes [][]byte, registeredIndices map[int]string) *STUMP {
	if len(registeredIndices) == 0 || len(leaves) == 0 {
		return nil
	}

	nLeaves := len(leaves)
	nextPoT := nextPowerOfTwo(nLeaves)

	// Calculate tree height (number of levels from leaves to root, exclusive of root).
	treeHeight := uint8(0)
	for n := nextPoT; n > 1; n >>= 1 {
		treeHeight++
	}

	// Single leaf: root equals the leaf, no path needed.
	if treeHeight == 0 {
		return &STUMP{
			BlockHeight: blockHeight,
			TreeHeight:  0,
		}
	}

	// getNode returns the hash at the given level and index.
	// Level 0 = leaves, level 1+ = internalNodes.
	getNode := func(level uint8, index int) []byte {
		if level == 0 {
			if index < nLeaves {
				return leaves[index]
			}
			return nil // padded position
		}
		// Internal nodes layout: level k starts at offset sum(nextPoT/2^i, i=1..k-1).
		off := 0
		sz := nextPoT >> 1
		for k := uint8(1); k < level; k++ {
			off += sz
			sz >>= 1
		}
		pos := off + index
		if pos < len(internalNodes) {
			return internalNodes[pos]
		}
		return nil
	}

	stump := &STUMP{
		BlockHeight: blockHeight,
		TreeHeight:  treeHeight,
		Paths:       make([]PathLevel, treeHeight),
	}

	// Track which indices we need at each level.
	needed := make(map[int]bool)
	for idx := range registeredIndices {
		needed[idx] = true
	}

	// Track the count of real (non-padding) nodes at each level.
	realCount := nLeaves

	for level := uint8(0); level < treeHeight; level++ {
		addedOffsets := make(map[uint64]bool)
		var pathLeaves []Leaf
		nextNeeded := make(map[int]bool)

		for idx := range needed {
			// At level 0, include the registered txid itself.
			if level == 0 {
				if _, isReg := registeredIndices[idx]; isReg && !addedOffsets[uint64(idx)] {
					pathLeaves = append(pathLeaves, Leaf{
						Offset: uint64(idx),
						Hash:   leaves[idx],
						TxID:   true,
					})
					addedOffsets[uint64(idx)] = true
				}
			}

			// Add sibling hash needed for proof.
			siblingIdx := idx ^ 1
			if !addedOffsets[uint64(siblingIdx)] {
				leaf := Leaf{Offset: uint64(siblingIdx)}

				if siblingIdx >= realCount {
					// Sibling is beyond real data — verifier duplicates the working hash.
					leaf.Duplicate = true
				} else {
					h := getNode(level, siblingIdx)
					if h != nil {
						hashCopy := make([]byte, len(h))
						copy(hashCopy, h)
						leaf.Hash = hashCopy
					} else {
						leaf.Duplicate = true
					}
					// Check if sibling is also a registered txid at level 0.
					if level == 0 {
						if _, isReg := registeredIndices[siblingIdx]; isReg {
							leaf.TxID = true
						}
					}
				}

				pathLeaves = append(pathLeaves, leaf)
				addedOffsets[uint64(siblingIdx)] = true
			}

			nextNeeded[idx/2] = true
		}

		// Sort leaves by offset for deterministic output.
		sort.Slice(pathLeaves, func(i, j int) bool {
			return pathLeaves[i].Offset < pathLeaves[j].Offset
		})

		stump.Paths[level] = PathLevel{Leaves: pathLeaves}
		needed = nextNeeded
		realCount = (realCount + 1) / 2
	}

	return stump
}

// Encode serializes a STUMP to BRC-0074 BUMP binary format.
func (s *STUMP) Encode() []byte {
	if s == nil {
		return nil
	}

	buf := make([]byte, 0, 256)

	// Block height (CompactSize VarInt).
	buf = appendCompactSize(buf, s.BlockHeight)

	// Tree height (single byte).
	buf = append(buf, s.TreeHeight)

	// Path levels.
	for _, level := range s.Paths {
		// Number of leaves at this level (CompactSize VarInt).
		buf = appendCompactSize(buf, uint64(len(level.Leaves)))

		for _, leaf := range level.Leaves {
			// Offset (CompactSize VarInt).
			buf = appendCompactSize(buf, leaf.Offset)

			// Flags byte: 0x00 = hash follows (not txid), 0x01 = duplicate, 0x02 = hash follows (is txid).
			var flags byte
			if leaf.Duplicate {
				flags = 0x01
			} else if leaf.TxID {
				flags = 0x02
			}
			buf = append(buf, flags)

			// Hash (32 bytes, only if not duplicate).
			if !leaf.Duplicate {
				buf = append(buf, leaf.Hash...)
			}
		}
	}

	return buf
}

// MergeSTUMPs merges multiple STUMPs for the same subtree into one.
func MergeSTUMPs(stumps []*STUMP) *STUMP {
	if len(stumps) == 0 {
		return nil
	}
	if len(stumps) == 1 {
		return stumps[0]
	}

	merged := &STUMP{
		BlockHeight: stumps[0].BlockHeight,
		TreeHeight:  stumps[0].TreeHeight,
		Paths:       make([]PathLevel, stumps[0].TreeHeight),
	}

	for level := uint8(0); level < merged.TreeHeight; level++ {
		seen := make(map[uint64]Leaf)
		for _, s := range stumps {
			if int(level) < len(s.Paths) {
				for _, leaf := range s.Paths[level].Leaves {
					if existing, exists := seen[leaf.Offset]; !exists {
						seen[leaf.Offset] = leaf
					} else if leaf.TxID && !existing.TxID {
						// Prefer the version with TxID flag set.
						seen[leaf.Offset] = leaf
					}
				}
			}
		}

		leaves := make([]Leaf, 0, len(seen))
		for _, leaf := range seen {
			leaves = append(leaves, leaf)
		}
		sort.Slice(leaves, func(i, j int) bool {
			return leaves[i].Offset < leaves[j].Offset
		})
		merged.Paths[level] = PathLevel{Leaves: leaves}
	}

	return merged
}

// GroupByCallback groups registered txids by their callback URL.
// Returns map[callbackURL][]txid.
func GroupByCallback(registrations map[string][]string) map[string][]string {
	result := make(map[string][]string)
	for txid, callbacks := range registrations {
		for _, cb := range callbacks {
			result[cb] = append(result[cb], txid)
		}
	}
	return result
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// appendCompactSize appends a Bitcoin CompactSize unsigned integer to buf.
func appendCompactSize(buf []byte, v uint64) []byte {
	switch {
	case v < 253:
		return append(buf, byte(v))
	case v <= 0xFFFF:
		b := [3]byte{0xFD}
		binary.LittleEndian.PutUint16(b[1:], uint16(v))
		return append(buf, b[:]...)
	case v <= 0xFFFFFFFF:
		b := [5]byte{0xFE}
		binary.LittleEndian.PutUint32(b[1:], uint32(v))
		return append(buf, b[:]...)
	default:
		b := [9]byte{0xFF}
		binary.LittleEndian.PutUint64(b[1:], v)
		return append(buf, b[:]...)
	}
}
