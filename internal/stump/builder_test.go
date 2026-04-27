package stump

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"
)

// doubleSHA256 computes SHA256(SHA256(left || right)), the standard Bitcoin merkle hash.
func doubleSHA256(left, right []byte) []byte {
	var combined [64]byte
	copy(combined[:32], left)
	copy(combined[32:], right)
	first := sha256.Sum256(combined[:])
	second := sha256.Sum256(first[:])
	return second[:]
}

// buildTestTree creates N leaf hashes and computes the internal merkle nodes
// matching go-subtree's BuildMerkleTreeStoreFromBytes layout.
// Returns (leaves, internalNodes, root).
func buildTestTree(t *testing.T, n int) ([][]byte, [][]byte, []byte) {
	t.Helper()

	leaves := make([][]byte, n)
	for i := 0; i < n; i++ {
		h := sha256.Sum256([]byte{byte(i), byte(i >> 8)})
		leaves[i] = h[:]
	}

	nextPoT := nextPowerOfTwo(n)
	if nextPoT <= 1 {
		return leaves, nil, leaves[0]
	}

	// Pad leaves to nextPoT with zero hashes.
	padded := make([][]byte, nextPoT)
	copy(padded, leaves)
	for i := n; i < nextPoT; i++ {
		padded[i] = make([]byte, 32)
	}

	// Build internal nodes matching go-subtree layout.
	// Level 1: pairs of leaves → nextPoT/2 nodes
	// Level 2: pairs of level 1 → nextPoT/4 nodes
	// ... until root (1 node)
	internalNodes := make([][]byte, nextPoT-1)

	// Level 1: hash pairs of padded leaves.
	for i := 0; i < nextPoT; i += 2 {
		internalNodes[i/2] = merkleHash(padded[i], padded[i+1])
	}

	// Higher levels: hash pairs of previous level.
	prevStart := 0
	prevSize := nextPoT / 2
	for prevSize > 1 {
		nextStart := prevStart + prevSize
		for i := 0; i < prevSize; i += 2 {
			internalNodes[nextStart+i/2] = merkleHash(
				internalNodes[prevStart+i],
				internalNodes[prevStart+i+1],
			)
		}
		prevStart = nextStart
		prevSize /= 2
	}

	root := internalNodes[len(internalNodes)-1]
	return leaves, internalNodes, root
}

// merkleHash computes the parent hash from two children,
// matching go-subtree's calcMerkle behavior.
func merkleHash(left, right []byte) []byte {
	zeroHash := make([]byte, 32)
	leftIsZero := bytes.Equal(left, zeroHash)
	rightIsZero := bytes.Equal(right, zeroHash)

	if leftIsZero {
		return make([]byte, 32) // both zero → zero
	}
	if rightIsZero {
		return doubleSHA256(left, left) // duplicate left
	}
	return doubleSHA256(left, right)
}

func TestGroupByCallback(t *testing.T) {
	registrations := map[string][]string{
		"txid1": {"http://cb1.com", "http://cb2.com"},
		"txid2": {"http://cb1.com"},
		"txid3": {"http://cb3.com"},
	}

	grouped := GroupByCallback(registrations)

	if len(grouped["http://cb1.com"]) != 2 {
		t.Errorf("cb1 should have 2 txids, got %d", len(grouped["http://cb1.com"]))
	}
	if len(grouped["http://cb2.com"]) != 1 {
		t.Errorf("cb2 should have 1 txid, got %d", len(grouped["http://cb2.com"]))
	}
	if len(grouped["http://cb3.com"]) != 1 {
		t.Errorf("cb3 should have 1 txid, got %d", len(grouped["http://cb3.com"]))
	}
}

func TestBuild_NilForEmptyIndices(t *testing.T) {
	leaves, internal, _ := buildTestTree(t, 4)
	result := Build(100, leaves, internal, map[int]string{})
	if result != nil {
		t.Error("expected nil for empty indices")
	}
}

func TestBuild_SingleTx(t *testing.T) {
	leaves, internal, _ := buildTestTree(t, 2)
	indices := map[int]string{0: "txid0"}

	result := Build(100, leaves, internal, indices)
	if result == nil {
		t.Fatal("expected non-nil STUMP")
	}
	if result.BlockHeight != 100 {
		t.Errorf("expected block height 100, got %d", result.BlockHeight)
	}
	if result.TreeHeight != 1 {
		t.Errorf("expected tree height 1, got %d", result.TreeHeight)
	}
	// Level 0 should have the txid and its sibling.
	if len(result.Paths[0].Leaves) != 2 {
		t.Fatalf("expected 2 leaves at level 0, got %d", len(result.Paths[0].Leaves))
	}
	// Offset 0 should be the registered txid.
	if !result.Paths[0].Leaves[0].TxID {
		t.Error("expected offset 0 to have TxID=true")
	}
	if result.Paths[0].Leaves[0].Offset != 0 {
		t.Errorf("expected first leaf offset 0, got %d", result.Paths[0].Leaves[0].Offset)
	}
}

func TestEncode_ProducesBinaryOutput(t *testing.T) {
	hash := make([]byte, 32)
	hash[0] = 0xAA
	s := &STUMP{
		BlockHeight: 100,
		TreeHeight:  2,
		Paths: []PathLevel{
			{Leaves: []Leaf{{Offset: 1, Hash: hash}}},
			{Leaves: []Leaf{{Offset: 0, Duplicate: true}}},
		},
	}

	encoded := s.Encode()
	if len(encoded) == 0 {
		t.Fatal("encoded STUMP should not be empty")
	}
	// First byte should be block height CompactSize (100 = 0x64).
	if encoded[0] != 0x64 {
		t.Errorf("expected first byte 0x64, got 0x%x", encoded[0])
	}
}

func TestMergeSTUMPs_Empty(t *testing.T) {
	result := MergeSTUMPs(nil)
	if result != nil {
		t.Error("expected nil for empty input")
	}
}

func TestMergeSTUMPs_Single(t *testing.T) {
	s := &STUMP{BlockHeight: 1, TreeHeight: 1, Paths: []PathLevel{{Leaves: []Leaf{{Offset: 0}}}}}
	result := MergeSTUMPs([]*STUMP{s})
	if result != s {
		t.Error("expected same STUMP returned for single input")
	}
}

func TestMergeSTUMPs_TwoSTUMPs(t *testing.T) {
	hashA := make([]byte, 32)
	hashA[0] = 0xAA
	hashB := make([]byte, 32)
	hashB[0] = 0xBB
	hashC := make([]byte, 32)
	hashC[0] = 0xCC

	s1 := &STUMP{
		BlockHeight: 500,
		TreeHeight:  2,
		Paths: []PathLevel{
			{Leaves: []Leaf{
				{Offset: 1, Hash: hashA},
			}},
			{Leaves: []Leaf{
				{Offset: 0, Duplicate: true},
			}},
		},
	}

	s2 := &STUMP{
		BlockHeight: 500,
		TreeHeight:  2,
		Paths: []PathLevel{
			{Leaves: []Leaf{
				{Offset: 0, Hash: hashB},
			}},
			{Leaves: []Leaf{
				{Offset: 1, Hash: hashC},
			}},
		},
	}

	merged := MergeSTUMPs([]*STUMP{s1, s2})
	if merged == nil {
		t.Fatal("expected non-nil merged STUMP")
	}
	if merged.BlockHeight != 500 {
		t.Errorf("BlockHeight: expected 500, got %d", merged.BlockHeight)
	}
	if merged.TreeHeight != 2 {
		t.Errorf("TreeHeight: expected 2, got %d", merged.TreeHeight)
	}
	if len(merged.Paths[0].Leaves) != 2 {
		t.Errorf("level 0: expected 2 leaves, got %d", len(merged.Paths[0].Leaves))
	}
	if len(merged.Paths[1].Leaves) != 2 {
		t.Errorf("level 1: expected 2 leaves, got %d", len(merged.Paths[1].Leaves))
	}
}

func TestMergeSTUMPs_DeduplicatesSameOffset(t *testing.T) {
	hash := make([]byte, 32)
	hash[0] = 0xDD

	s1 := &STUMP{
		BlockHeight: 100,
		TreeHeight:  1,
		Paths: []PathLevel{
			{Leaves: []Leaf{{Offset: 3, Hash: hash}}},
		},
	}

	s2 := &STUMP{
		BlockHeight: 100,
		TreeHeight:  1,
		Paths: []PathLevel{
			{Leaves: []Leaf{{Offset: 3, Hash: hash}}},
		},
	}

	merged := MergeSTUMPs([]*STUMP{s1, s2})
	if merged == nil {
		t.Fatal("expected non-nil merged STUMP")
	}
	if len(merged.Paths[0].Leaves) != 1 {
		t.Errorf("expected 1 leaf at level 0 (deduped), got %d", len(merged.Paths[0].Leaves))
	}
}

func TestBuild_MultipleTxIds(t *testing.T) {
	leaves, internal, _ := buildTestTree(t, 4)

	// Register two txids at leaf positions 0 and 2.
	indices := map[int]string{
		0: "txid_a",
		2: "txid_c",
	}

	result := Build(200, leaves, internal, indices)
	if result == nil {
		t.Fatal("expected non-nil STUMP")
	}
	if result.BlockHeight != 200 {
		t.Errorf("BlockHeight: expected 200, got %d", result.BlockHeight)
	}
	if result.TreeHeight != 2 {
		t.Errorf("TreeHeight: expected 2, got %d", result.TreeHeight)
	}
	if len(result.Paths) != 2 {
		t.Errorf("expected 2 path levels, got %d", len(result.Paths))
	}
	// Level 0 should contain both registered txids and their siblings.
	if len(result.Paths[0].Leaves) < 2 {
		t.Error("expected at least 2 leaves at level 0")
	}

	// Verify registered txids have TxID=true.
	txidFound := make(map[uint64]bool)
	for _, leaf := range result.Paths[0].Leaves {
		if leaf.TxID {
			txidFound[leaf.Offset] = true
		}
	}
	if !txidFound[0] {
		t.Error("expected txid at offset 0 with TxID=true")
	}
	if !txidFound[2] {
		t.Error("expected txid at offset 2 with TxID=true")
	}
}

func TestEncode_NilSTUMP(t *testing.T) {
	var s *STUMP
	encoded := s.Encode()
	if encoded != nil {
		t.Errorf("expected nil for nil STUMP encode, got %v", encoded)
	}
}

func TestEncode_DuplicateLeafOmitsHash(t *testing.T) {
	s := &STUMP{
		BlockHeight: 1,
		TreeHeight:  1,
		Paths: []PathLevel{
			{Leaves: []Leaf{{Offset: 0, Duplicate: true}}},
		},
	}

	encoded := s.Encode()
	if len(encoded) == 0 {
		t.Fatal("encoded STUMP should not be empty")
	}

	// Encoding:
	// - CompactSize(1) for block height = 1 byte (0x01)
	// - 1 byte tree height = 0x01
	// - CompactSize(1) for leaf count = 1 byte
	// - CompactSize(0) for offset = 1 byte
	// - 1 byte flags (0x01 for duplicate)
	// No hash bytes follow a duplicate leaf.
	// Total: 5 bytes
	expectedLen := 5
	if len(encoded) != expectedLen {
		t.Errorf("expected %d bytes for duplicate-only leaf, got %d", expectedLen, len(encoded))
	}
}

func TestEncode_TxIDFlag(t *testing.T) {
	hash := make([]byte, 32)
	hash[0] = 0xFF

	s := &STUMP{
		BlockHeight: 10,
		TreeHeight:  1,
		Paths: []PathLevel{
			{Leaves: []Leaf{{Offset: 1, Hash: hash, TxID: true}}},
		},
	}

	encoded := s.Encode()
	if len(encoded) == 0 {
		t.Fatal("encoded STUMP should not be empty")
	}

	// Layout: CompactSize(10) = 1 byte, TreeHeight = 1 byte,
	// CompactSize(1) leaf count = 1 byte, CompactSize(1) offset = 1 byte,
	// flags at index 4.
	flagsByte := encoded[4]
	if flagsByte != 0x02 {
		t.Errorf("expected TxID flag 0x02, got 0x%02x", flagsByte)
	}
}

func TestGroupByCallback_Empty(t *testing.T) {
	result := GroupByCallback(map[string][]string{})
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestGroupByCallback_SingleTxMultipleCallbacks(t *testing.T) {
	registrations := map[string][]string{
		"txid1": {"http://a.com", "http://b.com", "http://c.com"},
	}
	result := GroupByCallback(registrations)
	if len(result) != 3 {
		t.Errorf("expected 3 callback groups, got %d", len(result))
	}
	for _, cb := range []string{"http://a.com", "http://b.com", "http://c.com"} {
		txids := result[cb]
		if len(txids) != 1 || txids[0] != "txid1" {
			t.Errorf("callback %q: expected [txid1], got %v", cb, txids)
		}
	}
}

func TestNextPowerOfTwo(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1}, {1, 1}, {2, 2}, {3, 4}, {4, 4},
		{5, 8}, {7, 8}, {8, 8}, {9, 16}, {10, 16},
		{15, 16}, {16, 16}, {17, 32},
	}
	for _, tc := range tests {
		got := nextPowerOfTwo(tc.input)
		if got != tc.expected {
			t.Errorf("nextPowerOfTwo(%d): expected %d, got %d", tc.input, tc.expected, got)
		}
	}
}

func TestAppendCompactSize(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80}},
		{252, []byte{0xFC}},
		{253, []byte{0xFD, 0xFD, 0x00}},
		{300, []byte{0xFD, 0x2C, 0x01}},
		{65535, []byte{0xFD, 0xFF, 0xFF}},
		{65536, []byte{0xFE, 0x00, 0x00, 0x01, 0x00}},
	}
	for _, tc := range tests {
		buf := appendCompactSize(nil, tc.value)
		if !bytes.Equal(buf, tc.expected) {
			t.Errorf("appendCompactSize(%d): expected %x, got %x", tc.value, tc.expected, buf)
		}
	}
}

// --- End-to-end BUMP verification ---

// verifyBUMPProof verifies that a STUMP/BUMP path can compute the expected root
// from each registered txid. This is the core BRC-0074 verification algorithm.
func verifyBUMPProof(t *testing.T, s *STUMP, expectedRoot []byte) {
	t.Helper()

	if s == nil {
		t.Fatal("STUMP is nil")
	}

	// Collect all txids from level 0.
	var txidLeaves []Leaf
	for _, leaf := range s.Paths[0].Leaves {
		if leaf.TxID {
			txidLeaves = append(txidLeaves, leaf)
		}
	}
	if len(txidLeaves) == 0 {
		t.Fatal("no txid leaves found at level 0")
	}

	// For each registered txid, compute the root using the BUMP path.
	for _, txidLeaf := range txidLeaves {
		workingHash := make([]byte, 32)
		copy(workingHash, txidLeaf.Hash)
		offset := txidLeaf.Offset

		for level := uint8(0); level < s.TreeHeight; level++ {
			siblingOffset := offset ^ 1

			// Find sibling at this level.
			var siblingHash []byte
			found := false
			for _, l := range s.Paths[level].Leaves {
				if l.Offset == siblingOffset {
					if l.Duplicate {
						siblingHash = make([]byte, 32)
						copy(siblingHash, workingHash)
					} else {
						siblingHash = l.Hash
					}
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("txid at offset %d: missing sibling at level %d offset %d",
					txidLeaf.Offset, level, siblingOffset)
			}

			// Combine: if we're at even offset, we're left child; odd = right child.
			if offset%2 == 0 {
				workingHash = doubleSHA256(workingHash, siblingHash)
			} else {
				workingHash = doubleSHA256(siblingHash, workingHash)
			}

			offset /= 2
		}

		if !bytes.Equal(workingHash, expectedRoot) {
			t.Errorf("txid at offset %d: computed root %x != expected %x",
				txidLeaf.Offset, workingHash[:8], expectedRoot[:8])
		}
	}
}

// TestEndToEnd_10Transactions_BRC74BUMP is the comprehensive end-to-end test:
// 10 transactions in a subtree, 3 registered, verify the STUMP follows BRC-0074
// BUMP format and each registered txid's proof computes the correct subtree root.
func TestEndToEnd_10Transactions_BRC74BUMP(t *testing.T) {
	const nTx = 10

	// 1. Create 10 distinct leaf hashes (simulating 10 transaction IDs).
	leaves, internalNodes, expectedRoot := buildTestTree(t, nTx)

	// 2. Register 3 txids at varied positions: beginning, middle, end.
	registeredIndices := map[int]string{
		0: "tx0",
		4: "tx4",
		9: "tx9", // last real leaf (sibling at 8)
	}

	// 3. Build the STUMP.
	s := Build(850000, leaves, internalNodes, registeredIndices)
	if s == nil {
		t.Fatal("Build returned nil STUMP")
	}

	// 4. Verify structure.
	// 10 leaves → nextPoT=16 → treeHeight=4 (4 levels: 0,1,2,3 to reach root at level 4).
	if s.BlockHeight != 850000 {
		t.Errorf("BlockHeight: expected 850000, got %d", s.BlockHeight)
	}
	if s.TreeHeight != 4 {
		t.Errorf("TreeHeight: expected 4 for 10 txs (padded to 16), got %d", s.TreeHeight)
	}
	if len(s.Paths) != 4 {
		t.Fatalf("expected 4 path levels, got %d", len(s.Paths))
	}

	// 5. Verify level 0 contains all 3 registered txids with TxID=true.
	txidOffsets := make(map[uint64]bool)
	for _, leaf := range s.Paths[0].Leaves {
		if leaf.TxID {
			txidOffsets[leaf.Offset] = true
			// Verify hash matches the original leaf.
			if !bytes.Equal(leaf.Hash, leaves[leaf.Offset]) {
				t.Errorf("txid at offset %d: hash mismatch", leaf.Offset)
			}
		}
	}
	for idx := range registeredIndices {
		if !txidOffsets[uint64(idx)] {
			t.Errorf("registered txid at index %d not found in level 0 with TxID=true", idx)
		}
	}

	// 6. Verify level 0 also contains sibling hashes for each registered txid.
	allOffsets := make(map[uint64]bool)
	for _, leaf := range s.Paths[0].Leaves {
		allOffsets[leaf.Offset] = true
	}
	for idx := range registeredIndices {
		siblingIdx := uint64(idx ^ 1)
		if !allOffsets[siblingIdx] {
			t.Errorf("missing sibling at offset %d for registered txid at %d", siblingIdx, idx)
		}
	}

	// 7. CORE VERIFICATION: each registered txid's BUMP proof computes the correct root.
	verifyBUMPProof(t, s, expectedRoot)

	// 8. Verify the binary encoding is well-formed BRC-0074 BUMP.
	encoded := s.Encode()
	if len(encoded) == 0 {
		t.Fatal("Encode returned empty")
	}

	// Decode and verify the binary structure.
	pos := 0

	// Block height (CompactSize).
	blockHeight, n := readCompactSize(t, encoded[pos:])
	pos += n
	if blockHeight != 850000 {
		t.Errorf("encoded block height: expected 850000, got %d", blockHeight)
	}

	// Tree height (1 byte).
	if encoded[pos] != 4 {
		t.Errorf("encoded tree height: expected 4, got %d", encoded[pos])
	}
	pos++

	// Read each path level.
	for level := 0; level < 4; level++ {
		leafCount, n := readCompactSize(t, encoded[pos:])
		pos += n

		if leafCount == 0 {
			t.Errorf("level %d: expected at least 1 leaf", level)
		}

		for i := uint64(0); i < leafCount; i++ {
			// Offset.
			_, n := readCompactSize(t, encoded[pos:])
			pos += n

			// Flags.
			flags := encoded[pos]
			pos++

			if flags == 0x01 {
				// Duplicate: no hash follows.
			} else {
				// Hash follows (32 bytes).
				if pos+32 > len(encoded) {
					t.Fatalf("level %d, leaf %d: not enough bytes for hash at pos %d", level, i, pos)
				}
				pos += 32
			}
		}
	}

	// We should have consumed the entire buffer.
	if pos != len(encoded) {
		t.Errorf("encoded BUMP has %d trailing bytes (consumed %d of %d)", len(encoded)-pos, pos, len(encoded))
	}
}

// TestEndToEnd_10Transactions_DuplicateDetection verifies that the STUMP correctly
// uses the duplicate flag for padded positions at the edge of the tree.
func TestEndToEnd_10Transactions_DuplicateDetection(t *testing.T) {
	// 9 leaves: padded to 16. Leaf 8 has sibling at index 9 which is beyond
	// the 9 real leaves (indices 0-8), so sibling 9 should be a duplicate.
	const nTx = 9
	leaves, internalNodes, expectedRoot := buildTestTree(t, nTx)

	// Register the last real txid (index 8).
	registeredIndices := map[int]string{8: "tx8"}

	s := Build(100, leaves, internalNodes, registeredIndices)
	if s == nil {
		t.Fatal("Build returned nil")
	}

	// Level 0: txid at offset 8, sibling at offset 9 should be duplicate.
	var foundDuplicate bool
	for _, leaf := range s.Paths[0].Leaves {
		if leaf.Offset == 9 {
			if !leaf.Duplicate {
				t.Error("leaf at offset 9 should be Duplicate (beyond 9 real leaves)")
			}
			foundDuplicate = true
		}
	}
	if !foundDuplicate {
		t.Error("expected a leaf at offset 9")
	}

	// The proof should still verify correctly.
	verifyBUMPProof(t, s, expectedRoot)
}

// TestEndToEnd_2Transactions verifies minimal tree case.
func TestEndToEnd_2Transactions(t *testing.T) {
	leaves, internalNodes, expectedRoot := buildTestTree(t, 2)

	registeredIndices := map[int]string{0: "tx0", 1: "tx1"}

	s := Build(1, leaves, internalNodes, registeredIndices)
	if s == nil {
		t.Fatal("Build returned nil")
	}
	if s.TreeHeight != 1 {
		t.Errorf("expected tree height 1, got %d", s.TreeHeight)
	}

	// Both txids registered: level 0 should have 2 leaves, both with TxID=true.
	if len(s.Paths[0].Leaves) != 2 {
		t.Fatalf("expected 2 leaves at level 0, got %d", len(s.Paths[0].Leaves))
	}
	for _, leaf := range s.Paths[0].Leaves {
		if !leaf.TxID {
			t.Errorf("leaf at offset %d should have TxID=true", leaf.Offset)
		}
	}

	verifyBUMPProof(t, s, expectedRoot)
}

// TestEndToEnd_PowerOfTwo_4Transactions verifies a perfect power-of-2 tree (no padding).
func TestEndToEnd_PowerOfTwo_4Transactions(t *testing.T) {
	leaves, internalNodes, expectedRoot := buildTestTree(t, 4)

	registeredIndices := map[int]string{1: "tx1", 3: "tx3"}

	s := Build(500, leaves, internalNodes, registeredIndices)
	if s == nil {
		t.Fatal("Build returned nil")
	}
	if s.TreeHeight != 2 {
		t.Errorf("expected tree height 2, got %d", s.TreeHeight)
	}

	verifyBUMPProof(t, s, expectedRoot)
}

// readCompactSize reads a Bitcoin CompactSize uint from data, returns (value, bytesRead).
func readCompactSize(t *testing.T, data []byte) (uint64, int) {
	t.Helper()
	if len(data) == 0 {
		t.Fatal("readCompactSize: empty data")
	}
	first := data[0]
	switch {
	case first < 253:
		return uint64(first), 1
	case first == 0xFD:
		if len(data) < 3 {
			t.Fatal("readCompactSize: not enough bytes for 0xFD")
		}
		return uint64(binary.LittleEndian.Uint16(data[1:3])), 3
	case first == 0xFE:
		if len(data) < 5 {
			t.Fatal("readCompactSize: not enough bytes for 0xFE")
		}
		return uint64(binary.LittleEndian.Uint32(data[1:5])), 5
	default:
		if len(data) < 9 {
			t.Fatal("readCompactSize: not enough bytes for 0xFF")
		}
		return binary.LittleEndian.Uint64(data[1:9]), 9
	}
}
