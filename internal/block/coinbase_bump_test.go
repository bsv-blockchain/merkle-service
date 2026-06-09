package block

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	sdktx "github.com/bsv-blockchain/go-sdk/transaction"
	subtreepkg "github.com/bsv-blockchain/go-subtree"

	"github.com/bsv-blockchain/merkle-service/internal/stump"
)

// testHash returns a deterministic 32-byte hash for a seed, used to fabricate
// subtree leaves and the coinbase txid.
func testHash(seed uint64) chainhash.Hash {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seed)
	sum := sha256.Sum256(b[:])
	var h chainhash.Hash
	copy(h[:], sum[:])
	return h
}

func rootOf(t *testing.T, nodes []subtreepkg.Node) chainhash.Hash {
	t.Helper()
	r := (&subtreepkg.Subtree{Nodes: nodes}).RootHash()
	if r == nil {
		t.Fatal("nil root hash")
	}
	return *r
}

// TestBuildCoinbaseBUMP_FoldsToHeaderMerkleRoot is the core correctness check:
// the coinbase BUMP merkle-service builds must fold the real coinbase txid up
// to the canonical block merkle root, even though the stored subtree-0 root and
// the published subtree hashes are computed against the coinbase placeholder.
// It also confirms the encoded BUMP parses with go-sdk (arcade's parser) and
// computes the same root.
func TestBuildCoinbaseBUMP_FoldsToHeaderMerkleRoot(t *testing.T) {
	const placeholderSeed = 0 // subtree-0 leaf 0 placeholder (zero-ish but deterministic)
	cases := []struct {
		numSubtrees int
		leavesPer   int
	}{
		{1, 4},
		{2, 4},
		{4, 1},  // each subtree is a single leaf
		{3, 8},  // non-power-of-two subtree count
		{16, 8}, // mirrors the incident block (16 subtrees)
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("subtrees=%d_leaves=%d", tc.numSubtrees, tc.leavesPer), func(t *testing.T) {
			realCoinbase := testHash(999_999)
			placeholder := testHash(placeholderSeed)

			// Build every subtree's leaves. Subtree 0, leaf 0 is the coinbase
			// PLACEHOLDER (this is what the datahub/subtree store actually holds).
			subtreeNodes := make([][]subtreepkg.Node, tc.numSubtrees)
			for i := 0; i < tc.numSubtrees; i++ {
				nodes := make([]subtreepkg.Node, tc.leavesPer)
				for j := 0; j < tc.leavesPer; j++ {
					nodes[j] = subtreepkg.Node{Hash: testHash(uint64(i*1000 + j + 1))}
				}
				if i == 0 {
					nodes[0] = subtreepkg.Node{Hash: placeholder}
				}
				subtreeNodes[i] = nodes
			}

			// Placeholder-based subtree roots — what merkle-service publishes as
			// SubtreeHashes.
			placeholderRoots := make([]chainhash.Hash, tc.numSubtrees)
			for i := range subtreeNodes {
				placeholderRoots[i] = rootOf(t, subtreeNodes[i])
			}

			// Canonical (header) merkle root: recompute with the REAL coinbase at
			// subtree-0 leaf 0.
			realSub0 := append([]subtreepkg.Node(nil), subtreeNodes[0]...)
			realSub0[0] = subtreepkg.Node{Hash: realCoinbase}
			realSub0Root := rootOf(t, realSub0)
			var headerRoot chainhash.Hash
			if tc.numSubtrees == 1 {
				headerRoot = realSub0Root
			} else {
				topNodes := make([]subtreepkg.Node, tc.numSubtrees)
				topNodes[0] = subtreepkg.Node{Hash: realSub0Root}
				for i := 1; i < tc.numSubtrees; i++ {
					topNodes[i] = subtreepkg.Node{Hash: placeholderRoots[i]}
				}
				headerRoot = rootOf(t, topNodes)
			}

			// Build the coinbase siblings from the PLACEHOLDER-based inputs (what
			// the producer actually has).
			siblings, err := buildCoinbaseSiblings(subtreeNodes[0], placeholderRoots)
			if err != nil {
				t.Fatalf("buildCoinbaseSiblings: %v", err)
			}

			// Folding the real coinbase up the siblings must reproduce the header
			// merkle root.
			computed := stump.CoinbaseRootFromSiblings(realCoinbase[:], siblings)
			if !bytes.Equal(computed, headerRoot[:]) {
				gotHash, _ := chainhash.NewHash(computed)
				t.Fatalf("folded root = %s, want header root %s", gotHash, headerRoot)
			}

			// The encoded BUMP must parse with go-sdk (arcade's parser) and
			// ComputeRoot to the same value.
			encoded := stump.EncodeCoinbaseBUMP(123, realCoinbase[:], siblings)
			mp, err := sdktx.NewMerklePathFromBinary(encoded)
			if err != nil {
				t.Fatalf("go-sdk NewMerklePathFromBinary: %v", err)
			}
			sdkCoinbase, err := sdkchainhash.NewHash(realCoinbase[:])
			if err != nil {
				t.Fatalf("sdk coinbase hash: %v", err)
			}
			gotRoot, err := mp.ComputeRoot(sdkCoinbase)
			if err != nil {
				t.Fatalf("go-sdk ComputeRoot: %v", err)
			}
			if !bytes.Equal(gotRoot[:], headerRoot[:]) {
				t.Fatalf("go-sdk computed root = %s, want %s", gotRoot, &headerRoot)
			}
		})
	}
}

func TestMerkleRootFromHeader(t *testing.T) {
	// 80-byte header: version(4) prev(32) merkleRoot(32) time(4) bits(4) nonce(4).
	want := testHash(42)
	header := make([]byte, 80)
	copy(header[36:68], want[:])
	got, err := merkleRootFromHeader(hex.EncodeToString(header))
	if err != nil {
		t.Fatalf("merkleRootFromHeader: %v", err)
	}
	if !got.IsEqual(&want) {
		t.Fatalf("merkle root = %s, want %s", got, &want)
	}

	if _, err := merkleRootFromHeader("zz"); err == nil {
		t.Fatal("expected error for non-hex header")
	}
	if _, err := merkleRootFromHeader("00"); err == nil {
		t.Fatal("expected error for short header")
	}
}

func TestCoinbaseTxIDFromHex(t *testing.T) {
	raw := []byte{0x01, 0x02, 0x03, 0x04}
	want := chainhash.DoubleHashB(raw)
	got, err := coinbaseTxIDFromHex(hex.EncodeToString(raw))
	if err != nil {
		t.Fatalf("coinbaseTxIDFromHex: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("coinbase txid mismatch")
	}
	if _, err := coinbaseTxIDFromHex(""); err == nil {
		t.Fatal("expected error for empty coinbase")
	}
}
