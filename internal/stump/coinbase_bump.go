package stump

import "github.com/bsv-blockchain/go-bt/v2/chainhash"

// EncodeCoinbaseBUMP encodes a BRC-0074 BUMP proving the transaction at global
// block offset 0 — the coinbase — up to the block merkle root.
//
// siblings are the merkle-path sibling hashes, level 0 (the leaf level) first,
// one per level. Because the coinbase sits at offset 0 it always climbs the
// left spine of the tree, so at every level the working node is at offset 0 and
// its sibling is at offset 1. All hashes are 32-byte internal (little-endian)
// byte order, matching the rest of the STUMP encoding and what go-sdk's
// transaction.NewMerklePathFromBinary expects.
//
// The returned BUMP carries the real coinbase txid at level 0, offset 0 (flag
// 0x02 / txid), so a consumer can both read the coinbase txid and fold it up to
// the block merkle root. With no siblings (a single-transaction block) the
// result is an empty-path BUMP — callers that need the coinbase txid in the
// payload should guard against that degenerate case.
func EncodeCoinbaseBUMP(blockHeight uint64, coinbaseTxID []byte, siblings [][]byte) []byte {
	s := &STUMP{
		BlockHeight: blockHeight,
		TreeHeight:  uint8(len(siblings)), //nolint:gosec // tree height is log2(block tx count), far below 255
		Paths:       make([]PathLevel, len(siblings)),
	}
	for i, sib := range siblings {
		leaves := make([]Leaf, 0, 2)
		if i == 0 {
			cb := make([]byte, len(coinbaseTxID))
			copy(cb, coinbaseTxID)
			leaves = append(leaves, Leaf{Offset: 0, Hash: cb, TxID: true})
		}
		sibCopy := make([]byte, len(sib))
		copy(sibCopy, sib)
		leaves = append(leaves, Leaf{Offset: 1, Hash: sibCopy})
		s.Paths[i] = PathLevel{Leaves: leaves}
	}
	return s.Encode()
}

// CoinbaseRootFromSiblings folds the coinbase txid up through the index-0
// sibling hashes and returns the resulting block merkle root (32-byte internal
// byte order). The coinbase is always the left child, so each step hashes
// working||sibling. Used to self-validate a coinbase BUMP against the block
// header merkle root before publishing it.
func CoinbaseRootFromSiblings(coinbaseTxID []byte, siblings [][]byte) []byte {
	working := make([]byte, len(coinbaseTxID))
	copy(working, coinbaseTxID)
	for _, sib := range siblings {
		buf := make([]byte, 0, 64)
		buf = append(buf, working...)
		buf = append(buf, sib...)
		working = chainhash.DoubleHashB(buf)
	}
	return working
}
