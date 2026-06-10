package block

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	"github.com/bsv-blockchain/merkle-service/internal/stump"
)

// merkleRootFromHeader extracts the block header merkle root — bytes 36..68 of
// the 80-byte block header — from a hex-encoded header. Those bytes are stored
// in internal (little-endian) byte order; the returned chainhash.Hash.String()
// yields the conventional display-order hex used on the wire.
func merkleRootFromHeader(headerHex string) (*chainhash.Hash, error) {
	raw, err := hex.DecodeString(headerHex)
	if err != nil {
		return nil, fmt.Errorf("decode header hex: %w", err)
	}
	if len(raw) < 80 {
		return nil, fmt.Errorf("header too short: %d bytes (want >= 80)", len(raw))
	}
	return chainhash.NewHash(raw[36:68])
}

// coinbaseTxIDFromHex computes the coinbase transaction id (32-byte internal
// order) as the double-SHA256 of the raw coinbase transaction bytes.
func coinbaseTxIDFromHex(coinbaseHex string) ([]byte, error) {
	raw, err := hex.DecodeString(coinbaseHex)
	if err != nil {
		return nil, fmt.Errorf("decode coinbase hex: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty coinbase transaction")
	}
	return chainhash.DoubleHashB(raw), nil
}

// buildCoinbaseSiblings returns the merkle-path sibling hashes proving the
// coinbase transaction (subtree 0, leaf 0) up to the block merkle root:
// subtree-0 internal siblings first (level 0 = leaf level), then the
// across-subtree (top tree, leaves = subtree roots) siblings. It delegates the
// actual hashing to go-subtree so the zero-pad-to-power-of-two and
// duplicate-self-on-odd rules match the canonical block merkle root exactly.
//
// Because the coinbase sits at offset 0 it climbs the left spine, so every
// returned sibling is the offset-1 node at its level. subtreeRoots are internal
// byte order.
func buildCoinbaseSiblings(subtree0Nodes []subtreepkg.Node, subtreeRoots []chainhash.Hash) ([][]byte, error) {
	if len(subtree0Nodes) == 0 {
		return nil, fmt.Errorf("subtree 0 has no nodes")
	}

	sub0 := &subtreepkg.Subtree{Nodes: subtree0Nodes}
	sub0Proof, err := sub0.GetMerkleProof(0)
	if err != nil {
		return nil, fmt.Errorf("subtree-0 merkle proof: %w", err)
	}

	siblings := make([][]byte, 0, len(sub0Proof)+len(subtreeRoots))
	for _, h := range sub0Proof {
		siblings = append(siblings, append([]byte(nil), h[:]...))
	}

	// The across-subtree path only exists when the block has more than one
	// subtree; a single-subtree block's root is the (corrected) subtree-0 root.
	if len(subtreeRoots) > 1 {
		topNodes := make([]subtreepkg.Node, len(subtreeRoots))
		for i, r := range subtreeRoots {
			topNodes[i] = subtreepkg.Node{Hash: r}
		}
		top := &subtreepkg.Subtree{Nodes: topNodes}
		topProof, err := top.GetMerkleProof(0)
		if err != nil {
			return nil, fmt.Errorf("top-tree merkle proof: %w", err)
		}
		for _, h := range topProof {
			siblings = append(siblings, append([]byte(nil), h[:]...))
		}
	}

	return siblings, nil
}

// buildBlockProcessedData assembles the BlockProcessedData published on
// BLOCK_PROCESSED. It is best-effort: any step that fails degrades gracefully
// to the richest data gathered so far (a consumer that's missing a field falls
// back to a datahub), and it never returns nil for a block with subtrees. The
// merkle root and coinbase tx come from the P2P BlockMessage (no datahub); the
// subtree list comes from the already-fetched metadata; only the coinbase
// BUMP requires fetching subtree 0.
func (p *Processor) buildBlockProcessedData(
	ctx context.Context,
	blockMsg *kafka.BlockMessage,
	meta *datahub.BlockMetadata,
	resolvedURL string,
) *store.BlockProcessedData {
	data := &store.BlockProcessedData{
		SubtreeCount:  len(meta.Subtrees),
		SubtreeHashes: meta.Subtrees,
	}

	merkleRoot, err := merkleRootFromHeader(blockMsg.Header)
	if err != nil {
		// Without the merkle root a consumer can't validate against the canonical
		// chain, but the subtree list is still useful. Log and carry on.
		p.Logger.Warn("BLOCK_PROCESSED: could not extract merkle root from header",
			"blockHash", blockMsg.Hash, "error", err)
		return data
	}
	data.MerkleRoot = merkleRoot.String()

	// Everything below builds the coinbase BUMP. Any failure leaves the merkle
	// root + subtree list in place and drops only the coinbase BUMP.
	if len(meta.Subtrees) == 0 {
		return data // coinbase-only block: no subtrees, no coinbase BUMP needed.
	}

	cbTxID, err := coinbaseTxIDFromHex(blockMsg.Coinbase)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not compute coinbase txid; omitting coinbase BUMP",
			"blockHash", blockMsg.Hash, "error", err)
		return data
	}

	roots := make([]chainhash.Hash, len(meta.Subtrees))
	for i, s := range meta.Subtrees {
		h, hErr := chainhash.NewHashFromStr(s)
		if hErr != nil {
			p.Logger.Warn("BLOCK_PROCESSED: bad subtree hash; omitting coinbase BUMP",
				"blockHash", blockMsg.Hash, "subtreeHash", s, "error", hErr)
			return data
		}
		roots[i] = *h
	}

	// Fetch subtree 0's contents (the coinbase's subtree). Store it so the
	// subtree worker doesn't re-fetch it later.
	raw, err := p.dataHubClient.FetchSubtreeRaw(ctx, resolvedURL, meta.Subtrees[0])
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not fetch subtree 0; omitting coinbase BUMP",
			"blockHash", blockMsg.Hash, "error", err)
		return data
	}
	if p.subtreeStore != nil {
		if sErr := p.subtreeStore.StoreSubtree(meta.Subtrees[0], raw, uint64(meta.Height)); sErr != nil {
			p.Logger.Debug("BLOCK_PROCESSED: failed to cache subtree 0", "error", sErr)
		}
	}
	nodes, err := datahub.ParseRawNodes(raw)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not parse subtree 0; omitting coinbase BUMP",
			"blockHash", blockMsg.Hash, "error", err)
		return data
	}

	siblings, err := buildCoinbaseSiblings(nodes, roots)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not build coinbase siblings; omitting coinbase BUMP",
			"blockHash", blockMsg.Hash, "error", err)
		return data
	}

	// Self-validate: folding the real coinbase up the siblings must reproduce
	// the canonical header merkle root. Publishing a coinbase BUMP that fails
	// its own root check would poison the consumer — refuse and let it fall
	// back to a datahub.
	computed := stump.CoinbaseRootFromSiblings(cbTxID, siblings)
	if !bytes.Equal(computed, merkleRoot[:]) {
		computedHash, _ := chainhash.NewHash(computed)
		p.Logger.Warn("BLOCK_PROCESSED: coinbase BUMP self-validation failed; omitting coinbase BUMP",
			"blockHash", blockMsg.Hash,
			"computedRoot", hashString(computedHash),
			"headerRoot", merkleRoot.String())
		metrics.CoinbaseBumpValidationFailures.Inc()
		return data
	}

	data.CoinbaseBUMP = hex.EncodeToString(stump.EncodeCoinbaseBUMP(uint64(meta.Height), cbTxID, siblings))
	return data
}

// hashString renders a chainhash pointer that may be nil (NewHash only errors
// on a wrong length, but guard anyway) for log output.
func hashString(h *chainhash.Hash) string {
	if h == nil {
		return ""
	}
	return h.String()
}
