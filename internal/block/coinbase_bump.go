package block

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	sdktx "github.com/bsv-blockchain/go-sdk/transaction"
	subtreepkg "github.com/bsv-blockchain/go-subtree"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/logfields"
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

// validateCoinbaseBUMP checks that a BRC-74 coinbase BUMP folds the given
// coinbase txid to the given header merkle root at the given height. Used to
// gate teranode's ready-made coinbase BUMP (from the block binary tail)
// before preferring it over local reconstruction.
func validateCoinbaseBUMP(bumpHex string, cbTxID []byte, merkleRoot *chainhash.Hash, height uint32) error {
	raw, err := hex.DecodeString(bumpHex)
	if err != nil {
		return fmt.Errorf("decode coinbase BUMP hex: %w", err)
	}
	mp, err := sdktx.NewMerklePathFromBinary(raw)
	if err != nil {
		return fmt.Errorf("parse coinbase BUMP: %w", err)
	}
	if mp.BlockHeight != height {
		return fmt.Errorf("coinbase BUMP height %d != block height %d", mp.BlockHeight, height)
	}
	cbh, err := sdkchainhash.NewHash(cbTxID)
	if err != nil {
		return fmt.Errorf("coinbase txid: %w", err)
	}
	root, err := mp.ComputeRoot(cbh)
	if err != nil {
		return fmt.Errorf("compute root from coinbase BUMP: %w", err)
	}
	if !bytes.Equal(root[:], merkleRoot[:]) {
		return fmt.Errorf("coinbase BUMP folds to %s, want header root %s", root.String(), merkleRoot.String())
	}
	return nil
}

// verifySubtreeContentAddress checks that raw DataHub subtree bytes fold to
// the subtree hash they were requested by. Subtrees are content-addressed
// (the identifier in the block binary IS the placeholder-based subtree root),
// so any peer-side truncation, reordering, or substitution is detectable
// before the leaves are used to build proofs. Callers treat a mismatch like
// a fetch failure so failover/retry continues to the next peer.
func verifySubtreeContentAddress(raw []byte, subtreeHash string) error {
	nodes, err := datahub.ParseRawNodes(raw)
	if err != nil {
		return fmt.Errorf("parse subtree %s: %w", subtreeHash, err)
	}
	want, err := chainhash.NewHashFromStr(subtreeHash)
	if err != nil {
		return fmt.Errorf("subtree hash %s: %w", subtreeHash, err)
	}
	got := (&subtreepkg.Subtree{Nodes: nodes}).RootHash()
	if got == nil || !got.IsEqual(want) {
		return fmt.Errorf("subtree content mismatch: served leaves fold to %s, want %s", got, want)
	}
	return nil
}

// subtreeHeight returns the natural merkle height of a subtree with n leaves:
// the smallest h with 1<<h >= n (0 for a single leaf).
func subtreeHeight(n int) int {
	h := 0
	for 1<<h < n {
		h++
	}
	return h
}

// liftedSubtreeRoots returns the top-tree leaf hashes used to compose the
// block merkle root: the subtree roots with the FINAL root lifted to the
// first subtree's height when the final subtree is shorter. This mirrors
// teranode's canonical computation (model.Block CheckMerkleRoot): only the
// final subtree may be incomplete, and its root is self-hashed (H(r‖r)) once
// per missing level so it occupies the slot of a same-capacity subtree.
// Composing with UN-lifted roots produces a merkle root the canonical chain
// disagrees with for every block whose final subtree holds at most half the
// first subtree's capacity — the proof-corrupting bug found on the mainnet
// 954978–956998 backlog.
//
// The final subtree's leaf count is derived from the block's total tx count:
// teranode enforces that every non-final subtree is complete at the first
// subtree's (power-of-two) length.
func liftedSubtreeRoots(roots []chainhash.Hash, sub0Leaves int, totalTxCount uint64) ([]chainhash.Hash, error) {
	n := len(roots)
	if n <= 1 {
		return roots, nil
	}
	if sub0Leaves <= 0 || !subtreepkg.IsPowerOfTwo(sub0Leaves) {
		return nil, fmt.Errorf("first subtree leaf count %d is not a power of two", sub0Leaves)
	}
	finalLeaves := int(totalTxCount) - (n-1)*sub0Leaves //nolint:gosec // tx counts are far below int range
	if finalLeaves <= 0 || finalLeaves > sub0Leaves {
		return nil, fmt.Errorf("inconsistent block shape: %d txs across %d subtrees of %d leaves implies %d leaves in the final subtree",
			totalTxCount, n, sub0Leaves, finalLeaves)
	}
	lift := subtreeHeight(sub0Leaves) - subtreeHeight(finalLeaves)
	if lift <= 0 {
		return roots, nil
	}
	out := append([]chainhash.Hash(nil), roots...)
	lifted := out[n-1]
	for i := 0; i < lift; i++ {
		buf := make([]byte, 0, 64)
		buf = append(buf, lifted[:]...)
		buf = append(buf, lifted[:]...)
		copy(lifted[:], chainhash.DoubleHashB(buf))
	}
	out[n-1] = lifted
	return out, nil
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
// byte order and MUST already carry the final-subtree height-lift (see
// liftedSubtreeRoots) — passing raw roots produces siblings that fold to a
// root the canonical chain disagrees with whenever the final subtree is
// shorter than half the first subtree's capacity.
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
// merkle root and coinbase tx come from the already-fetched block metadata
// (falling back to the P2P BlockMessage fields, which are not reliably
// populated: teranode's announcement never carries the coinbase, and
// /reprocess-driven messages carry neither); the subtree list comes from the
// same metadata; only the coinbase BUMP requires fetching subtree 0.
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

	headerHex := meta.HeaderHex
	if headerHex == "" {
		headerHex = blockMsg.Header
	}
	merkleRoot, err := merkleRootFromHeader(headerHex)
	if err != nil {
		// Without the merkle root a consumer can't validate against the canonical
		// chain, but the subtree list is still useful. Log and carry on.
		p.Logger.Warn("BLOCK_PROCESSED: could not extract merkle root from header",
			logfields.BlockHash(blockMsg.Hash), "error", err)
		return data
	}
	data.MerkleRoot = merkleRoot.String()

	// Everything below builds the coinbase BUMP. Any failure leaves the merkle
	// root + subtree list in place and drops only the coinbase BUMP.
	if len(meta.Subtrees) == 0 {
		return data // coinbase-only block: no subtrees, no coinbase BUMP needed.
	}

	coinbaseHex := meta.CoinbaseTxHex
	if coinbaseHex == "" {
		coinbaseHex = blockMsg.Coinbase
	}
	cbTxID, err := coinbaseTxIDFromHex(coinbaseHex)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not compute coinbase txid; omitting coinbase BUMP",
			logfields.BlockHash(blockMsg.Hash), "error", err)
		return data
	}

	// Prefer teranode's ready-made coinbase BUMP from the block binary tail:
	// it is authoritative (computed by the node from the full block, lift
	// included) and available even when every peer has pruned the block's
	// subtree data. Gate it behind full validation; on any mismatch fall
	// back to local reconstruction below.
	if meta.CoinbaseBUMPHex != "" {
		if vErr := validateCoinbaseBUMP(meta.CoinbaseBUMPHex, cbTxID, merkleRoot, meta.Height); vErr != nil {
			p.Logger.Warn("BLOCK_PROCESSED: upstream coinbase BUMP failed validation; reconstructing locally",
				logfields.BlockHash(blockMsg.Hash), "error", vErr)
		} else {
			data.CoinbaseBUMP = meta.CoinbaseBUMPHex
			return data
		}
	}

	roots := make([]chainhash.Hash, len(meta.Subtrees))
	for i, s := range meta.Subtrees {
		h, hErr := chainhash.NewHashFromStr(s)
		if hErr != nil {
			p.Logger.Warn("BLOCK_PROCESSED: bad subtree hash; omitting coinbase BUMP",
				logfields.BlockHash(blockMsg.Hash), logfields.SubtreeHash(s), "error", hErr)
			return data
		}
		roots[i] = *h
	}

	// Fetch subtree 0's contents (the coinbase's subtree). Store it so the
	// subtree worker doesn't re-fetch it later. Fails over across DataHub
	// peers: the peer that served this block's metadata (resolvedURL) may have
	// already pruned its subtree contents while another peer still serves them
	// — without failover the coinbase BUMP would be dropped despite the data
	// being available on the network.
	raw, _, err := p.fetchSubtreeRawWithFailover(ctx, blockMsg.Hash, meta.Subtrees[0], resolvedURL)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not fetch subtree 0; omitting coinbase BUMP",
			logfields.BlockHash(blockMsg.Hash), "error", err)
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
			logfields.BlockHash(blockMsg.Hash), "error", err)
		return data
	}

	// Compose the top tree the way teranode does: lift the final subtree's
	// root to the first subtree's height when it is shorter. Without this,
	// the fold below cannot match the header root for lifted blocks.
	topRoots, err := liftedSubtreeRoots(roots, len(nodes), meta.TransactionCount)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: cannot determine top-tree shape; omitting coinbase BUMP",
			logfields.BlockHash(blockMsg.Hash), "error", err)
		return data
	}

	siblings, err := buildCoinbaseSiblings(nodes, topRoots)
	if err != nil {
		p.Logger.Warn("BLOCK_PROCESSED: could not build coinbase siblings; omitting coinbase BUMP",
			logfields.BlockHash(blockMsg.Hash), "error", err)
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
			logfields.BlockHash(blockMsg.Hash),
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
