package block

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	sdktx "github.com/bsv-blockchain/go-sdk/transaction"
	subtreepkg "github.com/bsv-blockchain/go-subtree"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
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
		return chainhash.Hash{} // unreachable after Fatal; guards the deref below
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

// buildTestHeaderHex returns the hex of an 80-byte header whose merkle-root
// field (bytes 36..68) carries root.
func buildTestHeaderHex(root chainhash.Hash) string {
	header := make([]byte, 80)
	copy(header[36:68], root[:])
	return hex.EncodeToString(header)
}

// genesisCoinbaseHex is the Bitcoin genesis coinbase transaction — a
// known-good, parseable coinbase (exactly one input) for tests that need
// real raw coinbase bytes.
const genesisCoinbaseHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000"

func newBlockDataTestProcessor(t *testing.T) *Processor {
	t.Helper()
	p := &Processor{}
	p.InitBase("block-processor-test")
	p.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return p
}

// TestBuildBlockProcessedData_ReprocessShape_HeaderFromMeta covers the
// /reprocess message shape: the BlockMessage carries neither Header nor
// Coinbase, so the merkle root must come from the fetched block metadata.
// Before HeaderHex existed this shape always failed with "header too short:
// 0 bytes" and BLOCK_PROCESSED shipped without a merkle root.
func TestBuildBlockProcessedData_ReprocessShape_HeaderFromMeta(t *testing.T) {
	p := newBlockDataTestProcessor(t)

	root := testHash(42)
	blockMsg := &kafka.BlockMessage{Hash: "reprocessed-block"} // no Header, no Coinbase
	meta := &datahub.BlockMetadata{HeaderHex: buildTestHeaderHex(root)}

	data := p.buildBlockProcessedData(context.Background(), blockMsg, meta, "http://unused")
	if data.MerkleRoot != root.String() {
		t.Fatalf("MerkleRoot = %q, want %q (meta header must be used when the message has none)",
			data.MerkleRoot, root.String())
	}
}

// TestBuildBlockProcessedData_LiveShape_CoinbaseFromMeta covers the live
// announcement shape: teranode populates Header but never Coinbase, so the
// coinbase BUMP must be built from the metadata's coinbase. Before
// CoinbaseTxHex existed every live multi-leaf block logged "could not compute
// coinbase txid" and shipped without a coinbase BUMP.
func TestBuildBlockProcessedData_LiveShape_CoinbaseFromMeta(t *testing.T) {
	p := newBlockDataTestProcessor(t)

	coinbaseRaw, err := hex.DecodeString(genesisCoinbaseHex)
	if err != nil {
		t.Fatalf("decode genesis coinbase: %v", err)
	}
	cbTxID := chainhash.DoubleHashB(coinbaseRaw)

	// Single subtree: leaf 0 is the coinbase placeholder, three real leaves.
	placeholder := testHash(0)
	nodes := []subtreepkg.Node{
		{Hash: placeholder},
		{Hash: testHash(1)},
		{Hash: testHash(2)},
		{Hash: testHash(3)},
	}
	placeholderRoot := rootOf(t, nodes)

	// Canonical header root: same subtree with the REAL coinbase at leaf 0.
	realNodes := append([]subtreepkg.Node(nil), nodes...)
	var cbHash chainhash.Hash
	copy(cbHash[:], cbTxID)
	realNodes[0] = subtreepkg.Node{Hash: cbHash}
	headerRoot := rootOf(t, realNodes)

	// DataHub test double serving subtree 0 (placeholder-based leaves).
	var subtreeRaw []byte
	for _, n := range nodes {
		subtreeRaw = append(subtreeRaw, n.Hash[:]...)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/subtree/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(subtreeRaw)
	}))
	defer server.Close()
	p.dataHubClient = datahub.NewClient(5, 0, p.Logger)

	blockMsg := &kafka.BlockMessage{
		Hash:   "live-block",
		Header: buildTestHeaderHex(headerRoot),
		// Coinbase deliberately empty — teranode's announcement never sets it.
	}
	meta := &datahub.BlockMetadata{
		Height:        957032,
		Subtrees:      []string{placeholderRoot.String()},
		CoinbaseTxHex: genesisCoinbaseHex,
	}

	data := p.buildBlockProcessedData(context.Background(), blockMsg, meta, server.URL)
	if data.MerkleRoot != headerRoot.String() {
		t.Fatalf("MerkleRoot = %q, want %q", data.MerkleRoot, headerRoot.String())
	}
	if data.CoinbaseBUMP == "" {
		t.Fatal("CoinbaseBUMP empty: metadata coinbase must be used when the message has none")
	}

	// The emitted BUMP must fold the real coinbase to the header root.
	encoded, err := hex.DecodeString(data.CoinbaseBUMP)
	if err != nil {
		t.Fatalf("decode CoinbaseBUMP: %v", err)
	}
	mp, err := sdktx.NewMerklePathFromBinary(encoded)
	if err != nil {
		t.Fatalf("go-sdk NewMerklePathFromBinary: %v", err)
	}
	sdkCoinbase, err := sdkchainhash.NewHash(cbTxID)
	if err != nil {
		t.Fatalf("sdk coinbase hash: %v", err)
	}
	gotRoot, err := mp.ComputeRoot(sdkCoinbase)
	if err != nil {
		t.Fatalf("go-sdk ComputeRoot: %v", err)
	}
	if !bytes.Equal(gotRoot[:], headerRoot[:]) {
		t.Fatalf("BUMP folds to %s, want header root %s", gotRoot, &headerRoot)
	}
}

// TestBuildBlockProcessedData_FallsBackToBlockMessage locks backward
// compatibility: when the metadata carries no header (older DataHub payload
// paths), the P2P BlockMessage header is still honored.
func TestBuildBlockProcessedData_FallsBackToBlockMessage(t *testing.T) {
	p := newBlockDataTestProcessor(t)

	root := testHash(7)
	blockMsg := &kafka.BlockMessage{
		Hash:   "live-block",
		Header: buildTestHeaderHex(root),
	}
	meta := &datahub.BlockMetadata{} // no HeaderHex/CoinbaseTxHex

	data := p.buildBlockProcessedData(context.Background(), blockMsg, meta, "http://unused")
	if data.MerkleRoot != root.String() {
		t.Fatalf("MerkleRoot = %q, want %q (message header must be used when meta has none)",
			data.MerkleRoot, root.String())
	}
}

// TestBuildBlockProcessedData_MetaHeaderPreferred locks the precedence: the
// header from the fetched block wins over the message field when both are
// present, since the fetched block is the same source the subtree list came
// from.
func TestBuildBlockProcessedData_MetaHeaderPreferred(t *testing.T) {
	p := newBlockDataTestProcessor(t)

	metaRoot := testHash(1001)
	msgRoot := testHash(2002)
	blockMsg := &kafka.BlockMessage{
		Hash:   "both-sources",
		Header: buildTestHeaderHex(msgRoot),
	}
	meta := &datahub.BlockMetadata{HeaderHex: buildTestHeaderHex(metaRoot)}

	data := p.buildBlockProcessedData(context.Background(), blockMsg, meta, "http://unused")
	if data.MerkleRoot != metaRoot.String() {
		t.Fatalf("MerkleRoot = %q, want meta-derived %q", data.MerkleRoot, metaRoot.String())
	}
}

// teranodeStyleHeaderRoot computes the block merkle root the way teranode
// does (model.Block CheckMerkleRoot): corrected subtree-0 root (real coinbase
// at leaf 0), all middle roots as-is, and the FINAL root lifted to the first
// subtree's height when its subtree is shorter.
func teranodeStyleHeaderRoot(t *testing.T, subtreeNodes [][]subtreepkg.Node, cbTxID chainhash.Hash) chainhash.Hash {
	t.Helper()
	n := len(subtreeNodes)
	tops := make([]subtreepkg.Node, n)
	for i, nodes := range subtreeNodes {
		if i == 0 {
			real := append([]subtreepkg.Node(nil), nodes...)
			real[0] = subtreepkg.Node{Hash: cbTxID}
			tops[0] = subtreepkg.Node{Hash: rootOf(t, real)}
			continue
		}
		root := rootOf(t, nodes)
		if i == n-1 {
			// lift to the first subtree's height
			h0, hLast := 0, 0
			for 1<<h0 < len(subtreeNodes[0]) {
				h0++
			}
			for 1<<hLast < len(nodes) {
				hLast++
			}
			lift := h0 - hLast
			for l := 0; l < lift; l++ {
				buf := append(append([]byte{}, root[:]...), root[:]...)
				copy(root[:], chainhash.DoubleHashB(buf))
			}
		}
		tops[i] = subtreepkg.Node{Hash: root}
	}
	if n == 1 {
		return tops[0].Hash
	}
	return rootOf(t, tops)
}

// TestBuildCoinbaseBUMP_LiftedFinalSubtree_FoldsToHeaderRoot is the
// regression test for the mainnet 954978–956998 proof-corruption bug: blocks
// whose FINAL subtree is shorter than half the first subtree's capacity have
// their final root height-lifted by teranode before top-tree composition.
// The production path (liftedSubtreeRoots + buildCoinbaseSiblings +
// CoinbaseRootFromSiblings) must reproduce the canonical header root for
// these shapes — the pre-fix code did not (all-equal-size shapes only).
func TestBuildCoinbaseBUMP_LiftedFinalSubtree_FoldsToHeaderRoot(t *testing.T) {
	cases := []struct {
		name   string
		shapes []int // leaves per subtree; first must be a power of two
	}{
		{"2subtrees_lift1", []int{8, 3}},         // h3 vs h2 → lift 1
		{"2subtrees_lift2", []int{8, 2}},         // h3 vs h1 → lift 2
		{"2subtrees_lift3", []int{8, 1}},         // h3 vs h0 → lift 3
		{"4subtrees_lift2", []int{8, 8, 8, 2}},   // mirrors block 955117 (…ed8668a5)
		{"2subtrees_mainnet", []int{32, 13}},     // mirrors block 954978 (…2cd6) shape ratio
		{"final_above_half_nolift", []int{8, 5}}, // h3 vs h3 → no lift; must still pass
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := testHash(999_999)
			subtreeNodes := make([][]subtreepkg.Node, len(tc.shapes))
			total := uint64(0)
			for i, leaves := range tc.shapes {
				nodes := make([]subtreepkg.Node, leaves)
				for j := 0; j < leaves; j++ {
					nodes[j] = subtreepkg.Node{Hash: testHash(uint64(i*10_000 + j + 1))}
				}
				if i == 0 {
					nodes[0] = subtreepkg.Node{Hash: testHash(0)} // placeholder
				}
				subtreeNodes[i] = nodes
				total += uint64(leaves)
			}
			headerRoot := teranodeStyleHeaderRoot(t, subtreeNodes, cb)

			roots := make([]chainhash.Hash, len(subtreeNodes))
			for i := range subtreeNodes {
				roots[i] = rootOf(t, subtreeNodes[i])
			}

			// PRODUCTION path: lift, then siblings, then fold.
			topRoots, err := liftedSubtreeRoots(roots, len(subtreeNodes[0]), total)
			if err != nil {
				t.Fatalf("liftedSubtreeRoots: %v", err)
			}
			siblings, err := buildCoinbaseSiblings(subtreeNodes[0], topRoots)
			if err != nil {
				t.Fatalf("buildCoinbaseSiblings: %v", err)
			}
			computed := stump.CoinbaseRootFromSiblings(cb[:], siblings)
			if !bytes.Equal(computed, headerRoot[:]) {
				gotHash, _ := chainhash.NewHash(computed)
				t.Fatalf("folded root = %s, want canonical header root %s", gotHash, headerRoot)
			}

			// The encoded BUMP must parse with go-sdk and compute the same root.
			encoded := stump.EncodeCoinbaseBUMP(123, cb[:], siblings)
			mp, err := sdktx.NewMerklePathFromBinary(encoded)
			if err != nil {
				t.Fatalf("go-sdk parse: %v", err)
			}
			sdkCb, _ := sdkchainhash.NewHash(cb[:])
			gotRoot, err := mp.ComputeRoot(sdkCb)
			if err != nil {
				t.Fatalf("go-sdk ComputeRoot: %v", err)
			}
			if !bytes.Equal(gotRoot[:], headerRoot[:]) {
				t.Fatalf("go-sdk computed root = %s, want %s", gotRoot, &headerRoot)
			}

			// And WITHOUT the lift the old behavior must fail for lift>0 shapes
			// (guards against the test silently passing for trivial reasons).
			h0, hLast := 0, 0
			for 1<<h0 < tc.shapes[0] {
				h0++
			}
			for 1<<hLast < tc.shapes[len(tc.shapes)-1] {
				hLast++
			}
			if h0 > hLast && len(tc.shapes) > 1 {
				rawSiblings, err := buildCoinbaseSiblings(subtreeNodes[0], roots)
				if err != nil {
					t.Fatalf("buildCoinbaseSiblings(raw): %v", err)
				}
				if bytes.Equal(stump.CoinbaseRootFromSiblings(cb[:], rawSiblings), headerRoot[:]) {
					t.Fatal("un-lifted fold unexpectedly matched — test shape does not exercise the lift")
				}
			}
		})
	}
}

// TestLiftedSubtreeRoots_Guards locks the failure modes: inconsistent shapes
// must error (not silently produce wrong roots).
func TestLiftedSubtreeRoots_Guards(t *testing.T) {
	r := []chainhash.Hash{testHash(1), testHash(2)}
	if _, err := liftedSubtreeRoots(r, 6, 9); err == nil {
		t.Error("non-power-of-two first subtree must error")
	}
	if _, err := liftedSubtreeRoots(r, 8, 8); err == nil {
		t.Error("txCount implying empty final subtree must error")
	}
	if _, err := liftedSubtreeRoots(r, 8, 99); err == nil {
		t.Error("txCount implying oversized final subtree must error")
	}
	single := []chainhash.Hash{testHash(1)}
	if out, err := liftedSubtreeRoots(single, 7, 7); err != nil || len(out) != 1 {
		t.Errorf("single subtree must pass through unchanged: %v", err)
	}
}

// buildTailFixture builds a single-subtree block scenario with a VALID
// ready-made coinbase BUMP (what teranode ships in the block binary tail).
func buildTailFixture(t *testing.T, height uint32) (meta *datahub.BlockMetadata, headerHex string, tailHex string) {
	t.Helper()
	coinbaseRaw, err := hex.DecodeString(genesisCoinbaseHex)
	if err != nil {
		t.Fatalf("decode coinbase: %v", err)
	}
	cbTxID := chainhash.DoubleHashB(coinbaseRaw)

	nodes := []subtreepkg.Node{
		{Hash: testHash(0)}, // placeholder
		{Hash: testHash(1)},
		{Hash: testHash(2)},
		{Hash: testHash(3)},
	}
	placeholderRoot := rootOf(t, nodes)

	real := append([]subtreepkg.Node(nil), nodes...)
	var cbh chainhash.Hash
	copy(cbh[:], cbTxID)
	real[0] = subtreepkg.Node{Hash: cbh}
	headerRoot := rootOf(t, real) // single subtree: corrected root IS the header root

	siblings, err := buildCoinbaseSiblings(nodes, []chainhash.Hash{placeholderRoot})
	if err != nil {
		t.Fatalf("siblings: %v", err)
	}
	tail := stump.EncodeCoinbaseBUMP(uint64(height), cbTxID, siblings)

	meta = &datahub.BlockMetadata{
		Height:          height,
		Subtrees:        []string{placeholderRoot.String()},
		CoinbaseTxHex:   genesisCoinbaseHex,
		CoinbaseBUMPHex: hex.EncodeToString(tail),
	}
	return meta, buildTestHeaderHex(headerRoot), hex.EncodeToString(tail)
}

// TestBuildBlockProcessedData_PrefersValidUpstreamCoinbaseBUMP: when the
// block binary carries a valid ready-made coinbase BUMP, it is used verbatim
// and NO subtree fetch happens — this is what keeps blocks recoverable after
// every peer has pruned their subtree data. The processor is constructed
// WITHOUT a datahub client: any fetch attempt would panic the test.
func TestBuildBlockProcessedData_PrefersValidUpstreamCoinbaseBUMP(t *testing.T) {
	p := newBlockDataTestProcessor(t) // no dataHubClient wired

	meta, headerHex, tailHex := buildTailFixture(t, 954978)
	meta.HeaderHex = headerHex
	blockMsg := &kafka.BlockMessage{Hash: "tail-block"}

	data := p.buildBlockProcessedData(context.Background(), blockMsg, meta, "http://unused")
	if data.CoinbaseBUMP != tailHex {
		t.Fatalf("expected the upstream coinbase BUMP to be used verbatim")
	}
	if data.MerkleRoot == "" {
		t.Fatal("merkle root must still be populated")
	}
}

// TestBuildBlockProcessedData_FallsBackWhenUpstreamBUMPInvalid: an upstream
// BUMP that fails validation (here: wrong block height) must not be trusted —
// the builder falls back to local reconstruction and still produces a valid
// coinbase BUMP.
func TestBuildBlockProcessedData_FallsBackWhenUpstreamBUMPInvalid(t *testing.T) {
	p := newBlockDataTestProcessor(t)

	meta, headerHex, tailHex := buildTailFixture(t, 954978)
	meta.HeaderHex = headerHex
	meta.Height = 954979 // tail encodes 954978 → height mismatch → invalid

	// Reconstruction path needs subtree 0 from the datahub.
	var subtreeRaw []byte
	nodes := []subtreepkg.Node{{Hash: testHash(0)}, {Hash: testHash(1)}, {Hash: testHash(2)}, {Hash: testHash(3)}}
	for _, n := range nodes {
		subtreeRaw = append(subtreeRaw, n.Hash[:]...)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/subtree/") {
			_, _ = w.Write(subtreeRaw)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	p.dataHubClient = datahub.NewClient(5, 0, p.Logger)
	meta.TransactionCount = 4

	blockMsg := &kafka.BlockMessage{Hash: "tail-block-bad"}
	data := p.buildBlockProcessedData(context.Background(), blockMsg, meta, server.URL)
	if data.CoinbaseBUMP == "" {
		t.Fatal("expected reconstruction fallback to produce a coinbase BUMP")
	}
	if data.CoinbaseBUMP == tailHex {
		t.Fatal("invalid upstream BUMP must not be used verbatim")
	}
}
