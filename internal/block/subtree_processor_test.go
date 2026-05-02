package block

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	"github.com/bsv-blockchain/merkle-service/internal/stump"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// buildRawSubtreeBytes creates raw DataHub-format subtree data (concatenated 32-byte hashes).
func buildRawSubtreeBytes(t *testing.T, n int) []byte {
	t.Helper()
	data := make([]byte, n*32)
	for i := 0; i < n; i++ {
		data[i*32] = byte(i + 1) // unique hash per node
	}
	return data
}

// buildNodesForMerkle creates subtree Nodes for merkle tree building.
func buildNodesForMerkle(t *testing.T, n int) []subtreepkg.Node {
	t.Helper()
	nodes := make([]subtreepkg.Node, n)
	for i := 0; i < n; i++ {
		nodes[i].Hash[0] = byte(i + 1)
	}
	return nodes
}

// TestParseRawTxids verifies parsing of DataHub raw hash format.
// ParseRawTxids must produce txids in Bitcoin display order (reversed bytes),
// matching chainhash.Hash.String() and the format used in registration storage.
func TestParseRawTxids(t *testing.T) {
	raw := buildRawSubtreeBytes(t, 4)
	txids, err := datahub.ParseRawTxids(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(txids) != 4 {
		t.Fatalf("expected 4 txids, got %d", len(txids))
	}
	// First hash has byte[0]=1, rest zeros.
	// In Bitcoin display order (reversed), byte[0]=1 appears at the END: "00...0001"
	if !strings.HasSuffix(txids[0], "01") {
		t.Errorf("expected first txid to end with '01' (reversed byte order), got %s", txids[0])
	}
	if len(txids[0]) != 64 {
		t.Errorf("expected 64-char hex txid, got %d chars", len(txids[0]))
	}
}

// TestParseRawTxids_InvalidLength verifies error on non-multiple-of-32 data.
func TestParseRawTxids_InvalidLength(t *testing.T) {
	_, err := datahub.ParseRawTxids([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
}

// TestParseRawNodes verifies parsing into Node structs.
func TestParseRawNodes(t *testing.T) {
	raw := buildRawSubtreeBytes(t, 2)
	nodes, err := datahub.ParseRawNodes(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].Hash[0] != 1 {
		t.Errorf("expected first node hash[0]=1, got %d", nodes[0].Hash[0])
	}
	if nodes[1].Hash[0] != 2 {
		t.Errorf("expected second node hash[0]=2, got %d", nodes[1].Hash[0])
	}
}

// TestSubtreeDataRetrieval_BlobStore verifies subtree data is retrieved from blob store.
func TestSubtreeDataRetrieval_BlobStore(t *testing.T) {
	rawBytes := buildRawSubtreeBytes(t, 4)

	blobStore := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blobStore, 1, testLogger())

	// Pre-store the raw subtree data (as DataHub would serve it).
	if err := subtreeStore.StoreSubtree("st-123", rawBytes, 0); err != nil {
		t.Fatalf("failed to store subtree: %v", err)
	}

	// Retrieve from blob store.
	data, err := subtreeStore.GetSubtree("st-123")
	if err != nil {
		t.Fatalf("failed to get subtree: %v", err)
	}

	// Parse and verify using the raw format parser.
	txids, err := datahub.ParseRawTxids(data)
	if err != nil {
		t.Fatalf("failed to parse raw txids: %v", err)
	}
	if len(txids) != 4 {
		t.Errorf("expected 4 txids, got %d", len(txids))
	}
}

// TestSubtreeDataRetrieval_DataHubFallback verifies fallback to DataHub when blob store is empty.
func TestSubtreeDataRetrieval_DataHubFallback(t *testing.T) {
	rawBytes := buildRawSubtreeBytes(t, 2)

	fetchCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/subtree/") {
			fetchCount++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(rawBytes)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	blobStore := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blobStore, 1, testLogger())
	dhClient := datahub.NewClient(5, 0, testLogger())

	// Blob store is empty — should fallback to DataHub.
	_, getErr := subtreeStore.GetSubtree("st-new")
	if getErr == nil {
		t.Fatal("expected error from empty blob store")
	}

	// Fetch from DataHub (as ProcessBlockSubtree would).
	rawData, err := dhClient.FetchSubtreeRaw(context.Background(), server.URL, "st-new")
	if err != nil {
		t.Fatalf("failed to fetch from DataHub: %v", err)
	}
	if fetchCount != 1 {
		t.Errorf("expected 1 DataHub fetch, got %d", fetchCount)
	}

	// Store in blob store for future use.
	if err = subtreeStore.StoreSubtree("st-new", rawData, 100); err != nil {
		t.Fatalf("failed to store subtree: %v", err)
	}

	// Verify it's now in blob store and parseable.
	data, err := subtreeStore.GetSubtree("st-new")
	if err != nil {
		t.Fatalf("subtree should now be in blob store: %v", err)
	}
	txids, err := datahub.ParseRawTxids(data)
	if err != nil {
		t.Fatalf("failed to parse stored data: %v", err)
	}
	if len(txids) != 2 {
		t.Errorf("expected 2 txids, got %d", len(txids))
	}
}

// TestMerkleTreeAndSTUMPBuild verifies the full STUMP build pipeline.
func TestMerkleTreeAndSTUMPBuild(t *testing.T) {
	nodes := buildNodesForMerkle(t, 4)

	// Build merkle tree from nodes.
	merkleTreeStore, err := subtreepkg.BuildMerkleTreeStoreFromBytes(nodes)
	if err != nil {
		t.Fatalf("failed to build merkle tree: %v", err)
	}
	if len(*merkleTreeStore) == 0 {
		t.Fatal("merkle tree should not be empty")
	}

	// Build leaves and internal nodes separately.
	leaves := make([][]byte, len(nodes))
	for i, node := range nodes {
		hashCopy := make([]byte, 32)
		copy(hashCopy, node.Hash[:])
		leaves[i] = hashCopy
	}

	internalNodes := make([][]byte, len(*merkleTreeStore))
	for i, h := range *merkleTreeStore {
		hashCopy := make([]byte, 32)
		copy(hashCopy, h[:])
		internalNodes[i] = hashCopy
	}

	// Register leaf 0 and leaf 2.
	registeredIndices := map[int]string{
		0: nodes[0].Hash.String(),
		2: nodes[2].Hash.String(),
	}

	s := stump.Build(100, leaves, internalNodes, registeredIndices)
	if s == nil {
		t.Fatal("STUMP should not be nil")
	}
	if s.BlockHeight != 100 {
		t.Errorf("expected block height 100, got %d", s.BlockHeight)
	}

	// Encode STUMP.
	encoded := s.Encode()
	if len(encoded) == 0 {
		t.Fatal("encoded STUMP should not be empty")
	}
}

// TestGroupByCallback verifies txid grouping by callback URL.
func TestGroupByCallback(t *testing.T) {
	registrations := map[string][]string{
		"txid1": {"http://cb1.com", "http://cb2.com"},
		"txid2": {"http://cb1.com"},
		"txid3": {"http://cb2.com", "http://cb3.com"},
	}

	groups := stump.GroupByCallback(registrations)

	if len(groups["http://cb1.com"]) != 2 {
		t.Errorf("expected 2 txids for cb1, got %d", len(groups["http://cb1.com"]))
	}
	if len(groups["http://cb2.com"]) != 2 {
		t.Errorf("expected 2 txids for cb2, got %d", len(groups["http://cb2.com"]))
	}
	if len(groups["http://cb3.com"]) != 1 {
		t.Errorf("expected 1 txid for cb3, got %d", len(groups["http://cb3.com"]))
	}
}

// buildBlockPayload creates a valid model.Block binary payload for testing.
func buildBlockPayload(height uint32, hashCount int) []byte {
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
	subtrees := make([]*chainhash.Hash, hashCount)
	for i := range subtrees {
		h := &chainhash.Hash{}
		h[0] = byte(i + 1)
		subtrees[i] = h
	}
	block, err := model.NewBlock(header, nil, subtrees, 0, 0, height, 0)
	if err != nil {
		panic("buildBlockPayload: " + err.Error())
	}
	data, err := block.Bytes()
	if err != nil {
		panic("buildBlockPayload Bytes: " + err.Error())
	}
	return data
}

// TestBlockMetadataFetch verifies DataHub block metadata fetching with multiple subtrees.
func TestBlockMetadataFetch(t *testing.T) {
	payload := buildBlockPayload(200, 3)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/block/") && !strings.HasSuffix(r.URL.Path, "/json") {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(payload)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	dhClient := datahub.NewClient(5, 0, testLogger())
	meta, err := dhClient.FetchBlockMetadata(context.Background(), server.URL, "block-hash-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if meta.Height != 200 {
		t.Errorf("expected height 200, got %d", meta.Height)
	}
	if len(meta.Subtrees) != 3 {
		t.Errorf("expected 3 subtrees, got %d", len(meta.Subtrees))
	}
}

// TestStumpCallbackMessageEncoding verifies STUMP callback messages are properly encoded/decoded.
func TestStumpCallbackMessageEncoding(t *testing.T) {
	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  "http://example.com/callback",
		Type:         kafka.CallbackStump,
		TxID:         "txid1",
		BlockHash:    "blockhash123",
		SubtreeIndex: 3,
		StumpRef:     "deadbeef",
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := kafka.DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type != kafka.CallbackStump {
		t.Errorf("expected STUMP, got %s", decoded.Type)
	}
	if decoded.CallbackURL != "http://example.com/callback" {
		t.Errorf("unexpected callback URL: %s", decoded.CallbackURL)
	}
	if decoded.TxID != "txid1" {
		t.Errorf("expected txid1, got %s", decoded.TxID)
	}
	if decoded.BlockHash != "blockhash123" {
		t.Errorf("unexpected block hash: %s", decoded.BlockHash)
	}
	if decoded.SubtreeIndex != 3 {
		t.Errorf("expected subtreeIndex 3, got %d", decoded.SubtreeIndex)
	}
}

// --- Block Dedup Cache Tests ---

// TestBlockDedupCache_SkipsDuplicateBlock verifies that the dedup cache
// prevents reprocessing of already-seen block hashes.
func TestBlockDedupCache_SkipsDuplicateBlock(t *testing.T) {
	dc := cache.NewDedupCache(100)

	// First time: not in cache.
	if dc.Contains("block-hash-abc") {
		t.Error("expected block hash not in cache initially")
	}

	// Simulate successful processing.
	dc.Add("block-hash-abc")

	// Second time: in cache, should be skipped.
	if !dc.Contains("block-hash-abc") {
		t.Error("expected block hash in cache after processing")
	}
}

// TestBlockDedupCache_AllowsRetryOnFailure verifies that if block processing
// fails (Add is never called), the same block hash can be retried.
func TestBlockDedupCache_AllowsRetryOnFailure(t *testing.T) {
	dc := cache.NewDedupCache(100)

	// Simulate: block message received, processing fails (no Add called).
	if dc.Contains("block-hash-fail") {
		t.Error("should not be in cache")
	}

	// Don't call Add (simulating failure — e.g., DataHub unreachable).

	// Retry: should still not be in cache, allowing retry.
	if dc.Contains("block-hash-fail") {
		t.Error("failed block processing should not add to cache")
	}

	// Now simulate successful retry.
	dc.Add("block-hash-fail")
	if !dc.Contains("block-hash-fail") {
		t.Error("expected block hash in cache after successful retry")
	}
}

// TestBlockDedupCache_MultipleBlocksIndependent verifies that different block
// hashes are tracked independently.
func TestBlockDedupCache_MultipleBlocksIndependent(t *testing.T) {
	dc := cache.NewDedupCache(100)

	dc.Add("block-1")
	dc.Add("block-2")

	if !dc.Contains("block-1") {
		t.Error("block-1 should be in cache")
	}
	if !dc.Contains("block-2") {
		t.Error("block-2 should be in cache")
	}
	if dc.Contains("block-3") {
		t.Error("block-3 should NOT be in cache")
	}
}

// TestSeenOnNetworkCallbackMessage verifies SEEN_ON_NETWORK message encoding.
func TestSeenOnNetworkCallbackMessage(t *testing.T) {
	msg := &kafka.CallbackTopicMessage{
		CallbackURL: "http://example.com/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "abc123",
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := kafka.DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type != kafka.CallbackSeenOnNetwork {
		t.Errorf("expected SEEN_ON_NETWORK, got %s", decoded.Type)
	}
	if decoded.TxID != "abc123" {
		t.Errorf("unexpected txid: %s", decoded.TxID)
	}
}
