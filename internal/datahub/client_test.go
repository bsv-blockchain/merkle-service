package datahub

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// buildBinaryBlockBytes creates a teranode model.Block binary payload.
// height is the block height, hashes is a slice of 32-byte subtree hashes.
func buildBinaryBlockBytes(height uint32, hashes [][]byte) []byte {
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}

	subtrees := make([]*chainhash.Hash, len(hashes))
	for i, h := range hashes {
		hash := &chainhash.Hash{}
		copy(hash[:], h)
		subtrees[i] = hash
	}

	block, err := model.NewBlock(header, nil, subtrees, 0, 0, height, 0)
	if err != nil {
		panic("buildBinaryBlockBytes NewBlock: " + err.Error())
	}

	data, err := block.Bytes()
	if err != nil {
		panic("buildBinaryBlockBytes Bytes: " + err.Error())
	}

	return data
}

// buildRawSubtreeBytes creates DataHub-format raw subtree data (concatenated 32-byte hashes).
func buildRawSubtreeBytes(n int) []byte {
	data := make([]byte, n*32)
	for i := 0; i < n; i++ {
		data[i*32] = byte(i + 1)
	}
	return data
}

func TestFetchSubtree_Success(t *testing.T) {
	// Build raw DataHub-format subtree data with 2 nodes.
	subtreeBytes := buildRawSubtreeBytes(2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/subtree/") {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(subtreeBytes)
	}))
	defer server.Close()

	client := NewClient(5, 0, testLogger())
	result, err := client.FetchSubtree(context.Background(), server.URL, "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil subtree")
	}
	if len(result.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(result.Nodes))
	}
	if result.Nodes[0].Hash[0] != 1 {
		t.Errorf("expected first node hash[0]=1, got %d", result.Nodes[0].Hash[0])
	}
}

func TestFetchSubtreeRaw_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(5, 0, testLogger())
	_, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc123")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected 404 in error, got: %v", err)
	}
}

func TestFetchBlockMetadata_Success(t *testing.T) {
	// Build binary payload: height=100, 3 subtree hashes.
	hashes := [][]byte{
		append([]byte{0x01}, make([]byte, 31)...),
		append([]byte{0x02}, make([]byte, 31)...),
		append([]byte{0x03}, make([]byte, 31)...),
	}
	payload := buildBinaryBlockBytes(100, hashes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/block/") {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Ensure the path does NOT end in /json.
		if strings.HasSuffix(r.URL.Path, "/json") {
			t.Errorf("expected binary endpoint, got JSON path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(payload)
	}))
	defer server.Close()

	client := NewClient(5, 0, testLogger())
	result, err := client.FetchBlockMetadata(context.Background(), server.URL, "blockhash")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Height != 100 {
		t.Errorf("expected height 100, got %d", result.Height)
	}
	if len(result.Subtrees) != 3 {
		t.Errorf("expected 3 subtrees, got %d", len(result.Subtrees))
	}
}

func TestParseBinaryBlockMetadata_Success(t *testing.T) {
	hashes := [][]byte{
		append([]byte{0xAA}, make([]byte, 31)...),
		append([]byte{0xBB}, make([]byte, 31)...),
		append([]byte{0xCC}, make([]byte, 31)...),
	}
	payload := buildBinaryBlockBytes(12345, hashes)

	meta, err := ParseBinaryBlockMetadata(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.Height != 12345 {
		t.Errorf("expected height 12345, got %d", meta.Height)
	}
	if len(meta.Subtrees) != 3 {
		t.Fatalf("expected 3 subtrees, got %d", len(meta.Subtrees))
	}
	// Each subtree should be a 64-char hex string.
	for i, s := range meta.Subtrees {
		if len(s) != 64 {
			t.Errorf("subtree %d: expected 64-char hex, got %d chars", i, len(s))
		}
	}
}

func TestParseBinaryBlockMetadata_EmptySubtrees(t *testing.T) {
	payload := buildBinaryBlockBytes(42, nil)

	meta, err := ParseBinaryBlockMetadata(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.Height != 42 {
		t.Errorf("expected height 42, got %d", meta.Height)
	}
	if len(meta.Subtrees) != 0 {
		t.Errorf("expected empty subtrees, got %d", len(meta.Subtrees))
	}
}

func TestParseBinaryBlockMetadata_TooShort(t *testing.T) {
	_, err := ParseBinaryBlockMetadata([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected error for payload too small to be a block")
	}
}

func TestParseBinaryBlockMetadata_Truncated(t *testing.T) {
	// Build a valid block binary and truncate by one byte to trigger a parse error.
	full := buildBinaryBlockBytes(100, [][]byte{make([]byte, 32)})
	_, err := ParseBinaryBlockMetadata(full[:len(full)-1])
	if err == nil {
		t.Fatal("expected error for truncated block data")
	}
}

func TestFetchSubtreeRaw_RetryOnServerError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("error"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := NewClient(5, 3, testLogger()) // 3 retries
	data, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc123")
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if string(data) != "ok" {
		t.Errorf("expected 'ok', got %q", string(data))
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestParseRawTxids(t *testing.T) {
	raw := buildRawSubtreeBytes(3)
	txids, err := ParseRawTxids(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(txids) != 3 {
		t.Fatalf("expected 3 txids, got %d", len(txids))
	}
	if len(txids[0]) != 64 {
		t.Errorf("expected 64-char hex, got %d", len(txids[0]))
	}
	// Byte[0]=1, rest zeros. Bitcoin display order reverses: "00...0001"
	if !strings.HasSuffix(txids[0], "01") {
		t.Errorf("expected reversed byte order (suffix '01'), got %s", txids[0])
	}
}

func TestParseRawTxids_InvalidLength(t *testing.T) {
	_, err := ParseRawTxids([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for non-multiple-of-32")
	}
}

func TestParseRawNodes(t *testing.T) {
	raw := buildRawSubtreeBytes(4)
	nodes, err := ParseRawNodes(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(nodes))
	}
	for i, node := range nodes {
		if node.Hash[0] != byte(i+1) {
			t.Errorf("node %d: expected hash[0]=%d, got %d", i, i+1, node.Hash[0])
		}
	}
}
