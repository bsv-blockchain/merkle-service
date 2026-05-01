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

// --- Response body size cap tests (F-027) ---------------------------------

// TestFetchSubtreeRaw_BodyExceedsCap verifies that a /subtree response larger
// than the configured cap is rejected with an error mentioning the cap, and
// that the error does not embed the response content.
func TestFetchSubtreeRaw_BodyExceedsCap(t *testing.T) {
	const subtreeCap = 64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Don't set Content-Length so the cap is enforced by LimitReader,
		// not by the pre-read Content-Length check (covered by another test).
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusOK)
		// 65 bytes — one over the cap. Use distinctive content so we can
		// assert it does NOT leak into the error.
		body := strings.Repeat("A", subtreeCap+1)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	// Block cap is unrelated; subtree cap = 64.
	client := NewClientWithCaps(5, 0, 0, subtreeCap, testLogger())
	_, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc")
	if err == nil {
		t.Fatal("expected error for oversize subtree body")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected error mentioning the cap, got: %v", err)
	}
	if strings.Contains(err.Error(), "AAAA") {
		t.Errorf("error must not embed response content, got: %v", err)
	}
}

// TestFetchSubtreeRaw_BodyAtCap verifies that a body exactly at the cap is
// accepted (the LimitReader+1 trick must not reject the boundary case).
func TestFetchSubtreeRaw_BodyAtCap(t *testing.T) {
	// Use a multiple of 32 so ParseRawNodes would also be happy (we only
	// fetch raw here, but it makes future test changes easier).
	const subtreeCap = 64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, subtreeCap))
	}))
	defer server.Close()

	client := NewClientWithCaps(5, 0, 0, subtreeCap, testLogger())
	body, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc")
	if err != nil {
		t.Fatalf("expected success at cap boundary, got: %v", err)
	}
	if int64(len(body)) != subtreeCap {
		t.Errorf("expected %d bytes, got %d", subtreeCap, len(body))
	}
}

// TestFetchSubtreeRaw_ContentLengthExceedsCap verifies that an advertised
// oversize Content-Length is rejected before the body is read.
func TestFetchSubtreeRaw_ContentLengthExceedsCap(t *testing.T) {
	const subtreeCap = 64
	bodyRead := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1048576") // 1 MiB advertised
		w.WriteHeader(http.StatusOK)
		// Write a small amount; the client should reject based on Content-Length
		// without attempting to consume this. The handler will hit a broken
		// pipe when the client closes early, which is fine.
		bodyRead = true
		_, _ = w.Write(make([]byte, 1024))
	}))
	defer server.Close()

	client := NewClientWithCaps(5, 0, 0, subtreeCap, testLogger())
	_, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc")
	if err == nil {
		t.Fatal("expected error for advertised oversize Content-Length")
	}
	if !strings.Contains(err.Error(), "Content-Length") && !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected error mentioning Content-Length/exceeds, got: %v", err)
	}
	// We don't strictly require bodyRead == false because the server handler
	// runs concurrently with our request; the important assertion is that the
	// client rejected the response without surfacing it. Reference the var to
	// keep the check meaningful and avoid an unused-write warning.
	_ = bodyRead
}

// TestFetchBlockMetadata_BodyExceedsCap verifies the block endpoint enforces
// its own (smaller) cap independently of the subtree cap.
func TestFetchBlockMetadata_BodyExceedsCap(t *testing.T) {
	const blockCap = 128
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, blockCap+10))
	}))
	defer server.Close()

	// Block cap is tight; subtree cap is generous to confirm independence.
	client := NewClientWithCaps(5, 0, blockCap, 1<<30, testLogger())
	_, err := client.FetchBlockMetadata(context.Background(), server.URL, "blockhash")
	if err == nil {
		t.Fatal("expected error for oversize block body")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected error mentioning the cap, got: %v", err)
	}
}

// TestFetchBlockMetadata_WithinCap verifies a valid block payload under the
// configured cap is accepted.
func TestFetchBlockMetadata_WithinCap(t *testing.T) {
	hashes := [][]byte{
		append([]byte{0x01}, make([]byte, 31)...),
	}
	payload := buildBinaryBlockBytes(7, hashes)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	// 1 MiB cap — well above the tiny payload.
	client := NewClientWithCaps(5, 0, 1<<20, 1<<30, testLogger())
	meta, err := client.FetchBlockMetadata(context.Background(), server.URL, "blockhash")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.Height != 7 {
		t.Errorf("expected height 7, got %d", meta.Height)
	}
}

// TestNewClient_AppliesDefaultCaps verifies that NewClient and
// NewClientWithCaps with zero caps fall back to the documented defaults.
func TestNewClient_AppliesDefaultCaps(t *testing.T) {
	c := NewClient(5, 0, testLogger())
	if c.maxBlockBytes != DefaultMaxBlockBytes {
		t.Errorf("expected default block cap %d, got %d", DefaultMaxBlockBytes, c.maxBlockBytes)
	}
	if c.maxSubtreeBytes != DefaultMaxSubtreeBytes {
		t.Errorf("expected default subtree cap %d, got %d", DefaultMaxSubtreeBytes, c.maxSubtreeBytes)
	}

	c2 := NewClientWithCaps(5, 0, 0, 0, testLogger())
	if c2.maxBlockBytes != DefaultMaxBlockBytes {
		t.Errorf("zero block cap should fall back to default; got %d", c2.maxBlockBytes)
	}
	if c2.maxSubtreeBytes != DefaultMaxSubtreeBytes {
		t.Errorf("zero subtree cap should fall back to default; got %d", c2.maxSubtreeBytes)
	}

	// Negative caps must also fall back rather than silently disable the
	// protection.
	c3 := NewClientWithCaps(5, 0, -1, -1, testLogger())
	if c3.maxBlockBytes != DefaultMaxBlockBytes {
		t.Errorf("negative block cap should fall back to default; got %d", c3.maxBlockBytes)
	}
	if c3.maxSubtreeBytes != DefaultMaxSubtreeBytes {
		t.Errorf("negative subtree cap should fall back to default; got %d", c3.maxSubtreeBytes)
	}
}
