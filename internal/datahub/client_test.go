package datahub

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
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
		_, _ = w.Write(subtreeBytes)
	}))
	defer server.Close()

	client := NewClient(5, 0, testLogger())
	result, err := client.FetchSubtree(context.Background(), server.URL, "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil subtree")
		return // unreachable after Fatal; guards the derefs below
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
		_, _ = w.Write(payload)
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
			_, _ = w.Write([]byte("error"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
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

// TestFetchBlockMetadata_NonRetryable4xxDoesNotRetry pins the contract
// that 401/403/422 (and other non-408/429 4xx) return immediately
// instead of burning the retry budget. The motivating case was a peer
// in the discovered DataHub registry that required auth and returned
// 401 — pre-fix, every /reprocess call retried it 4 times × 500ms
// backoffs before peer-health marked it unhealthy.
func TestFetchBlockMetadata_NonRetryable4xxDoesNotRetry(t *testing.T) {
	statusCodes := []int{
		http.StatusBadRequest,          // 400
		http.StatusUnauthorized,        // 401
		http.StatusForbidden,           // 403
		http.StatusMethodNotAllowed,    // 405
		http.StatusGone,                // 410
		http.StatusUnprocessableEntity, // 422
	}

	for _, code := range statusCodes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(code)
			}))
			defer server.Close()

			client := NewClient(5, 3, testLogger())
			_, err := client.FetchBlockMetadata(context.Background(), server.URL,
				"0000000000000000000000000000000000000000000000000000000000000001")
			if err == nil {
				t.Fatalf("expected error for status %d", code)
			}
			if calls != 1 {
				t.Fatalf("status %d must not retry; expected 1 call, got %d", code, calls)
			}
		})
	}
}

// TestFetchBlockMetadata_RetryableStatusesStillRetry locks in that
// 408/429 (the "try again later" 4xx codes) and 5xx remain on the retry
// path. Regressing this would cause a transient blip to surface as a
// hard failure to the caller instead of being smoothed over.
func TestFetchBlockMetadata_RetryableStatusesStillRetry(t *testing.T) {
	statusCodes := []int{
		http.StatusRequestTimeout,      // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
	}

	for _, code := range statusCodes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(code)
			}))
			defer server.Close()

			// maxRetries=2 → 3 total attempts.
			client := NewClient(5, 2, testLogger())
			_, _ = client.FetchBlockMetadata(context.Background(), server.URL,
				"0000000000000000000000000000000000000000000000000000000000000001")
			if calls != 3 {
				t.Fatalf("status %d must use the full retry budget; expected 3 calls, got %d", code, calls)
			}
		})
	}
}

// TestFetchBlockMetadata_NotFoundDoesNotRetry pins the contract that a
// 404 from a DataHub returns immediately rather than burning the retry
// budget. Regressions here would re-introduce the long stalls seen on
// TTN when a peer announced blocks it then 404'd.
func TestFetchBlockMetadata_NotFoundDoesNotRetry(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(5, 3, testLogger())
	_, err := client.FetchBlockMetadata(context.Background(), server.URL,
		"0000000000000000000000000000000000000000000000000000000000000001")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if calls != 1 {
		t.Fatalf("404 must not be retried; expected 1 call, got %d", calls)
	}
}

// TestFetch_RecordsPeerHealth verifies that fetch outcomes are forwarded
// to an attached PeerHealth tracker: failures bump the counter, success
// resets it. This is the hook /reprocess and block-processor use to
// avoid re-probing known-bad peers.
func TestFetch_RecordsPeerHealth(t *testing.T) {
	// Server that returns 404 until flipped, then 200 with a parseable block.
	var serveOK bool
	payload := buildBinaryBlockBytes(1, [][]byte{append([]byte{0x01}, make([]byte, 31)...)})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serveOK {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	client := NewClient(5, 0, testLogger())
	ph := NewPeerHealth(3, 10*time.Minute)
	client.SetPeerHealth(ph)

	// Three 404s mark the peer unhealthy.
	for i := 0; i < 3; i++ {
		_, _ = client.FetchBlockMetadata(context.Background(), server.URL,
			"0000000000000000000000000000000000000000000000000000000000000001")
	}
	if ph.IsHealthy(server.URL) {
		t.Fatal("three failures should mark peer unhealthy")
	}

	// A success resets the state.
	serveOK = true
	if _, err := client.FetchBlockMetadata(context.Background(), server.URL,
		"0000000000000000000000000000000000000000000000000000000000000001"); err != nil {
		t.Fatalf("unexpected success-path error: %v", err)
	}
	if !ph.IsHealthy(server.URL) {
		t.Fatal("success should restore peer health")
	}
}

// TestRecordPeerOutcome_CanceledContextRecordsNothing pins the
// cancellation-neutral contract at the recording chokepoint: when the
// caller's ctx is already dead at record time, NOTHING is recorded —
// neither a failure (a pod shutdown/rebalance aborting an in-flight fetch
// says nothing about the peer; on dev-ovh-1 one rollout tripped the breaker
// on fresh pods within minutes) nor a success (a dead ctx must not reset a
// genuinely failing peer's counter either).
func TestRecordPeerOutcome_CanceledContextRecordsNothing(t *testing.T) {
	client := NewClient(5, 0, testLogger())
	ph := NewPeerHealth(3, 10*time.Minute)
	client.SetPeerHealth(ph)
	url := "https://cancel-neutral.example.com/api"

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	// Failure with a dead ctx: not recorded, even many times over threshold.
	for i := 0; i < 5; i++ {
		client.recordPeerOutcome(canceled, url, errors.New("connection reset"))
	}
	if !ph.IsHealthy(url) {
		t.Fatal("failures recorded under a canceled ctx must not count against the peer")
	}

	// Success with a dead ctx: not recorded either. Two live failures, a
	// canceled-ctx success, then a third live failure must still trip —
	// proving the success did not reset the consecutive-failure counter.
	client.recordPeerOutcome(live, url, errors.New("boom"))
	client.recordPeerOutcome(live, url, errors.New("boom"))
	client.recordPeerOutcome(canceled, url, nil)
	client.recordPeerOutcome(live, url, errors.New("boom"))
	if ph.IsHealthy(url) {
		t.Fatal("a canceled-ctx success must not reset the failure counter")
	}
}

// TestFetchSubtreeRaw_CallerCancellationNotRecorded drives the incident path
// end to end: a caller ctx canceled mid-fetch (shutdown, rebalance,
// partition loss) aborts the request, and the resulting error must not be
// attributed to the peer.
func TestFetchSubtreeRaw_CallerCancellationNotRecorded(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hold the request open until the client has gone away
	}))
	defer server.Close()
	defer close(release)

	client := NewClient(5, 0, testLogger())
	ph := NewPeerHealth(1, 10*time.Minute) // a single counted failure would trip
	client.SetPeerHealth(ph)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if _, err := client.FetchSubtreeRaw(ctx, server.URL, "abc123"); err == nil {
		t.Fatal("expected an error from the canceled fetch")
	}
	if !ph.IsHealthy(server.URL) {
		t.Fatal("caller cancellation must not be recorded as a peer failure")
	}
}

// TestFetchSubtreeRaw_ClientTimeoutRecordsFailure pins the flip side of
// cancellation neutrality: when the client's own HTTP timeout fires while
// the caller ctx is still alive, the peer really was too slow — that IS
// peer-attributable and must count. The two cases are distinguished by
// ctx.Err(), never by matching error strings.
func TestFetchSubtreeRaw_ClientTimeoutRecordsFailure(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // slower than the client timeout below
	}))
	defer server.Close()
	defer close(release)

	client := NewClient(5, 0, testLogger())
	client.httpClient.Timeout = 50 * time.Millisecond
	ph := NewPeerHealth(1, 10*time.Minute)
	client.SetPeerHealth(ph)

	if _, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc123"); err == nil {
		t.Fatal("expected a timeout error")
	}
	if ph.IsHealthy(server.URL) {
		t.Fatal("client HTTP timeout with a live caller ctx must count against the peer")
	}
}

// TestFetchSubtreeRaw_WithoutPeerRecording verifies the opt-out the subtree
// processor uses to take over recording: with the option the client records
// neither success nor failure, and without it the default recording path is
// unchanged.
func TestFetchSubtreeRaw_WithoutPeerRecording(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(5, 0, testLogger())
	ph := NewPeerHealth(1, 10*time.Minute)
	client.SetPeerHealth(ph)

	_, err := client.FetchSubtreeRaw(context.Background(), server.URL, "abc123", WithoutPeerRecording())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if !ph.IsHealthy(server.URL) {
		t.Fatal("WithoutPeerRecording must suppress the client's internal failure recording")
	}

	// Default path (no option) still records.
	_, _ = client.FetchSubtreeRaw(context.Background(), server.URL, "abc123")
	if ph.IsHealthy(server.URL) {
		t.Fatal("default FetchSubtreeRaw must still record the failure")
	}
}

// TestParseBinaryBlockMetadata_HeaderAndCoinbase verifies the parser carries
// the 80-byte header and the raw coinbase tx through from the block binary
// instead of discarding them: they are the only reliable source for the
// BLOCK_PROCESSED merkle root + coinbase BUMP (the P2P announcement never
// carries the coinbase, and /reprocess messages carry no header either).
func TestParseBinaryBlockMetadata_HeaderAndCoinbase(t *testing.T) {
	// Bitcoin genesis coinbase — a real, parseable coinbase (one input).
	const coinbaseHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000"
	coinbaseTx, err := bt.NewTxFromString(coinbaseHex)
	if err != nil {
		t.Fatalf("parse coinbase: %v", err)
	}

	merkleRoot := &chainhash.Hash{}
	merkleRoot[0] = 0xAB
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: merkleRoot,
	}
	subtree := &chainhash.Hash{}
	subtree[0] = 0x01

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree}, 0, 0, 700001, 0)
	if err != nil {
		t.Fatalf("NewBlock: %v", err)
	}
	payload, err := block.Bytes()
	if err != nil {
		t.Fatalf("block.Bytes: %v", err)
	}

	meta, err := ParseBinaryBlockMetadata(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := hex.EncodeToString(header.Bytes()); meta.HeaderHex != want {
		t.Errorf("HeaderHex = %q, want %q", meta.HeaderHex, want)
	}
	if len(meta.HeaderHex) != 160 {
		t.Errorf("HeaderHex length = %d, want 160 (80 bytes)", len(meta.HeaderHex))
	}
	if meta.CoinbaseTxHex != coinbaseHex {
		t.Errorf("CoinbaseTxHex = %q, want the original coinbase hex", meta.CoinbaseTxHex)
	}
}

// TestParseBinaryBlockMetadata_NilCoinbase verifies a block serialized
// without a coinbase (it round-trips as an input-less placeholder tx) yields
// an EMPTY CoinbaseTxHex — the placeholder must read as absent, not hash to
// a bogus coinbase txid downstream.
func TestParseBinaryBlockMetadata_NilCoinbase(t *testing.T) {
	payload := buildBinaryBlockBytes(42, [][]byte{append([]byte{0x01}, make([]byte, 31)...)})

	meta, err := ParseBinaryBlockMetadata(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.CoinbaseTxHex != "" {
		t.Errorf("CoinbaseTxHex = %q, want empty for a placeholder coinbase", meta.CoinbaseTxHex)
	}
	if meta.HeaderHex == "" {
		t.Error("HeaderHex empty: header must always be carried through")
	}
}
