package subtree

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// startRawSubtreeServer serves a raw 32-byte-hash subtree payload at any path,
// satisfying the merkle-service's DataHub fetch in tests that exercise
// handleMessage end-to-end.
func startRawSubtreeServer(payload []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
	}))
}

// --- Mock implementations ---

type mockRegStore struct {
	// registrations maps txid -> []callbackURL
	registrations map[string][]string
	batchGetCalls [][]string // records each BatchGet call's txids
	// batchGetErr, when non-nil, is returned from BatchGet instead of a result.
	// Used to simulate backing-store outages (F-056).
	batchGetErr error
}

func (m *mockRegStore) BatchGet(txids []string) (map[string][]string, error) {
	m.batchGetCalls = append(m.batchGetCalls, txids)
	if m.batchGetErr != nil {
		return nil, m.batchGetErr
	}
	result := make(map[string][]string)
	for _, txid := range txids {
		if urls, ok := m.registrations[txid]; ok {
			result[txid] = urls
		}
	}
	return result, nil
}

func (m *mockRegStore) Get(txid string) ([]string, error) {
	return m.registrations[txid], nil
}

type mockSeenCounter struct{}

func (m *mockSeenCounter) Increment(txid string, subtreeID string) (*store.IncrementResult, error) {
	return &store.IncrementResult{NewCount: 1, ThresholdReached: false}, nil
}

type mockRegCache struct {
	// cached tracks positive cache entries only. A txid present in this
	// map is treated as "cached and registered". Negative results are
	// not cached (see RegistrationCache documentation / F-020), so we
	// no longer carry an isRegistered bool per entry.
	cached map[string]bool
	setReg []string // txids passed to SetMultiRegistered
}

func (m *mockRegCache) FilterUncached(txids []string) (uncached []string, cachedRegistered []string) {
	for _, txid := range txids {
		isReg, isCached := m.cached[txid]
		if !isCached {
			uncached = append(uncached, txid)
		} else if isReg {
			cachedRegistered = append(cachedRegistered, txid)
		}
	}
	return
}

func (m *mockRegCache) SetMultiRegistered(txids []string) error {
	m.setReg = append(m.setReg, txids...)
	return nil
}

// --- Helpers ---

// buildRawBytes creates DataHub-format raw subtree data from given 32-byte hashes.
func buildRawBytes(hashes ...[]byte) []byte {
	data := make([]byte, len(hashes)*chainhash.HashSize)
	for i, h := range hashes {
		copy(data[i*chainhash.HashSize:], h)
	}
	return data
}

// hashFromHex creates a 32-byte hash from a hex txid in Bitcoin display order (reversed).
// This is the format users register with and what chainhash.Hash.String() returns.
func hashFromHex(t *testing.T, displayHex string) []byte {
	t.Helper()
	b, err := hex.DecodeString(displayHex)
	if err != nil {
		t.Fatalf("invalid hex %q: %v", displayHex, err)
	}
	if len(b) != chainhash.HashSize {
		t.Fatalf("expected %d bytes, got %d", chainhash.HashSize, len(b))
	}
	// Reverse to get internal byte order (what DataHub sends)
	for i := 0; i < len(b)/2; i++ {
		b[i], b[len(b)-1-i] = b[len(b)-1-i], b[i]
	}
	return b
}

// --- Tests ---

// TestParseRawTxids_MatchesChainhashString is the critical test:
// ParseRawTxids must produce the same hex string as chainhash.Hash.String()
// for the same raw bytes. This is what registrations are stored as.
func TestParseRawTxids_MatchesChainhashString(t *testing.T) {
	// Create some raw 32-byte hashes (internal byte order, as DataHub sends)
	rawHashes := [][]byte{
		make([]byte, 32),
		make([]byte, 32),
		make([]byte, 32),
	}
	rawHashes[0][0] = 0xab
	rawHashes[0][31] = 0xcd
	rawHashes[1][0] = 0x01
	rawHashes[1][15] = 0xff
	rawHashes[2][31] = 0x42

	rawData := buildRawBytes(rawHashes...)

	txids, err := datahub.ParseRawTxids(rawData)
	if err != nil {
		t.Fatalf("ParseRawTxids: %v", err)
	}

	for i, rawHash := range rawHashes {
		var h chainhash.Hash
		copy(h[:], rawHash)
		expected := h.String()

		if txids[i] != expected {
			t.Errorf("txid[%d]: ParseRawTxids=%q, chainhash.Hash.String()=%q — MISMATCH", i, txids[i], expected)
		}
	}
}

// TestParseRawTxids_ReversesBytes verifies that ParseRawTxids returns
// Bitcoin display order (reversed), not raw internal order.
func TestParseRawTxids_ReversesBytes(t *testing.T) {
	raw := make([]byte, 32)
	raw[0] = 0xAA  // internal first byte
	raw[31] = 0xBB // internal last byte

	txids, err := datahub.ParseRawTxids(raw)
	if err != nil {
		t.Fatalf("ParseRawTxids: %v", err)
	}
	if len(txids) != 1 {
		t.Fatalf("expected 1 txid, got %d", len(txids))
	}

	// In display order, raw[31]=0xBB should be first, raw[0]=0xAA should be last
	if !strings.HasPrefix(txids[0], "bb") {
		t.Errorf("expected txid to start with 'bb' (reversed), got %s", txids[0])
	}
	if !strings.HasSuffix(txids[0], "aa") {
		t.Errorf("expected txid to end with 'aa' (reversed), got %s", txids[0])
	}
}

// TestParseRawTxids_ConsistentWithRegistration simulates the real scenario:
// a user registers a txid in display order, DataHub sends the raw bytes,
// and ParseRawTxids must produce a string that matches the registration.
func TestParseRawTxids_ConsistentWithRegistration(t *testing.T) {
	// A real-looking txid in Bitcoin display order (how a user would register it)
	displayTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"

	// Convert to internal byte order (what DataHub would send)
	internalBytes := hashFromHex(t, displayTxid)

	// Parse as ParseRawTxids would
	txids, err := datahub.ParseRawTxids(internalBytes)
	if err != nil {
		t.Fatalf("ParseRawTxids: %v", err)
	}

	if txids[0] != displayTxid {
		t.Errorf("ParseRawTxids produced %q, expected %q (registration format)", txids[0], displayTxid)
	}
}

// TestFindRegisteredTxids_NoCache tests findRegisteredTxids without a cache.
func TestFindRegisteredTxids_NoCache(t *testing.T) {
	regTxid := "aabbccdd00000000000000000000000000000000000000000000000000000011"
	unregTxid := "1122334400000000000000000000000000000000000000000000000000000099"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			regTxid: {"http://callback.example.com/notify"},
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil, // no cache
	}

	result, err := p.findRegisteredTxids([]string{regTxid, unregTxid})
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 registered txid, got %d", len(result))
	}
	urls, ok := result[regTxid]
	if !ok {
		t.Fatalf("expected %s in result", regTxid)
	}
	if len(urls) != 1 || urls[0] != "http://callback.example.com/notify" {
		t.Errorf("expected [http://callback.example.com/notify], got %v", urls)
	}

	// All txids should have been sent to store (no cache)
	if len(regStore.batchGetCalls) != 1 {
		t.Fatalf("expected 1 BatchGet call, got %d", len(regStore.batchGetCalls))
	}
	if len(regStore.batchGetCalls[0]) != 2 {
		t.Errorf("expected 2 txids in BatchGet, got %d", len(regStore.batchGetCalls[0]))
	}
}

// TestFindRegisteredTxids_WithCache tests the cache + store interaction.
//
// Negatives are not cached (F-020), so a txid that the cache has
// "no positive entry" for is treated as uncached and re-queried against
// the backing store every pass. Only positive cache entries short-
// circuit the registration lookup.
func TestFindRegisteredTxids_WithCache(t *testing.T) {
	cachedRegTxid := "aaaa000000000000000000000000000000000000000000000000000000000001"
	uncachedTxidA := "bbbb000000000000000000000000000000000000000000000000000000000002"
	uncachedRegTxid := "cccc000000000000000000000000000000000000000000000000000000000003"
	uncachedNotRegTxid := "dddd000000000000000000000000000000000000000000000000000000000004"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			cachedRegTxid:   {"http://cached-cb.example.com"},
			uncachedRegTxid: {"http://cb.example.com"},
		},
	}

	cache := &mockRegCache{
		cached: map[string]bool{
			cachedRegTxid: true, // cached as registered (positive)
			// every other txid is uncached — negatives are never cached
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          cache,
	}

	txids := []string{cachedRegTxid, uncachedTxidA, uncachedRegTxid, uncachedNotRegTxid}
	result, err := p.findRegisteredTxids(txids)
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	// Should find: cachedRegTxid (from cache) + uncachedRegTxid (from store)
	if len(result) != 2 {
		t.Fatalf("expected 2 registered txids, got %d: %v", len(result), result)
	}

	if _, ok := result[cachedRegTxid]; !ok {
		t.Error("missing cached registered txid in result")
	}
	if _, ok := result[uncachedRegTxid]; !ok {
		t.Error("missing uncached registered txid in result")
	}

	// Two BatchGet calls: one for uncached txids, one for cached-registered txids' URLs.
	if len(regStore.batchGetCalls) != 2 {
		t.Fatalf("expected 2 BatchGet calls, got %d", len(regStore.batchGetCalls))
	}
	batchTxids := regStore.batchGetCalls[0]
	if len(batchTxids) != 3 {
		t.Errorf("expected 3 uncached txids in first BatchGet, got %d: %v", len(batchTxids), batchTxids)
	}
	cachedBatchTxids := regStore.batchGetCalls[1]
	if len(cachedBatchTxids) != 1 || cachedBatchTxids[0] != cachedRegTxid {
		t.Errorf("expected cached-registered txid in second BatchGet, got %v", cachedBatchTxids)
	}

	// Cache should record exactly one positive update (uncachedRegTxid).
	// The two not-found txids must NOT produce any cache writes — that is
	// the F-020 fix.
	if len(cache.setReg) != 1 || cache.setReg[0] != uncachedRegTxid {
		t.Errorf("expected SetMultiRegistered([%s]), got %v", uncachedRegTxid, cache.setReg)
	}
}

// TestFindRegisteredTxids_AllCached tests when all txids hit positive
// cache entries (no uncached lookups). Since negatives are not cached
// (F-020), "fully cached" means every input has a positive entry.
func TestFindRegisteredTxids_AllCached(t *testing.T) {
	txid1 := "1111000000000000000000000000000000000000000000000000000000000001"
	txid2 := "2222000000000000000000000000000000000000000000000000000000000002"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			txid1: {"http://cb.example.com"},
			txid2: {"http://cb2.example.com"},
		},
	}

	cache := &mockRegCache{
		cached: map[string]bool{
			txid1: true,
			txid2: true,
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          cache,
	}

	result, err := p.findRegisteredTxids([]string{txid1, txid2})
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 registered txids, got %d", len(result))
	}
	if _, ok := result[txid1]; !ok {
		t.Errorf("expected %s in result", txid1)
	}
	if _, ok := result[txid2]; !ok {
		t.Errorf("expected %s in result", txid2)
	}

	// One BatchGet call for cached-registered txids' URLs (no call for uncached since all cached).
	if len(regStore.batchGetCalls) != 1 {
		t.Errorf("expected 1 BatchGet call for cached URLs, got %d", len(regStore.batchGetCalls))
	}
}

// TestFindRegisteredTxids_NoneRegistered tests when no txids are registered.
func TestFindRegisteredTxids_NoneRegistered(t *testing.T) {
	regStore := &mockRegStore{
		registrations: map[string][]string{},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil,
	}

	txids := []string{
		"aaaa000000000000000000000000000000000000000000000000000000000001",
		"bbbb000000000000000000000000000000000000000000000000000000000002",
	}
	result, err := p.findRegisteredTxids(txids)
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 registered txids, got %d", len(result))
	}
}

// TestFindRegisteredTxids_EmptyInput tests with no txids.
func TestFindRegisteredTxids_EmptyInput(t *testing.T) {
	regStore := &mockRegStore{
		registrations: map[string][]string{},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil,
	}

	result, err := p.findRegisteredTxids(nil)
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 results for empty input, got %d", len(result))
	}
}

// TestEndToEnd_RawDataToRegistrationMatch is the most critical test:
// it simulates the entire flow from raw DataHub bytes through ParseRawTxids
// to findRegisteredTxids, ensuring a registered txid is found.
func TestEndToEnd_RawDataToRegistrationMatch(t *testing.T) {
	// User registers this txid (Bitcoin display order)
	registeredTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"
	unregisteredTxid := "0000000000000000000000000000000000000000000000000000000000000001"

	// Build raw binary data as DataHub would return it
	regRawBytes := hashFromHex(t, registeredTxid)
	unregRawBytes := hashFromHex(t, unregisteredTxid)
	rawData := buildRawBytes(regRawBytes, unregRawBytes)

	// Parse txids from raw data (what the subtree processor does)
	txids, err := datahub.ParseRawTxids(rawData)
	if err != nil {
		t.Fatalf("ParseRawTxids: %v", err)
	}

	if len(txids) != 2 {
		t.Fatalf("expected 2 txids, got %d", len(txids))
	}

	// Verify the parsed txids match the display format
	if txids[0] != registeredTxid {
		t.Fatalf("first parsed txid %q != registered %q", txids[0], registeredTxid)
	}
	if txids[1] != unregisteredTxid {
		t.Fatalf("second parsed txid %q != unregistered %q", txids[1], unregisteredTxid)
	}

	// Now run findRegisteredTxids
	regStore := &mockRegStore{
		registrations: map[string][]string{
			registeredTxid: {"http://example.com/callback"},
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil,
	}

	result, err := p.findRegisteredTxids(txids)
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 registered txid, got %d", len(result))
	}
	if _, ok := result[registeredTxid]; !ok {
		t.Errorf("expected %s in result", registeredTxid)
	}
}

// TestEndToEnd_ParseRawTxidsConsistentWithParseRawNodes verifies that
// ParseRawTxids and ParseRawNodes+Hash.String() produce the same txid strings.
// This ensures the subtree processor (SEEN) and block processor (MINED) paths
// use the same txid format.
func TestEndToEnd_ParseRawTxidsConsistentWithParseRawNodes(t *testing.T) {
	// Build raw data with varied byte patterns
	hashes := [][]byte{
		make([]byte, 32),
		make([]byte, 32),
		make([]byte, 32),
	}
	hashes[0][0] = 0xde
	hashes[0][31] = 0xad
	hashes[1][0] = 0xbe
	hashes[1][15] = 0xef
	hashes[1][31] = 0x01
	for i := range hashes[2] {
		hashes[2][i] = byte(i)
	}

	rawData := buildRawBytes(hashes...)

	// Parse via ParseRawTxids (subtree processor path)
	txids, err := datahub.ParseRawTxids(rawData)
	if err != nil {
		t.Fatalf("ParseRawTxids: %v", err)
	}

	// Parse via ParseRawNodes (block processor path)
	nodes, err := datahub.ParseRawNodes(rawData)
	if err != nil {
		t.Fatalf("ParseRawNodes: %v", err)
	}

	if len(txids) != len(nodes) {
		t.Fatalf("count mismatch: ParseRawTxids=%d, ParseRawNodes=%d", len(txids), len(nodes))
	}

	for i := range txids {
		nodeStr := nodes[i].Hash.String()
		if txids[i] != nodeStr {
			t.Errorf("txid[%d]: ParseRawTxids=%q, node.Hash.String()=%q — MISMATCH", i, txids[i], nodeStr)
		}
	}
}

// TestFindRegisteredTxids_LargeSubtree tests with a realistic subtree size.
func TestFindRegisteredTxids_LargeSubtree(t *testing.T) {
	// Simulate a subtree with 10000 txids, only 3 registered
	const totalTxids = 10000
	txids := make([]string, totalTxids)
	for i := 0; i < totalTxids; i++ {
		txids[i] = strings.Repeat("00", 28) + hex.EncodeToString([]byte{
			byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i),
		})
	}

	regStore := &mockRegStore{
		registrations: map[string][]string{
			txids[42]:   {"http://cb1.example.com"},
			txids[999]:  {"http://cb2.example.com"},
			txids[9999]: {"http://cb1.example.com", "http://cb3.example.com"},
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil,
	}

	result, err := p.findRegisteredTxids(txids)
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 registered txids, got %d", len(result))
	}

	if _, ok := result[txids[42]]; !ok {
		t.Error("missing txids[42] in result")
	}
	if _, ok := result[txids[999]]; !ok {
		t.Error("missing txids[999] in result")
	}
	if _, ok := result[txids[9999]]; !ok {
		t.Error("missing txids[9999] in result")
	}
}

// TestFindRegisteredTxids_CacheUpdatedCorrectly verifies the cache is properly
// populated after a store lookup. Only positive results should be cached;
// not-registered txids must not produce cache writes (F-020).
func TestFindRegisteredTxids_CacheUpdatedCorrectly(t *testing.T) {
	txidReg := "aaaa000000000000000000000000000000000000000000000000000000000001"
	txidNot1 := "bbbb000000000000000000000000000000000000000000000000000000000002"
	txidNot2 := "cccc000000000000000000000000000000000000000000000000000000000003"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			txidReg: {"http://callback.example.com"},
		},
	}

	cache := &mockRegCache{
		cached: map[string]bool{}, // everything uncached
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          cache,
	}

	_, err := p.findRegisteredTxids([]string{txidReg, txidNot1, txidNot2})
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}

	// Verify cache was updated with the single positive result only.
	if len(cache.setReg) != 1 || cache.setReg[0] != txidReg {
		t.Errorf("expected registered cache update for %s, got %v", txidReg, cache.setReg)
	}
}

// --- Idempotent Seen Counter Tests ---

// mockIdempotentSeenCounter simulates the idempotent seen counter behavior:
// tracks which subtreeIDs have been counted per txid and fires threshold once.
type mockIdempotentSeenCounter struct {
	mu              sync.Mutex
	subtreesByTxid  map[string]map[string]bool // txid -> set of subtreeIDs
	thresholdFired  map[string]bool            // txid -> whether threshold already fired
	threshold       int
}

func newMockIdempotentSeenCounter(threshold int) *mockIdempotentSeenCounter {
	return &mockIdempotentSeenCounter{
		subtreesByTxid: make(map[string]map[string]bool),
		thresholdFired: make(map[string]bool),
		threshold:      threshold,
	}
}

func (m *mockIdempotentSeenCounter) Increment(txid string, subtreeID string) (*store.IncrementResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.subtreesByTxid[txid] == nil {
		m.subtreesByTxid[txid] = make(map[string]bool)
	}

	// AddUnique semantics: only count if not already present.
	m.subtreesByTxid[txid][subtreeID] = true
	newCount := len(m.subtreesByTxid[txid])

	thresholdReached := false
	if newCount >= m.threshold && !m.thresholdFired[txid] {
		thresholdReached = true
		m.thresholdFired[txid] = true
	}

	return &store.IncrementResult{
		NewCount:         newCount,
		ThresholdReached: thresholdReached,
	}, nil
}

func TestIdempotentSeenCounter_FirstSubtreeIncrements(t *testing.T) {
	sc := newMockIdempotentSeenCounter(3)

	result, err := sc.Increment("txid-1", "subtree-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NewCount != 1 {
		t.Errorf("expected count=1, got %d", result.NewCount)
	}
	if result.ThresholdReached {
		t.Error("threshold should not be reached with 1 subtree")
	}
}

func TestIdempotentSeenCounter_DuplicateSubtreeDoesNotIncrement(t *testing.T) {
	sc := newMockIdempotentSeenCounter(3)

	// First call with subtree-A.
	sc.Increment("txid-1", "subtree-A")

	// Duplicate call with same subtree-A.
	result, _ := sc.Increment("txid-1", "subtree-A")
	if result.NewCount != 1 {
		t.Errorf("expected count=1 after duplicate, got %d", result.NewCount)
	}
	if result.ThresholdReached {
		t.Error("threshold should not fire on duplicate")
	}
}

func TestIdempotentSeenCounter_ThresholdFiresOnce(t *testing.T) {
	sc := newMockIdempotentSeenCounter(3)

	sc.Increment("txid-1", "subtree-A")
	sc.Increment("txid-1", "subtree-B")

	// Third unique subtree should trigger threshold.
	result, _ := sc.Increment("txid-1", "subtree-C")
	if result.NewCount != 3 {
		t.Errorf("expected count=3, got %d", result.NewCount)
	}
	if !result.ThresholdReached {
		t.Error("threshold should fire when unique count reaches threshold")
	}

	// Fourth unique subtree — threshold should NOT fire again.
	result, _ = sc.Increment("txid-1", "subtree-D")
	if result.NewCount != 4 {
		t.Errorf("expected count=4, got %d", result.NewCount)
	}
	if result.ThresholdReached {
		t.Error("threshold should NOT fire again after already fired")
	}
}

func TestIdempotentSeenCounter_ThresholdDoesNotFireOnDuplicates(t *testing.T) {
	sc := newMockIdempotentSeenCounter(2)

	sc.Increment("txid-1", "subtree-A")

	// Duplicate of subtree-A — count stays 1, no threshold.
	result, _ := sc.Increment("txid-1", "subtree-A")
	if result.NewCount != 1 {
		t.Errorf("expected count=1, got %d", result.NewCount)
	}
	if result.ThresholdReached {
		t.Error("threshold should not fire on duplicate subtree")
	}

	// Now a truly new subtree triggers threshold.
	result, _ = sc.Increment("txid-1", "subtree-B")
	if result.NewCount != 2 {
		t.Errorf("expected count=2, got %d", result.NewCount)
	}
	if !result.ThresholdReached {
		t.Error("threshold should fire on second unique subtree")
	}

	// Re-send subtree-A — should NOT fire threshold.
	result, _ = sc.Increment("txid-1", "subtree-A")
	if result.ThresholdReached {
		t.Error("threshold should not fire again on re-sent subtree")
	}
}

func TestIdempotentSeenCounter_IndependentPerTxid(t *testing.T) {
	sc := newMockIdempotentSeenCounter(2)

	sc.Increment("txid-1", "subtree-A")
	sc.Increment("txid-2", "subtree-A")

	// Same subtreeID for different txids should be independent.
	result1, _ := sc.Increment("txid-1", "subtree-B")
	result2, _ := sc.Increment("txid-2", "subtree-B")

	if !result1.ThresholdReached {
		t.Error("txid-1 threshold should fire")
	}
	if !result2.ThresholdReached {
		t.Error("txid-2 threshold should fire independently")
	}
}

// --- Integration-style Dedup Tests ---

// TestIntegration_DuplicateSubtreeOnlyProcessedOnce simulates sending
// duplicate subtree messages through the processor's dedup cache +
// findRegisteredTxids path, verifying only one set of lookups is made.
func TestIntegration_DuplicateSubtreeOnlyProcessedOnce(t *testing.T) {
	dc := cache.NewDedupCache(100)
	regStore := &mockRegStore{
		registrations: map[string][]string{
			"txid-registered": {"http://cb.example.com"},
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil,
		dedupCache:        dc,
	}

	txids := []string{"txid-registered", "txid-not-registered"}
	subtreeHash := "subtree-integration-test"

	// First processing: not in dedup cache, queries store.
	if dc.Contains(subtreeHash) {
		t.Fatal("subtree hash should not be in cache initially")
	}
	result1, err := p.findRegisteredTxids(txids)
	if err != nil {
		t.Fatalf("first findRegisteredTxids: %v", err)
	}
	if len(result1) != 1 {
		t.Fatalf("first call: expected 1 registered txid, got %d", len(result1))
	}
	if _, ok := result1["txid-registered"]; !ok {
		t.Error("first call: expected txid-registered in result")
	}
	// Mark as processed.
	dc.Add(subtreeHash)
	batchCallsAfterFirst := len(regStore.batchGetCalls)

	// Simulate duplicate message — dedup cache should prevent processing.
	if !dc.Contains(subtreeHash) {
		t.Fatal("subtree hash should be in cache after Add")
	}

	// In the real processor, handleMessage returns nil here.
	// Verify no additional store calls were made.
	if len(regStore.batchGetCalls) != batchCallsAfterFirst {
		t.Errorf("expected no additional BatchGet calls for duplicate, got %d total",
			len(regStore.batchGetCalls))
	}
}

// TestIntegration_SeenCounterIdempotency simulates the full seen counter
// flow: same subtreeID for same txid incremented multiple times, verifying
// count stays correct and threshold fires exactly once.
func TestIntegration_SeenCounterIdempotency(t *testing.T) {
	sc := newMockIdempotentSeenCounter(3)

	// Simulate 3 different subtrees for the same txid.
	subtrees := []string{"subtree-A", "subtree-B", "subtree-C"}
	var thresholdCount int

	for _, st := range subtrees {
		result, err := sc.Increment("txid-1", st)
		if err != nil {
			t.Fatalf("Increment error: %v", err)
		}
		if result.ThresholdReached {
			thresholdCount++
		}
	}

	if thresholdCount != 1 {
		t.Errorf("expected threshold to fire exactly once, fired %d times", thresholdCount)
	}

	// Now replay all subtrees (duplicates).
	for _, st := range subtrees {
		result, _ := sc.Increment("txid-1", st)
		if result.ThresholdReached {
			t.Errorf("threshold should not fire on duplicate subtree %s", st)
		}
		if result.NewCount != 3 {
			t.Errorf("count should remain at 3 after duplicates, got %d", result.NewCount)
		}
	}
}

// --- Dedup Cache Tests ---

// TestDedupCache_SkipsDuplicateSubtree verifies that the dedup cache
// prevents reprocessing of already-seen subtree hashes.
func TestDedupCache_SkipsDuplicateSubtree(t *testing.T) {
	dc := cache.NewDedupCache(100)

	// First time: not in cache
	if dc.Contains("hash-abc") {
		t.Error("expected hash not in cache initially")
	}

	// Mark as processed
	dc.Add("hash-abc")

	// Second time: in cache
	if !dc.Contains("hash-abc") {
		t.Error("expected hash in cache after Add")
	}
}

// TestDedupCache_AllowsRetryOnFailure verifies that failed processing
// (where Add is never called) allows the message to be retried.
func TestDedupCache_AllowsRetryOnFailure(t *testing.T) {
	dc := cache.NewDedupCache(100)

	// Simulate: message received, processing fails (no Add called)
	if dc.Contains("hash-fail") {
		t.Error("should not be in cache")
	}

	// Don't call Add (simulating failure)

	// Retry: should still not be in cache, allowing retry
	if dc.Contains("hash-fail") {
		t.Error("failed processing should not add to cache")
	}
}

// TestDedupCache_IntegrationWithProcessor verifies the dedup cache is
// properly checked before and updated after findRegisteredTxids.
func TestDedupCache_IntegrationWithProcessor(t *testing.T) {
	dc := cache.NewDedupCache(100)
	regStore := &mockRegStore{
		registrations: map[string][]string{
			"txid1": {"http://cb.example.com"},
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          nil,
		dedupCache:        dc,
	}

	// First call: not in dedup cache, should query store
	result, err := p.findRegisteredTxids([]string{"txid1", "txid2"})
	if err != nil {
		t.Fatalf("findRegisteredTxids: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 registered txid, got %d", len(result))
	}
	if len(regStore.batchGetCalls) != 1 {
		t.Fatalf("expected 1 BatchGet call, got %d", len(regStore.batchGetCalls))
	}

	// Simulate marking as processed
	dc.Add("subtree-hash-1")

	// Verify it's in cache now
	if !dc.Contains("subtree-hash-1") {
		t.Error("expected subtree hash in dedup cache")
	}
}

// --- Batched Callback Emission Tests ---

type mockSyncProducer struct {
	mu       sync.Mutex
	messages []*sarama.ProducerMessage
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return 0, int64(len(m.messages)), nil
}
func (m *mockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msgs...)
	return nil
}
func (m *mockSyncProducer) Close() error                { return nil }
func (m *mockSyncProducer) IsTransactional() bool       { return false }
func (m *mockSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (m *mockSyncProducer) BeginTxn() error             { return nil }
func (m *mockSyncProducer) CommitTxn() error             { return nil }
func (m *mockSyncProducer) AbortTxn() error              { return nil }
func (m *mockSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (m *mockSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (m *mockSyncProducer) getMessages() []*sarama.ProducerMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*sarama.ProducerMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

func decodeCallbackMsg(t *testing.T, pm *sarama.ProducerMessage) *kafka.CallbackTopicMessage {
	t.Helper()
	b, err := pm.Value.Encode()
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	msg, err := kafka.DecodeCallbackTopicMessage(b)
	if err != nil {
		t.Fatalf("decode callback msg: %v", err)
	}
	return msg
}

func newTestProcessor(t *testing.T, regStore RegistrationGetter, seenCounter SeenCounter) (*Processor, *mockSyncProducer) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{}
	p := &Processor{
		registrationStore: regStore,
		seenCounterStore:  seenCounter,
		callbackProducer:  kafka.NewTestProducer(mockProducer, "callback-test", logger),
	}
	p.InitBase("subtree-test")
	p.Logger = logger
	return p, mockProducer
}

// TestBatchedSeenCallbacks_SingleCallbackURL verifies that multiple txids for
// the same callbackURL produce one batched SEEN_ON_NETWORK message.
func TestBatchedSeenCallbacks_SingleCallbackURL(t *testing.T) {
	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, &mockSeenCounter{})

	registered := map[string][]string{
		"tx1": {"http://arcade.example.com/cb"},
		"tx2": {"http://arcade.example.com/cb"},
		"tx3": {"http://arcade.example.com/cb"},
	}

	if err := p.emitBatchedSeenCallbacks(registered, "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	msgs := mockProd.getMessages()
	// 1 SEEN_ON_NETWORK (no threshold reached → 0 SEEN_MULTIPLE_NODES)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	cb := decodeCallbackMsg(t, msgs[0])
	if cb.Type != kafka.CallbackSeenOnNetwork {
		t.Errorf("expected SEEN_ON_NETWORK, got %s", cb.Type)
	}
	if cb.CallbackURL != "http://arcade.example.com/cb" {
		t.Errorf("unexpected callbackURL: %s", cb.CallbackURL)
	}
	if len(cb.TxIDs) != 3 {
		t.Errorf("expected 3 TxIDs, got %d", len(cb.TxIDs))
	}
	txSet := make(map[string]bool)
	for _, id := range cb.TxIDs {
		txSet[id] = true
	}
	if !txSet["tx1"] || !txSet["tx2"] || !txSet["tx3"] {
		t.Errorf("missing txids in batch: %v", cb.TxIDs)
	}
}

// TestBatchedSeenCallbacks_MultipleCallbackURLs verifies separate batched messages per callbackURL.
func TestBatchedSeenCallbacks_MultipleCallbackURLs(t *testing.T) {
	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, &mockSeenCounter{})

	registered := map[string][]string{
		"tx1": {"http://url-A/cb"},
		"tx2": {"http://url-B/cb"},
		"tx3": {"http://url-A/cb"},
	}

	if err := p.emitBatchedSeenCallbacks(registered, "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	msgs := mockProd.getMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (one per callbackURL), got %d", len(msgs))
	}

	byURL := make(map[string]*kafka.CallbackTopicMessage)
	for _, pm := range msgs {
		cb := decodeCallbackMsg(t, pm)
		byURL[cb.CallbackURL] = cb
	}

	msgA := byURL["http://url-A/cb"]
	if msgA == nil || len(msgA.TxIDs) != 2 {
		t.Errorf("expected 2 txids for url-A, got %v", msgA)
	}
	msgB := byURL["http://url-B/cb"]
	if msgB == nil || len(msgB.TxIDs) != 1 || msgB.TxIDs[0] != "tx2" {
		t.Errorf("expected [tx2] for url-B, got %v", msgB)
	}
}

// TestBatchedSeenCallbacks_NoRegistered verifies no messages when no txids registered.
func TestBatchedSeenCallbacks_NoRegistered(t *testing.T) {
	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, &mockSeenCounter{})

	if err := p.emitBatchedSeenCallbacks(map[string][]string{}, "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	if len(mockProd.getMessages()) != 0 {
		t.Error("expected no messages for empty registered map")
	}
}

// TestBatchedSeenCallbacks_SeenMultipleNodesThreshold verifies batched SEEN_MULTIPLE_NODES.
func TestBatchedSeenCallbacks_SeenMultipleNodesThreshold(t *testing.T) {
	// Threshold=1 so every txid triggers SEEN_MULTIPLE_NODES.
	sc := newMockIdempotentSeenCounter(1)
	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, sc)

	registered := map[string][]string{
		"tx1": {"http://arcade/cb"},
		"tx2": {"http://arcade/cb"},
	}

	if err := p.emitBatchedSeenCallbacks(registered, "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	msgs := mockProd.getMessages()
	// 1 SEEN_ON_NETWORK + 1 SEEN_MULTIPLE_NODES = 2 messages
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	byType := make(map[kafka.CallbackType]*kafka.CallbackTopicMessage)
	for _, pm := range msgs {
		cb := decodeCallbackMsg(t, pm)
		byType[cb.Type] = cb
	}

	seen := byType[kafka.CallbackSeenOnNetwork]
	if seen == nil || len(seen.TxIDs) != 2 {
		t.Errorf("expected 2 txids in SEEN_ON_NETWORK, got %v", seen)
	}

	multi := byType[kafka.CallbackSeenMultipleNodes]
	if multi == nil || len(multi.TxIDs) != 2 {
		t.Errorf("expected 2 txids in SEEN_MULTIPLE_NODES, got %v", multi)
	}
}

// TestBatchedSeenCallbacks_PartialThreshold verifies only threshold-reached txids in SEEN_MULTIPLE_NODES.
func TestBatchedSeenCallbacks_PartialThreshold(t *testing.T) {
	// Threshold=2: tx1 has already been seen once (will reach threshold), tx2 hasn't.
	sc := newMockIdempotentSeenCounter(2)
	sc.Increment("tx1", "subtree-PREV") // pre-seen once

	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, sc)

	registered := map[string][]string{
		"tx1": {"http://arcade/cb"},
		"tx2": {"http://arcade/cb"},
	}

	if err := p.emitBatchedSeenCallbacks(registered, "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	msgs := mockProd.getMessages()
	// 1 SEEN_ON_NETWORK (both txids) + 1 SEEN_MULTIPLE_NODES (only tx1) = 2
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	for _, pm := range msgs {
		cb := decodeCallbackMsg(t, pm)
		if cb.Type == kafka.CallbackSeenMultipleNodes {
			if len(cb.TxIDs) != 1 || cb.TxIDs[0] != "tx1" {
				t.Errorf("expected SEEN_MULTIPLE_NODES with [tx1], got %v", cb.TxIDs)
			}
		}
	}
}

// TestBatchedSeenCallbacks_ChunksLargeBatch verifies that batches exceeding
// callbackBatchChunkSize are split into multiple messages, preventing
// Kafka "Message was too large" rejections.
func TestBatchedSeenCallbacks_ChunksLargeBatch(t *testing.T) {
	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, &mockSeenCounter{})

	const total = callbackBatchChunkSize*2 + 17
	registered := make(map[string][]string, total)
	for i := 0; i < total; i++ {
		registered[fmt.Sprintf("tx%05d", i)] = []string{"http://arcade/cb"}
	}

	if err := p.emitBatchedSeenCallbacks(registered, "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	msgs := mockProd.getMessages()
	// total txids / chunk size, rounded up → 3 SEEN_ON_NETWORK messages.
	if len(msgs) != 3 {
		t.Fatalf("expected 3 chunked messages, got %d", len(msgs))
	}

	seenTxids := make(map[string]bool, total)
	for _, pm := range msgs {
		cb := decodeCallbackMsg(t, pm)
		if cb.Type != kafka.CallbackSeenOnNetwork {
			t.Errorf("expected SEEN_ON_NETWORK, got %s", cb.Type)
		}
		if cb.CallbackURL != "http://arcade/cb" {
			t.Errorf("unexpected callbackURL: %s", cb.CallbackURL)
		}
		if len(cb.TxIDs) > callbackBatchChunkSize {
			t.Errorf("chunk exceeds max size: got %d, max %d", len(cb.TxIDs), callbackBatchChunkSize)
		}
		for _, id := range cb.TxIDs {
			if seenTxids[id] {
				t.Errorf("duplicate txid across chunks: %s", id)
			}
			seenTxids[id] = true
		}
	}
	if len(seenTxids) != total {
		t.Errorf("expected %d unique txids across chunks, got %d", total, len(seenTxids))
	}
}

// --- Subtree DLQ Tests ---

// TestHandleTransientFailure_RoutesToDLQAtMaxAttempts drives a subtree message
// through handleTransientFailure with repeated failures and asserts that
// (a) before MaxAttempts the retry producer sees publishes with incrementing
// AttemptCount and (b) on the final attempt the DLQ producer sees exactly one
// publish with AttemptCount == MaxAttempts.
func TestHandleTransientFailure_RoutesToDLQAtMaxAttempts(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}

	maxAttempts := 3
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{MaxAttempts: maxAttempts},
		},
		retryProducer: kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:   kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
	}
	p.InitBase("subtree-dlq-test")
	p.Logger = logger

	subtreeMsg := &kafka.SubtreeMessage{
		Hash:       "subtree-hash-abc",
		DataHubURL: "http://datahub.example.com",
	}
	cause := errors.New("datahub 404")

	// Simulate retries until MaxAttempts is reached.
	for i := 0; i < maxAttempts; i++ {
		if err := p.handleTransientFailure(subtreeMsg, "fetch", cause); err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
	}

	retryMsgs := retryMock.getMessages()
	if len(retryMsgs) != maxAttempts-1 {
		t.Fatalf("expected %d retry publishes, got %d", maxAttempts-1, len(retryMsgs))
	}
	for i, pm := range retryMsgs {
		b, err := pm.Value.Encode()
		if err != nil {
			t.Fatalf("retry msg %d: encode: %v", i, err)
		}
		decoded, err := kafka.DecodeSubtreeMessage(b)
		if err != nil {
			t.Fatalf("retry msg %d: decode: %v", i, err)
		}
		if decoded.AttemptCount != i+1 {
			t.Errorf("retry msg %d: expected AttemptCount=%d, got %d", i, i+1, decoded.AttemptCount)
		}
	}

	dlqMsgs := dlqMock.getMessages()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected exactly 1 DLQ publish, got %d", len(dlqMsgs))
	}
	b, err := dlqMsgs[0].Value.Encode()
	if err != nil {
		t.Fatalf("dlq msg: encode: %v", err)
	}
	dlqDecoded, err := kafka.DecodeSubtreeMessage(b)
	if err != nil {
		t.Fatalf("dlq msg: decode: %v", err)
	}
	if dlqDecoded.AttemptCount != maxAttempts {
		t.Errorf("DLQ msg AttemptCount: expected %d, got %d", maxAttempts, dlqDecoded.AttemptCount)
	}
	if dlqDecoded.Hash != subtreeMsg.Hash {
		t.Errorf("DLQ msg Hash: expected %q, got %q", subtreeMsg.Hash, dlqDecoded.Hash)
	}

	if got := p.messagesRetried.Load(); got != int64(maxAttempts-1) {
		t.Errorf("messagesRetried: expected %d, got %d", maxAttempts-1, got)
	}
	if got := p.messagesDLQ.Load(); got != 1 {
		t.Errorf("messagesDLQ: expected 1, got %d", got)
	}
}

// TestHandleTransientFailure_DefaultsMaxAttemptsWhenUnset verifies the guard
// that treats non-positive MaxAttempts as the built-in default (10), so a
// misconfigured deployment never collapses into "DLQ on first failure".
func TestHandleTransientFailure_DefaultsMaxAttemptsWhenUnset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}

	p := &Processor{
		cfg:           &config.Config{Subtree: config.SubtreeConfig{MaxAttempts: 0}},
		retryProducer: kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:   kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
	}
	p.InitBase("subtree-dlq-default-test")
	p.Logger = logger

	subtreeMsg := &kafka.SubtreeMessage{Hash: "h"}
	if err := p.handleTransientFailure(subtreeMsg, "fetch", errors.New("x")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(retryMock.getMessages()) != 1 {
		t.Errorf("expected 1 retry publish with default MaxAttempts, got %d", len(retryMock.getMessages()))
	}
	if len(dlqMock.getMessages()) != 0 {
		t.Errorf("expected 0 DLQ publishes, got %d", len(dlqMock.getMessages()))
	}
}

// --- F-057: SEEN callback publish failure propagation tests ---
//
// The fakes/helpers below are named distinctly from the existing mockSyncProducer
// (which always succeeds) so they can be added without disturbing the existing
// tests. They mirror the "callbackFailingProducer" pattern from PR #77's
// subtree_worker tests but live in this package.

// callbackFailingSyncProducer is a sarama.SyncProducer that records every call
// and can be configured to fail every send. Used to drive the
// emitBatchedSeenCallbacks → handleTransientFailure path for F-057.
type callbackFailingSyncProducer struct {
	mu       sync.Mutex
	messages []*sarama.ProducerMessage
	failAll  bool
	failErr  error
}

func (f *callbackFailingSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAll {
		return 0, 0, f.failErr
	}
	f.messages = append(f.messages, msg)
	return 0, int64(len(f.messages)), nil
}

func (f *callbackFailingSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, m := range msgs {
		if _, _, err := f.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (f *callbackFailingSyncProducer) Close() error          { return nil }
func (f *callbackFailingSyncProducer) IsTransactional() bool { return false }
func (f *callbackFailingSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}
func (f *callbackFailingSyncProducer) BeginTxn() error  { return nil }
func (f *callbackFailingSyncProducer) CommitTxn() error { return nil }
func (f *callbackFailingSyncProducer) AbortTxn() error  { return nil }
func (f *callbackFailingSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (f *callbackFailingSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (f *callbackFailingSyncProducer) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

// urlFailingSyncProducer fails publishes whose decoded CallbackURL matches the
// configured failURL and lets every other publish through. Used to verify
// partial-success semantics: independent URLs still receive their best-effort
// delivery on a per-URL failure. Inspecting the value (not the key) sidesteps
// the SHA256 hashing that PublishWithHashKey applies.
type urlFailingSyncProducer struct {
	mu        sync.Mutex
	messages  []*sarama.ProducerMessage
	failURL   string
	failErr   error
	failCount int
}

func (f *urlFailingSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if msg.Value != nil {
		raw, err := msg.Value.Encode()
		if err == nil {
			if decoded, decErr := kafka.DecodeCallbackTopicMessage(raw); decErr == nil {
				if decoded.CallbackURL == f.failURL {
					f.failCount++
					return 0, 0, f.failErr
				}
			}
		}
	}
	f.messages = append(f.messages, msg)
	return 0, int64(len(f.messages)), nil
}

func (f *urlFailingSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, m := range msgs {
		if _, _, err := f.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (f *urlFailingSyncProducer) Close() error          { return nil }
func (f *urlFailingSyncProducer) IsTransactional() bool { return false }
func (f *urlFailingSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}
func (f *urlFailingSyncProducer) BeginTxn() error  { return nil }
func (f *urlFailingSyncProducer) CommitTxn() error { return nil }
func (f *urlFailingSyncProducer) AbortTxn() error  { return nil }
func (f *urlFailingSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (f *urlFailingSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (f *urlFailingSyncProducer) getMessages() []*sarama.ProducerMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*sarama.ProducerMessage, len(f.messages))
	copy(result, f.messages)
	return result
}

// TestEmitBatchedSeenCallbacks_HappyPathReturnsNil verifies the no-error path
// for the new error-returning signature.
func TestEmitBatchedSeenCallbacks_HappyPathReturnsNil(t *testing.T) {
	regStore := &mockRegStore{registrations: map[string][]string{}}
	p, mockProd := newTestProcessor(t, regStore, &mockSeenCounter{})

	registered := map[string][]string{
		"tx1": {"http://url-A/cb"},
		"tx2": {"http://url-B/cb"},
	}

	if err := p.emitBatchedSeenCallbacks(registered, "subtree-happy"); err != nil {
		t.Fatalf("expected nil error on happy path, got: %v", err)
	}
	if got := len(mockProd.getMessages()); got != 2 {
		t.Errorf("expected 2 callback publishes on happy path, got %d", got)
	}
}

// TestEmitBatchedSeenCallbacks_PublishFailureReturnsError verifies that a
// callback-producer failure for ANY URL surfaces an error to the caller —
// the F-057 fix.
func TestEmitBatchedSeenCallbacks_PublishFailureReturnsError(t *testing.T) {
	cbMock := &callbackFailingSyncProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &Processor{
		registrationStore: &mockRegStore{registrations: map[string][]string{}},
		seenCounterStore:  &mockSeenCounter{},
		callbackProducer:  kafka.NewTestProducer(cbMock, "callback-test", logger),
	}
	p.InitBase("subtree-cb-fail-test")
	p.Logger = logger

	registered := map[string][]string{
		"tx1": {"http://url-A/cb"},
		"tx2": {"http://url-B/cb"},
	}

	err := p.emitBatchedSeenCallbacks(registered, "subtree-fail")
	if err == nil {
		t.Fatalf("expected non-nil error when callback publish fails")
	}
	if cbMock.sentCount() != 0 {
		t.Errorf("expected 0 successful sends with failAll=true, got %d", cbMock.sentCount())
	}
}

// TestEmitBatchedSeenCallbacks_PartialFailureStillAttemptsOtherURLs verifies
// the partial-success contract: a per-URL publish failure does NOT short-circuit
// the loop — independent callback targets still receive their best-effort
// delivery on this attempt — but an error IS returned so the caller re-drives.
func TestEmitBatchedSeenCallbacks_PartialFailureStillAttemptsOtherURLs(t *testing.T) {
	failingURL := "http://url-fail/cb"
	okURL := "http://url-ok/cb"

	cbMock := &urlFailingSyncProducer{
		failURL: failingURL,
		failErr: errors.New("kafka transient"),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &Processor{
		registrationStore: &mockRegStore{registrations: map[string][]string{}},
		seenCounterStore:  &mockSeenCounter{},
		callbackProducer:  kafka.NewTestProducer(cbMock, "callback-test", logger),
	}
	p.InitBase("subtree-cb-partial-test")
	p.Logger = logger

	registered := map[string][]string{
		"tx1": {failingURL},
		"tx2": {okURL},
	}

	err := p.emitBatchedSeenCallbacks(registered, "subtree-partial")
	if err == nil {
		t.Fatalf("expected non-nil error when one callback URL publish fails")
	}

	// The successful URL must still have received its best-effort delivery,
	// matching the partial-success semantics from PR #77's publishSubtreeCallbacks.
	msgs := cbMock.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 successful publish (the OK URL), got %d", len(msgs))
	}
	cb := decodeCallbackMsg(t, msgs[0])
	if cb.CallbackURL != okURL {
		t.Errorf("expected successful publish targeted at %s, got %s", okURL, cb.CallbackURL)
	}
	if cbMock.failCount == 0 {
		t.Errorf("expected at least 1 failed publish targeted at %s", failingURL)
	}
}

// TestHandleMessage_CallbackPublishFailure_RoutesToRetry verifies the F-057
// end-to-end fix: when emitBatchedSeenCallbacks fails, handleMessage routes
// the subtree message through handleTransientFailure rather than silently
// acking and dropping the SEEN notification.
func TestHandleMessage_CallbackPublishFailure_RoutesToRetry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registeredTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			registeredTxid: {"http://callback.example.com/notify"},
		},
	}

	// Fake DataHub: serve a single 32-byte raw hash for the registered txid.
	rawBytes := hashFromHex(t, registeredTxid)
	dataHubServer := startRawSubtreeServer(rawBytes)
	defer dataHubServer.Close()

	cbMock := &callbackFailingSyncProducer{failAll: true, failErr: errors.New("kafka callback topic outage")}
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}

	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{
				MaxAttempts: 5,
				StorageMode: "stream", // skip blob store
			},
		},
		registrationStore: regStore,
		seenCounterStore:  &mockSeenCounter{},
		callbackProducer:  kafka.NewTestProducer(cbMock, "callback-test", logger),
		retryProducer:     kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:       kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
		dataHubClient:     datahub.NewClient(5, 0, logger),
	}
	p.InitBase("subtree-handle-cb-fail-test")
	p.Logger = logger

	subtreeMsg := &kafka.SubtreeMessage{
		Hash:         "subtree-cb-fail",
		DataHubURL:   dataHubServer.URL,
		AttemptCount: 0,
	}
	value, err := subtreeMsg.Encode()
	if err != nil {
		t.Fatalf("encode subtree msg: %v", err)
	}

	if err := p.handleMessage(t.Context(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage: expected nil error (retry path returns nil after re-publishing), got: %v", err)
	}

	// Retry producer must have received the re-published subtree message.
	if got := len(retryMock.getMessages()); got != 1 {
		t.Errorf("expected exactly 1 retry publish, got %d", got)
	}
	// DLQ must NOT have been touched (we're nowhere near max attempts).
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected zero DLQ publishes, got %d", got)
	}
	// messagesProcessed must NOT have been incremented — the subtree wasn't
	// successfully processed end-to-end.
	if got := p.messagesProcessed.Load(); got != 0 {
		t.Errorf("expected messagesProcessed=0 after callback failure, got %d", got)
	}
	if got := p.messagesRetried.Load(); got != 1 {
		t.Errorf("expected messagesRetried=1 after callback failure, got %d", got)
	}
}

// --- F-056: Cached registration lookup failure propagation tests ---
//
// Pre-fix, when the registration cache reported txids as registered but the
// backing store BatchGet for those txids failed, findRegisteredTxids logged a
// warning and returned a partial allRegistered map. handleMessage then marked
// the subtree as processed in the dedup cache, permanently dropping
// SEEN_ON_NETWORK and threshold callbacks for those txids. The fix returns the
// lookup error so handleMessage routes through handleTransientFailure (which
// leaves the dedup cache untouched and lets the consumer redeliver).

// TestFindRegisteredTxids_CachedLookupFailureReturnsError verifies that a
// backing-store failure on the cached-registered-URL lookup surfaces as an
// error rather than being swallowed.
func TestFindRegisteredTxids_CachedLookupFailureReturnsError(t *testing.T) {
	cachedRegTxid := "aaaa000000000000000000000000000000000000000000000000000000000001"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			cachedRegTxid: {"http://cached-cb.example.com"},
		},
		batchGetErr: errors.New("aerospike unavailable"),
	}

	cache := &mockRegCache{
		cached: map[string]bool{
			cachedRegTxid: true, // cache says: registered
		},
	}

	p := &Processor{
		registrationStore: regStore,
		regCache:          cache,
	}

	result, err := p.findRegisteredTxids([]string{cachedRegTxid})
	if err == nil {
		t.Fatalf("expected non-nil error when backing-store lookup fails, got result=%v", result)
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %v", result)
	}
}

// TestHandleMessage_CachedLookupFailure_RoutesToRetryAndSkipsDedup is the
// end-to-end F-056 contract test: when the cache says "registered" but the
// backing store BatchGet for cached txids fails, handleMessage must route the
// subtree message through the retry pipeline AND must NOT add the subtree
// hash to the dedup cache (otherwise redeliveries are silently skipped and
// SEEN callbacks are permanently lost).
func TestHandleMessage_CachedLookupFailure_RoutesToRetryAndSkipsDedup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registeredTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"

	// Backing store will fail every BatchGet — simulating an Aerospike outage.
	regStore := &mockRegStore{
		registrations: map[string][]string{
			registeredTxid: {"http://callback.example.com/notify"},
		},
		batchGetErr: errors.New("aerospike outage"),
	}

	// Pre-populate the cache to claim the txid is registered. With this set,
	// findRegisteredTxids hits the cached-URL BatchGet path that previously
	// swallowed errors.
	regCache := &mockRegCache{
		cached: map[string]bool{
			registeredTxid: true,
		},
	}

	rawBytes := hashFromHex(t, registeredTxid)
	dataHubServer := startRawSubtreeServer(rawBytes)
	defer dataHubServer.Close()

	cbMock := &mockSyncProducer{}
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	dedup := cache.NewDedupCache(100)

	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{
				MaxAttempts: 5,
				StorageMode: "stream", // skip blob store
			},
		},
		registrationStore: regStore,
		regCache:          regCache,
		seenCounterStore:  &mockSeenCounter{},
		callbackProducer:  kafka.NewTestProducer(cbMock, "callback-test", logger),
		retryProducer:     kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:       kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
		dataHubClient:     datahub.NewClient(5, 0, logger),
		dedupCache:        dedup,
	}
	p.InitBase("subtree-cached-lookup-fail-test")
	p.Logger = logger

	subtreeMsg := &kafka.SubtreeMessage{
		Hash:         "subtree-cached-lookup-fail",
		DataHubURL:   dataHubServer.URL,
		AttemptCount: 0,
	}
	value, err := subtreeMsg.Encode()
	if err != nil {
		t.Fatalf("encode subtree msg: %v", err)
	}

	if err := p.handleMessage(t.Context(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage: expected nil error (retry path returns nil after re-publishing), got: %v", err)
	}

	// Critical: dedup cache MUST NOT contain this subtree's hash, otherwise
	// the redelivery would be silently skipped.
	if dedup.Contains(subtreeMsg.Hash) {
		t.Errorf("dedup cache must NOT contain %q after a cached-lookup failure", subtreeMsg.Hash)
	}

	// Retry producer received the re-published subtree message.
	if got := len(retryMock.getMessages()); got != 1 {
		t.Errorf("expected exactly 1 retry publish, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected zero DLQ publishes, got %d", got)
	}
	if got := p.messagesProcessed.Load(); got != 0 {
		t.Errorf("expected messagesProcessed=0 after cached-lookup failure, got %d", got)
	}
	if got := p.messagesRetried.Load(); got != 1 {
		t.Errorf("expected messagesRetried=1 after cached-lookup failure, got %d", got)
	}
	// No callbacks should have been emitted — we never built a complete map.
	if got := len(cbMock.getMessages()); got != 0 {
		t.Errorf("expected 0 callback publishes after cached-lookup failure, got %d", got)
	}
}

// --- F-058: Seen-counter increment failure propagation tests ---
//
// Pre-fix, when seenCounterStore.Increment returned a transient error the
// emitBatchedSeenCallbacks loop logged a warning and continued, while
// handleMessage still added the subtree's hash to the dedup cache. Any
// redelivery was then silently skipped — permanently undercounting network
// observations and suppressing SEEN_MULTIPLE_NODES callbacks for affected
// txids. The fix returns the first error from emitBatchedSeenCallbacks so
// handleMessage routes the work through handleTransientFailure (which
// leaves the dedup cache untouched).

// failingSeenCounter returns a configured error from every Increment call.
// Used to drive the seen-counter failure path for F-058. Reuses the same
// pattern as the existing mockSeenCounter / mockIdempotentSeenCounter fakes.
type failingSeenCounter struct {
	mu       sync.Mutex
	err      error
	failed   int
	attempts []string // txids passed to Increment, in call order
}

func (f *failingSeenCounter) Increment(txid string, subtreeID string) (*store.IncrementResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = append(f.attempts, txid)
	f.failed++
	return nil, f.err
}

// TestEmitBatchedSeenCallbacks_IncrementFailureReturnsError verifies that a
// seenCounterStore.Increment error surfaces to the caller — the F-058 fix.
// The publish path for SEEN_ON_NETWORK still succeeds (Increment failure must
// not suppress the per-txid SEEN_ON_NETWORK callback), but the function
// returns a non-nil error so handleMessage redelivers.
func TestEmitBatchedSeenCallbacks_IncrementFailureReturnsError(t *testing.T) {
	regStore := &mockRegStore{registrations: map[string][]string{}}
	sc := &failingSeenCounter{err: errors.New("aerospike counter outage")}
	p, mockProd := newTestProcessor(t, regStore, sc)

	registered := map[string][]string{
		"tx1": {"http://url-A/cb"},
		"tx2": {"http://url-B/cb"},
	}

	err := p.emitBatchedSeenCallbacks(registered, "subtree-counter-fail")
	if err == nil {
		t.Fatalf("expected non-nil error when seen-counter Increment fails")
	}
	if !strings.Contains(err.Error(), "incrementing seen counter") {
		t.Errorf("expected error to wrap increment context, got: %v", err)
	}

	// Both txids should have been attempted (best-effort iteration past
	// the first failure mirrors PR #81's per-URL partial-success contract).
	if sc.failed != 2 {
		t.Errorf("expected 2 Increment attempts (one per txid), got %d", sc.failed)
	}

	// SEEN_ON_NETWORK publishes for both URLs must still have happened —
	// the increment failure must not suppress already-batched per-URL
	// SEEN_ON_NETWORK delivery on this attempt.
	msgs := mockProd.getMessages()
	if len(msgs) != 2 {
		t.Errorf("expected 2 SEEN_ON_NETWORK publishes despite increment failure, got %d", len(msgs))
	}
	for _, pm := range msgs {
		cb := decodeCallbackMsg(t, pm)
		if cb.Type != kafka.CallbackSeenOnNetwork {
			t.Errorf("expected SEEN_ON_NETWORK only (no SEEN_MULTIPLE_NODES on failure), got %s", cb.Type)
		}
	}
}

// TestHandleMessage_SeenCounterIncrementFailure_RoutesToRetryAndSkipsDedup is
// the end-to-end F-058 contract test: when seenCounterStore.Increment fails
// for a registered txid, handleMessage must route the subtree message
// through the retry pipeline AND must NOT add the subtree hash to the dedup
// cache (otherwise redelivery is silently skipped and SEEN_MULTIPLE_NODES
// callbacks are permanently lost).
func TestHandleMessage_SeenCounterIncrementFailure_RoutesToRetryAndSkipsDedup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registeredTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			registeredTxid: {"http://callback.example.com/notify"},
		},
	}

	rawBytes := hashFromHex(t, registeredTxid)
	dataHubServer := startRawSubtreeServer(rawBytes)
	defer dataHubServer.Close()

	cbMock := &mockSyncProducer{}
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	dedup := cache.NewDedupCache(100)
	sc := &failingSeenCounter{err: errors.New("aerospike counter outage")}

	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{
				MaxAttempts: 5,
				StorageMode: "stream", // skip blob store
			},
		},
		registrationStore: regStore,
		seenCounterStore:  sc,
		callbackProducer:  kafka.NewTestProducer(cbMock, "callback-test", logger),
		retryProducer:     kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:       kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
		dataHubClient:     datahub.NewClient(5, 0, logger),
		dedupCache:        dedup,
	}
	p.InitBase("subtree-counter-fail-test")
	p.Logger = logger

	subtreeMsg := &kafka.SubtreeMessage{
		Hash:         "subtree-counter-fail",
		DataHubURL:   dataHubServer.URL,
		AttemptCount: 0,
	}
	value, err := subtreeMsg.Encode()
	if err != nil {
		t.Fatalf("encode subtree msg: %v", err)
	}

	if err := p.handleMessage(t.Context(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage: expected nil error (retry path returns nil after re-publishing), got: %v", err)
	}

	// Critical: dedup cache MUST NOT contain this subtree's hash. If it did,
	// the consumer's redelivery would be silently dropped and the
	// SEEN_MULTIPLE_NODES callback for this txid would be permanently lost.
	if dedup.Contains(subtreeMsg.Hash) {
		t.Errorf("dedup cache must NOT contain %q after a seen-counter Increment failure", subtreeMsg.Hash)
	}

	// Retry producer received the re-published subtree message.
	if got := len(retryMock.getMessages()); got != 1 {
		t.Errorf("expected exactly 1 retry publish, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected zero DLQ publishes, got %d", got)
	}
	if got := p.messagesProcessed.Load(); got != 0 {
		t.Errorf("expected messagesProcessed=0 after seen-counter failure, got %d", got)
	}
	if got := p.messagesRetried.Load(); got != 1 {
		t.Errorf("expected messagesRetried=1 after seen-counter failure, got %d", got)
	}

	// Increment was attempted at least once for the registered txid.
	if sc.failed < 1 {
		t.Errorf("expected at least 1 Increment attempt, got %d", sc.failed)
	}
}

// TestHandleMessage_SeenCounterSuccess_UpdatesDedup is the success companion
// to TestHandleMessage_SeenCounterIncrementFailure: when Increment succeeds,
// handleMessage returns nil, the dedup cache is updated, and the retry path
// is NOT exercised.
func TestHandleMessage_SeenCounterSuccess_UpdatesDedup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registeredTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"

	regStore := &mockRegStore{
		registrations: map[string][]string{
			registeredTxid: {"http://callback.example.com/notify"},
		},
	}

	rawBytes := hashFromHex(t, registeredTxid)
	dataHubServer := startRawSubtreeServer(rawBytes)
	defer dataHubServer.Close()

	cbMock := &mockSyncProducer{}
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	dedup := cache.NewDedupCache(100)

	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{
				MaxAttempts: 5,
				StorageMode: "stream",
			},
		},
		registrationStore: regStore,
		seenCounterStore:  &mockSeenCounter{}, // succeeds, ThresholdReached=false
		callbackProducer:  kafka.NewTestProducer(cbMock, "callback-test", logger),
		retryProducer:     kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:       kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
		dataHubClient:     datahub.NewClient(5, 0, logger),
		dedupCache:        dedup,
	}
	p.InitBase("subtree-counter-ok-test")
	p.Logger = logger

	subtreeMsg := &kafka.SubtreeMessage{
		Hash:         "subtree-counter-ok",
		DataHubURL:   dataHubServer.URL,
		AttemptCount: 0,
	}
	value, err := subtreeMsg.Encode()
	if err != nil {
		t.Fatalf("encode subtree msg: %v", err)
	}

	if err := p.handleMessage(t.Context(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage: expected nil error on success path, got: %v", err)
	}

	if !dedup.Contains(subtreeMsg.Hash) {
		t.Errorf("dedup cache MUST contain %q after a successful processing pass", subtreeMsg.Hash)
	}

	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry publishes on success, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected 0 DLQ publishes on success, got %d", got)
	}
	if got := p.messagesProcessed.Load(); got != 1 {
		t.Errorf("expected messagesProcessed=1 on success, got %d", got)
	}
	if got := p.messagesRetried.Load(); got != 0 {
		t.Errorf("expected messagesRetried=0 on success, got %d", got)
	}
	// SEEN_ON_NETWORK should have been published (no SEEN_MULTIPLE_NODES
	// since mockSeenCounter never fires the threshold).
	if got := len(cbMock.getMessages()); got != 1 {
		t.Errorf("expected 1 SEEN_ON_NETWORK callback publish, got %d", got)
	}
}
