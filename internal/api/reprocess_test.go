package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/go-chi/chi/v5"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

const fixtureBlockHash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

// recordingProducer captures Kafka publishes so /reprocess tests can assert
// on the enqueued BlockMessage without standing up a broker.
type recordingProducer struct {
	publishErr error
	keys       []string
	values     [][]byte
}

func (r *recordingProducer) PublishWithHashKey(key string, value []byte) error {
	if r.publishErr != nil {
		return r.publishErr
	}
	r.keys = append(r.keys, key)
	cp := make([]byte, len(value))
	copy(cp, value)
	r.values = append(r.values, cp)
	return nil
}

// fakeDataHubRegistry returns a fixed list and optionally an error.
type fakeDataHubRegistry struct {
	urls   []string
	getErr error
}

func (f *fakeDataHubRegistry) Add(string) error          { return nil }
func (f *fakeDataHubRegistry) GetAll() ([]string, error) { return f.urls, f.getErr }

// newReprocessRouter builds a chi router with the /reprocess route and the
// supplied dependencies. Avoids depending on api.Server.Init (which builds a
// listener) so tests stay hermetic.
func newReprocessRouter(s *Server) http.Handler {
	router := chi.NewRouter()
	router.Post("/reprocess", s.handleReprocess)
	return router
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// dataHubServer returns an httptest.Server that responds at the
// /block/<hash> path with the configured status. When status==200, the
// response body is the bytes returned by bodyFn(hash). bodyFn==nil writes
// an empty 200 body which the DataHub binary parser will reject — that
// path is not exercised by the success cases below (which use a real
// fixture). 404/5xx are easy to construct since they never read the body.
func dataHubServer(t *testing.T, status int, bodyFn func(hash string) []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status == http.StatusNotFound {
			http.NotFound(w, r)
			return
		}
		if status >= 500 {
			http.Error(w, "boom", status)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(status)
		if bodyFn != nil {
			_, _ = w.Write(bodyFn(r.URL.Path))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newReprocessServer(t *testing.T, deps *ReprocessDeps) *Server {
	t.Helper()
	s := &Server{}
	s.InitBase("test")
	s.Logger = discardLogger()
	s.SetReprocessDeps(deps)
	return s
}

func postReprocess(router http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/reprocess", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// TestHandleReprocess_NotConfigured returns 503 when the endpoint is wired
// without a DataHub client or block producer.
func TestHandleReprocess_NotConfigured(t *testing.T) {
	s := &Server{}
	s.InitBase("test")
	s.Logger = discardLogger()
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (body=%s)", w.Code, w.Body.String())
	}
}

func TestHandleReprocess_Validation(t *testing.T) {
	prod := &recordingProducer{}
	dataHub := datahub.NewClient(5, 0, discardLogger())
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient: dataHub,
		BlockProducer: nil, // must be supplied separately below
	})
	// SetReprocessDeps wraps a *kafka.Producer; for tests we bypass the
	// public setter and assign the interface field directly.
	s.blockProducer = prod
	router := newReprocessRouter(s)

	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing blockHash", `{"callbackUrl":"https://1.1.1.1/cb"}`, http.StatusBadRequest},
		{"bad blockHash", `{"blockHash":"abc","callbackUrl":"https://1.1.1.1/cb"}`, http.StatusBadRequest},
		{"missing callbackUrl", fmt.Sprintf(`{"blockHash":%q}`, fixtureBlockHash), http.StatusBadRequest},
		{"loopback callbackUrl rejected", fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"http://127.0.0.1/cb"}`, fixtureBlockHash), http.StatusBadRequest},
		{"bad json", `not json`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := postReprocess(router, tc.body)
			if w.Code != tc.want {
				t.Fatalf("expected %d, got %d (body=%s)", tc.want, w.Code, w.Body.String())
			}
		})
	}

	if len(prod.keys) != 0 {
		t.Fatalf("validation failures must not enqueue, got %d publishes", len(prod.keys))
	}
}

// TestHandleReprocess_NoCandidates returns 503 when no DataHub URLs are
// configured and the registry is empty.
func TestHandleReprocess_NoCandidates(t *testing.T) {
	prod := &recordingProducer{}
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient: datahub.NewClient(5, 0, discardLogger()),
		BlockProducer: nil,
	})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// TestHandleReprocess_AllCandidates404 returns 404 when every probed
// DataHub returns 404 — the block is genuinely unknown to the network.
func TestHandleReprocess_AllCandidates404(t *testing.T) {
	hub1 := dataHubServer(t, http.StatusNotFound, nil)
	hub2 := dataHubServer(t, http.StatusNotFound, nil)
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(5, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub1.URL, hub2.URL},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// TestHandleReprocess_All5xx returns 502 when every probe fails for
// non-404 reasons. The caller can't tell whether the block exists, so
// "bad gateway" rather than "not found" is the honest answer.
func TestHandleReprocess_All5xx(t *testing.T) {
	hub1 := dataHubServer(t, http.StatusInternalServerError, nil)
	hub2 := dataHubServer(t, http.StatusInternalServerError, nil)
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(2, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub1.URL, hub2.URL},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// TestHandleReprocess_MixedFailures returns 502 when at least one probe
// failed for non-404 reasons — we can't conclude the block doesn't exist.
func TestHandleReprocess_MixedFailures(t *testing.T) {
	hub1 := dataHubServer(t, http.StatusNotFound, nil)
	hub2 := dataHubServer(t, http.StatusInternalServerError, nil)
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(2, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub1.URL, hub2.URL},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// TestHandleReprocess_DedupesCandidates ensures /reprocess does not double-
// probe a URL that is both configured and in the discovered registry.
func TestHandleReprocess_DedupesCandidates(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.NotFound(w, &http.Request{})
	}))
	defer srv.Close()

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(2, 0, discardLogger()),
		FallbackDataHubURLs: []string{srv.URL},
		DataHubRegistry:     &fakeDataHubRegistry{urls: []string{srv.URL}},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected single probe (configured + discovered should dedupe), got %d", got)
	}
}

// TestHandleReprocess_SkipsUnhealthyDiscoveredPeer verifies that a peer
// marked unhealthy by the shared PeerHealth tracker is skipped from the
// /reprocess probe loop, while operator-configured fallbacks are still
// tried.
func TestHandleReprocess_SkipsUnhealthyDiscoveredPeer(t *testing.T) {
	var deadCalls, liveCalls atomic.Int32
	// "Discovered" peer that would return 500 if probed. Pre-marking it
	// unhealthy should keep its call count at zero.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deadCalls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer dead.Close()
	// Operator-configured fallback that 404s. Must be probed even though
	// the only discovered peer is unhealthy.
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		liveCalls.Add(1)
		http.NotFound(w, &http.Request{})
	}))
	defer live.Close()

	client := datahub.NewClient(2, 0, discardLogger())
	ph := datahub.NewPeerHealth(1, 10*time.Minute)
	client.SetPeerHealth(ph)
	ph.RecordFailure(dead.URL)

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       client,
		FallbackDataHubURLs: []string{live.URL},
		DataHubRegistry:     &fakeDataHubRegistry{urls: []string{dead.URL}},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (live 404 + dead skipped), got %d (body=%s)",
			w.Code, w.Body.String())
	}
	if deadCalls.Load() != 0 {
		t.Errorf("unhealthy discovered peer should not be probed, got %d calls",
			deadCalls.Load())
	}
	if liveCalls.Load() != 1 {
		t.Errorf("operator fallback should still be probed, got %d calls",
			liveCalls.Load())
	}
}

// TestHandleReprocess_AlwaysProbesOperatorFallbacks verifies that an
// operator-configured fallback URL is probed even when the tracker
// reports it unhealthy — operators trust these URLs, and our health
// view may be stale after a long quiet period.
func TestHandleReprocess_AlwaysProbesOperatorFallbacks(t *testing.T) {
	var calls atomic.Int32
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.NotFound(w, &http.Request{})
	}))
	defer hub.Close()

	client := datahub.NewClient(2, 0, discardLogger())
	ph := datahub.NewPeerHealth(1, 10*time.Minute)
	client.SetPeerHealth(ph)
	// Pre-mark the operator fallback unhealthy. It should be probed anyway.
	ph.RecordFailure(hub.URL)

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       client,
		FallbackDataHubURLs: []string{hub.URL},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (body=%s)", w.Code, w.Body.String())
	}
	if calls.Load() != 1 {
		t.Errorf("operator fallback must always be probed, got %d calls",
			calls.Load())
	}
}

// TestHandleReprocess_AllDiscoveredUnhealthyReturns502 verifies that if
// every candidate is a discovered peer marked unhealthy (and no operator
// fallbacks are configured), the endpoint returns 502 rather than 404 —
// we cannot honestly conclude the block is missing without probing
// anyone.
func TestHandleReprocess_AllDiscoveredUnhealthyReturns502(t *testing.T) {
	var calls atomic.Int32
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.NotFound(w, &http.Request{})
	}))
	defer dead.Close()

	client := datahub.NewClient(2, 0, discardLogger())
	ph := datahub.NewPeerHealth(1, 10*time.Minute)
	client.SetPeerHealth(ph)
	ph.RecordFailure(dead.URL)

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:   client,
		DataHubRegistry: &fakeDataHubRegistry{urls: []string{dead.URL}},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 (nothing probed), got %d (body=%s)",
			w.Code, w.Body.String())
	}
	if calls.Load() != 0 {
		t.Errorf("unhealthy peer should not be probed, got %d calls", calls.Load())
	}
}

// TestHandleReprocess_RegistryErrorFallsBackToConfigured tolerates a
// DataHub registry lookup error: the configured fallback list still
// serves the request.
func TestHandleReprocess_RegistryErrorFallsBackToConfigured(t *testing.T) {
	hub := dataHubServer(t, http.StatusNotFound, nil)
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(2, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub.URL},
		DataHubRegistry:     &fakeDataHubRegistry{getErr: errors.New("registry down")},
	})
	s.blockProducer = &recordingProducer{}
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
	w := postReprocess(router, body)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (registry error tolerated), got %d", w.Code)
	}
}

// TestHandleReprocess_PublishesScopedBlockMessage exercises the success
// path with a fixture DataHub. The fake DataHub returns a parsable
// model.Block so FetchBlockMetadata succeeds; we then assert the
// recorded BlockMessage carries the override fields and BypassDedup, and
// the partition key scopes to (blockHash | callbackURL).
func TestHandleReprocess_PublishesScopedBlockMessage(t *testing.T) {
	blockBody := tinyBlockFixture(t)
	hub := dataHubServer(t, http.StatusOK, func(string) []byte { return blockBody })
	prod := &recordingProducer{}
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(5, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub.URL},
	})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	const callbackURL = "https://1.1.1.1/arcade/cb"
	const callbackToken = "tok-reprocess"
	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":%q,"callbackToken":%q}`, fixtureBlockHash, callbackURL, callbackToken)
	w := postReprocess(router, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(prod.keys) != 1 {
		t.Fatalf("expected exactly one publish, got %d", len(prod.keys))
	}
	wantKey := fixtureBlockHash + "|" + callbackURL
	if prod.keys[0] != wantKey {
		t.Fatalf("partition key mismatch: got %q want %q", prod.keys[0], wantKey)
	}
	var msg kafka.BlockMessage
	if err := json.Unmarshal(prod.values[0], &msg); err != nil {
		t.Fatalf("decode publish payload: %v", err)
	}
	if msg.Hash != fixtureBlockHash {
		t.Errorf("Hash mismatch: %q", msg.Hash)
	}
	if msg.OverrideCallbackURL != callbackURL {
		t.Errorf("OverrideCallbackURL: got %q want %q", msg.OverrideCallbackURL, callbackURL)
	}
	if msg.OverrideCallbackToken != callbackToken {
		t.Errorf("OverrideCallbackToken: got %q want %q", msg.OverrideCallbackToken, callbackToken)
	}
	if !msg.BypassDedup {
		t.Errorf("expected BypassDedup=true on reprocess message")
	}
	if msg.DataHubURL != hub.URL {
		t.Errorf("DataHubURL: got %q want %q", msg.DataHubURL, hub.URL)
	}

	// 202 body echoes the resolved URL so the caller can correlate.
	var resp ReprocessResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode 202 body: %v", err)
	}
	if resp.Status != "queued" || resp.BlockHash != fixtureBlockHash || resp.DataHubURL != hub.URL {
		t.Errorf("response body: %+v", resp)
	}
}

// tinyBlockFixture builds a minimal teranode model.Block binary payload
// suitable for the DataHub /block/<hash> response. The block hash itself is
// not validated against the request hash by ParseBinaryBlockMetadata, so we
// only need the bytes to be a parsable block.
func tinyBlockFixture(t *testing.T) []byte {
	t.Helper()
	return blockFixtureWithSubtrees(t, 1)
}

// blockFixtureWithSubtrees builds a parsable block payload with the
// requested number of subtree hashes — used by the dedup-clear tests
// to verify the handler enumerates one STUMP dedup key per index.
func blockFixtureWithSubtrees(t *testing.T, subtreeCount int) []byte {
	t.Helper()
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
	subtrees := make([]*chainhash.Hash, subtreeCount)
	for i := range subtrees {
		h := &chainhash.Hash{}
		h[0] = byte(0xa0 + i)
		subtrees[i] = h
	}
	block, err := model.NewBlock(header, nil, subtrees, 0, 0, 1, 0)
	if err != nil {
		t.Fatalf("model.NewBlock: %v", err)
	}
	data, err := block.Bytes()
	if err != nil {
		t.Fatalf("block.Bytes: %v", err)
	}
	return data
}

// fakeCallbackDeduper is an in-memory store.CallbackDedupStore for
// asserting which dedup keys /reprocess clears. The internal key uses
// the same three-tuple shape that the real backends key on, so test
// assertions read naturally.
type fakeCallbackDeduper struct {
	mu        sync.Mutex
	entries   map[string]struct{}
	deletes   []dedupTriple
	deleteErr error
}

type dedupTriple struct {
	Txid, URL, StatusType string
}

func newFakeCallbackDeduper() *fakeCallbackDeduper {
	return &fakeCallbackDeduper{entries: map[string]struct{}{}}
}

func fakeDedupKey(t dedupTriple) string {
	return t.Txid + "\x00" + t.URL + "\x00" + t.StatusType
}

func (f *fakeCallbackDeduper) Exists(txid, url, st string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.entries[fakeDedupKey(dedupTriple{txid, url, st})]
	return ok, nil
}

func (f *fakeCallbackDeduper) Record(txid, url, st string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[fakeDedupKey(dedupTriple{txid, url, st})] = struct{}{}
	return nil
}

func (f *fakeCallbackDeduper) Delete(txid, url, st string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	tr := dedupTriple{txid, url, st}
	f.deletes = append(f.deletes, tr)
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.entries, fakeDedupKey(tr))
	return nil
}

func (f *fakeCallbackDeduper) takeDeletes() []dedupTriple {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]dedupTriple, len(f.deletes))
	copy(out, f.deletes)
	return out
}

// TestHandleReprocess_ClearsDedupBeforePublish pins the fix for
// bsv-blockchain/merkle-service#122: a /reprocess request first
// removes any callback-dedup entries left behind by a previous
// DLQ'd delivery for (blockHash, callbackURL), so callback-delivery
// doesn't silently swallow the freshly-emitted STUMPs and
// BLOCK_PROCESSED. With a 2-subtree fixture block, we expect
// exactly three deletes: BLOCK_PROCESSED, STUMP:0, STUMP:1.
func TestHandleReprocess_ClearsDedupBeforePublish(t *testing.T) {
	blockBody := blockFixtureWithSubtrees(t, 2)
	hub := dataHubServer(t, http.StatusOK, func(string) []byte { return blockBody })
	prod := &recordingProducer{}
	dedup := newFakeCallbackDeduper()

	const callbackURL = "https://1.1.1.1/arcade/cb"

	// Pre-populate dedup state to simulate the DLQ'd-prior-attempt
	// scenario from the bug.
	_ = dedup.Record(fixtureBlockHash, callbackURL, "BLOCK_PROCESSED", time.Hour)
	_ = dedup.Record(fixtureBlockHash+":0", callbackURL, "STUMP", time.Hour)
	_ = dedup.Record(fixtureBlockHash+":1", callbackURL, "STUMP", time.Hour)

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(5, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub.URL},
		DedupStore:          dedup,
	})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":%q}`, fixtureBlockHash, callbackURL)
	w := postReprocess(router, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%s)", w.Code, w.Body.String())
	}

	// Assert dedup-clear surface: three deletes, one per dedup key the
	// downstream pipeline could re-emit for this (blockHash, callbackURL).
	got := dedup.takeDeletes()
	want := map[dedupTriple]bool{
		{fixtureBlockHash, callbackURL, "BLOCK_PROCESSED"}: false,
		{fixtureBlockHash + ":0", callbackURL, "STUMP"}:    false,
		{fixtureBlockHash + ":1", callbackURL, "STUMP"}:    false,
	}
	for _, d := range got {
		if _, ok := want[d]; !ok {
			t.Errorf("unexpected Delete call: %+v", d)
			continue
		}
		want[d] = true
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("expected Delete for %+v but it was not called", k)
		}
	}

	// All three pre-populated entries should now be absent.
	for _, tr := range []dedupTriple{
		{fixtureBlockHash, callbackURL, "BLOCK_PROCESSED"},
		{fixtureBlockHash + ":0", callbackURL, "STUMP"},
		{fixtureBlockHash + ":1", callbackURL, "STUMP"},
	} {
		exists, err := dedup.Exists(tr.Txid, tr.URL, tr.StatusType)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Errorf("entry %+v should have been cleared", tr)
		}
	}

	// Block message was still published (the dedup-clear is best-effort
	// pre-publish; the publish itself must always run on probe success).
	if len(prod.keys) != 1 {
		t.Errorf("expected one block publish, got %d", len(prod.keys))
	}
}

// TestHandleReprocess_DedupClearFailureDoesNotBlockPublish verifies the
// best-effort contract: a Delete error from the dedup store is logged
// but does not block the publish. The user still gets 202.
func TestHandleReprocess_DedupClearFailureDoesNotBlockPublish(t *testing.T) {
	blockBody := blockFixtureWithSubtrees(t, 1)
	hub := dataHubServer(t, http.StatusOK, func(string) []byte { return blockBody })
	prod := &recordingProducer{}
	dedup := newFakeCallbackDeduper()
	dedup.deleteErr = errors.New("aerospike down")

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(5, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub.URL},
		DedupStore:          dedup,
	})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":%q}`, fixtureBlockHash, "https://1.1.1.1/cb")
	w := postReprocess(router, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 despite Delete errors, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(prod.keys) != 1 {
		t.Errorf("expected publish to proceed despite dedup-clear failures; got %d publishes", len(prod.keys))
	}
}

// TestHandleReprocess_NoDedupStoreConfigured verifies backward
// compatibility: when the deploy does not wire DedupStore, the handler
// proceeds with publish and no panic.
func TestHandleReprocess_NoDedupStoreConfigured(t *testing.T) {
	blockBody := blockFixtureWithSubtrees(t, 1)
	hub := dataHubServer(t, http.StatusOK, func(string) []byte { return blockBody })
	prod := &recordingProducer{}

	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(5, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub.URL},
		// DedupStore omitted on purpose.
	})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":%q}`, fixtureBlockHash, "https://1.1.1.1/cb")
	w := postReprocess(router, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(prod.keys) != 1 {
		t.Errorf("expected publish to proceed when DedupStore is nil; got %d publishes", len(prod.keys))
	}
}

// TestHandleReprocess_ZeroSubtreesClearsOnlyBlockProcessed pins the
// empty-block behavior: a coinbase-only block has no STUMP dedup
// entries, so only the BLOCK_PROCESSED key is cleared.
func TestHandleReprocess_ZeroSubtreesClearsOnlyBlockProcessed(t *testing.T) {
	blockBody := blockFixtureWithSubtrees(t, 0)
	hub := dataHubServer(t, http.StatusOK, func(string) []byte { return blockBody })
	prod := &recordingProducer{}
	dedup := newFakeCallbackDeduper()

	const callbackURL = "https://1.1.1.1/arcade/cb"
	s := newReprocessServer(t, &ReprocessDeps{
		DataHubClient:       datahub.NewClient(5, 0, discardLogger()),
		FallbackDataHubURLs: []string{hub.URL},
		DedupStore:          dedup,
	})
	s.blockProducer = prod
	router := newReprocessRouter(s)

	body := fmt.Sprintf(`{"blockHash":%q,"callbackUrl":%q}`, fixtureBlockHash, callbackURL)
	w := postReprocess(router, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (body=%s)", w.Code, w.Body.String())
	}
	got := dedup.takeDeletes()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 Delete (BLOCK_PROCESSED), got %d: %+v", len(got), got)
	}
	if got[0] != (dedupTriple{fixtureBlockHash, callbackURL, "BLOCK_PROCESSED"}) {
		t.Errorf("Delete target mismatch: got %+v", got[0])
	}
}
