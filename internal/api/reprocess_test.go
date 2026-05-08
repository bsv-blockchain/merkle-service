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
	"sync/atomic"
	"testing"

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
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
	subtreeHash := &chainhash.Hash{}
	subtreeHash[0] = 0xab
	block, err := model.NewBlock(header, nil, []*chainhash.Hash{subtreeHash}, 0, 0, 1, 0)
	if err != nil {
		t.Fatalf("model.NewBlock: %v", err)
	}
	data, err := block.Bytes()
	if err != nil {
		t.Fatalf("block.Bytes: %v", err)
	}
	return data
}
