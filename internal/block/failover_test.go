package block

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// stubDataHubRegistry returns a fixed URL list for failover tests. Add
// is a no-op so the production code path (recording the resolved URL
// after success) does not need a real Aerospike.
type stubDataHubRegistry struct {
	urls    []string
	addedTo []string
}

func (s *stubDataHubRegistry) Add(url string) error {
	s.addedTo = append(s.addedTo, url)
	return nil
}

func (s *stubDataHubRegistry) GetAll() ([]string, error) {
	out := make([]string, len(s.urls))
	copy(out, s.urls)
	return out, nil
}

// buildProcessorWithRegistry mirrors buildProcessorWithProducer but
// also wires up a DataHubRegistry so the failover path has candidates.
func buildProcessorWithRegistry(t *testing.T, sp kafka.Publisher, reg store.DataHubRegistry) *Processor {
	t.Helper()
	logger := testLogger()
	dedup := cache.NewDedupCache(64)
	counter := newFakeSubtreeCounter()
	blobStore := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blobStore, 1, logger)

	p := &Processor{
		subtreeWorkProducer: kafka.NewTestProducer(sp, "subtree-work-test", logger),
		subtreeStore:        subtreeStore,
		subtreeCounter:      counter,
		dataHubRegistry:     reg,
		dedupCache:          dedup,
		dataHubClient:       datahub.NewClient(5, 0, logger),
	}
	p.InitBase("block-processor-failover-test")
	p.Logger = logger
	return p
}

// TestHandleMessage_FailoverToRegistryPeer verifies that when the
// announced DataHub returns 404, the processor falls over to a healthy
// peer from the registry and stamps the *resolved* URL onto every
// downstream subtree-work message.
func TestHandleMessage_FailoverToRegistryPeer(t *testing.T) {
	// Bad peer: always 404.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer bad.Close()

	// Good peer: serves the binary block payload.
	good := newDataHubServerWithSubtrees(t, 2)
	defer good.Close()

	mockProducer := &failingSyncProducer{failAt: -1}
	reg := &stubDataHubRegistry{urls: []string{good.URL}}
	p := buildProcessorWithRegistry(t, mockProducer, reg)

	const blockHash = "block-failover"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, bad.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected failover to succeed, got error: %v", err)
	}

	if got := len(mockProducer.messages); got != 2 {
		t.Fatalf("expected 2 published subtree-work messages, got %d", got)
	}

	// Every published subtree-work message must carry the GOOD URL,
	// not the announced bad one — otherwise subtree workers would
	// inherit the dead peer.
	for i, m := range mockProducer.messages {
		decoded, err := kafka.DecodeSubtreeWorkMessage(m.Value)
		if err != nil {
			t.Fatalf("decode subtree-work message %d: %v", i, err)
		}
		if decoded.DataHubURL != good.URL {
			t.Errorf("subtree-work message %d: DataHubURL = %q, want resolved URL %q",
				i, decoded.DataHubURL, good.URL)
		}
	}

	// The registry should record the resolved URL on success (so the bad
	// announced URL doesn't pollute the registry).
	if len(reg.addedTo) != 1 || reg.addedTo[0] != good.URL {
		t.Errorf("expected registry Add(%q), got %v", good.URL, reg.addedTo)
	}
}

// TestHandleMessage_FailoverSkipsUnhealthyCandidates verifies that an
// unhealthy peer in the registry is skipped, even if it would have
// served the block — peer health is enforced at the call site.
func TestHandleMessage_FailoverSkipsUnhealthyCandidates(t *testing.T) {
	// Two "registry" peers: one would serve, but it's pre-marked
	// unhealthy; the second one serves the block.
	healthyButWrongCalls := 0
	servedOK := newDataHubServerWithSubtrees(t, 1)
	defer servedOK.Close()
	unhealthyButCapable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If this peer is consulted, the test should fail — but we
		// also need to actually serve something to make the test
		// failure mode "extra request was made" rather than 404.
		healthyButWrongCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer unhealthyButCapable.Close()

	// Announced URL also 404s.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer bad.Close()

	mockProducer := &failingSyncProducer{failAt: -1}
	// Order: unhealthy peer first, then the good one.
	reg := &stubDataHubRegistry{urls: []string{unhealthyButCapable.URL, servedOK.URL}}
	p := buildProcessorWithRegistry(t, mockProducer, reg)

	// Attach a PeerHealth tracker and pre-mark unhealthyButCapable
	// unhealthy. The announced URL is exempt from the health check
	// (forceTry), so it will be tried first regardless.
	ph := datahub.NewPeerHealth(1, 10*time.Minute)
	p.dataHubClient.SetPeerHealth(ph)
	ph.RecordFailure(unhealthyButCapable.URL)

	const blockHash = "block-skip-unhealthy"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, bad.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected failover to good peer, got: %v", err)
	}
	if healthyButWrongCalls != 0 {
		t.Errorf("unhealthy peer should not have been consulted; got %d calls",
			healthyButWrongCalls)
	}
	if len(reg.addedTo) != 1 || reg.addedTo[0] != servedOK.URL {
		t.Errorf("expected registry Add(%q), got %v", servedOK.URL, reg.addedTo)
	}
}

// TestHandleMessage_FailoverAllPeersFailReturnsError verifies that when
// every candidate (announced + registry) fails, handleMessage returns a
// non-nil error so the Kafka offset is not committed.
func TestHandleMessage_FailoverAllPeersFailReturnsError(t *testing.T) {
	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer notFound.Close()

	mockProducer := &failingSyncProducer{failAt: -1}
	reg := &stubDataHubRegistry{urls: []string{notFound.URL}}
	p := buildProcessorWithRegistry(t, mockProducer, reg)

	const blockHash = "block-all-fail"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, notFound.URL),
	}

	err := p.handleMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error when every candidate 404s")
	}
	if !strings.Contains(err.Error(), "fetching block metadata") {
		t.Errorf("expected wrapped fetch error, got: %v", err)
	}
	if len(mockProducer.messages) != 0 {
		t.Errorf("no subtree work should be published on metadata failure; got %d",
			len(mockProducer.messages))
	}
}

// subtreeOnlyServer serves raw subtree bytes at /subtree/* and 404s
// everything else. served records whether it was hit.
func subtreeOnlyServer(payload []byte, served *bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/subtree/") {
			if served != nil {
				*served = true
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(payload)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

const testSubtreeHash = "078f3e8c684dfd1fe2e7e5a45a337c29bac886a00c0dc459be2a8f52c9078fde"

// TestFetchSubtreeRawWithFailover_PrunedPreferredPeer is the coinbase-BUMP
// scenario: the peer that served the block's metadata (preferred) has pruned
// this block's subtree (404), but a registry peer still serves it. The fetch
// must fail over to that peer instead of dropping the coinbase BUMP.
func TestFetchSubtreeRawWithFailover_PrunedPreferredPeer(t *testing.T) {
	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound) // subtree pruned here
	}))
	defer pruned.Close()

	want := make([]byte, 64) // two 32-byte nodes
	want[0], want[32] = 0x01, 0x02
	var goodHit bool
	good := subtreeOnlyServer(want, &goodHit)
	defer good.Close()

	reg := &stubDataHubRegistry{urls: []string{good.URL}}
	p := buildProcessorWithRegistry(t, &failingSyncProducer{failAt: -1}, reg)

	raw, resolved, err := p.fetchSubtreeRawWithFailover(
		context.Background(), "blk-pruned", testSubtreeHash, pruned.URL)
	if err != nil {
		t.Fatalf("expected failover to the registry peer, got error: %v", err)
	}
	if resolved != good.URL {
		t.Errorf("resolved = %q, want failover peer %q", resolved, good.URL)
	}
	if len(raw) != len(want) {
		t.Errorf("raw len = %d, want %d", len(raw), len(want))
	}
	if !goodHit {
		t.Error("failover peer was never queried")
	}
}

// TestFetchSubtreeRawWithFailover_PreferredServes verifies no failover when
// the preferred peer has the subtree (the common live-block case).
func TestFetchSubtreeRawWithFailover_PreferredServes(t *testing.T) {
	want := make([]byte, 32)
	want[0] = 0xAB
	good := subtreeOnlyServer(want, nil)
	defer good.Close()

	// A registry peer that would fail the test if it were ever queried.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("registry peer should not be queried when the preferred peer serves")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer other.Close()

	reg := &stubDataHubRegistry{urls: []string{other.URL}}
	p := buildProcessorWithRegistry(t, &failingSyncProducer{failAt: -1}, reg)

	raw, resolved, err := p.fetchSubtreeRawWithFailover(
		context.Background(), "blk", testSubtreeHash, good.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != good.URL {
		t.Errorf("resolved = %q, want preferred %q", resolved, good.URL)
	}
	if len(raw) != 32 {
		t.Errorf("raw len = %d, want 32", len(raw))
	}
}

// TestFetchSubtreeRawWithFailover_AllPeersMissing verifies a clean error
// (wrapping datahub.ErrNotFound) when no peer has the subtree.
func TestFetchSubtreeRawWithFailover_AllPeersMissing(t *testing.T) {
	notFound := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
	}
	a, b := notFound(), notFound()
	defer a.Close()
	defer b.Close()

	reg := &stubDataHubRegistry{urls: []string{b.URL}}
	p := buildProcessorWithRegistry(t, &failingSyncProducer{failAt: -1}, reg)

	_, _, err := p.fetchSubtreeRawWithFailover(
		context.Background(), "blk", testSubtreeHash, a.URL)
	if err == nil {
		t.Fatal("expected error when every peer 404s")
	}
	if !errors.Is(err, datahub.ErrNotFound) {
		t.Errorf("error should wrap datahub.ErrNotFound, got: %v", err)
	}
}
