package block

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"

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
func buildProcessorWithRegistry(t *testing.T, sp sarama.SyncProducer, reg store.DataHubRegistry) *Processor {
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
	msg := &sarama.ConsumerMessage{
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
		body, ok := m.Value.(sarama.ByteEncoder)
		if !ok {
			t.Fatalf("message %d: unexpected sarama.Encoder type %T", i, m.Value)
		}
		decoded, err := kafka.DecodeSubtreeWorkMessage([]byte(body))
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
	msg := &sarama.ConsumerMessage{
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
	msg := &sarama.ConsumerMessage{
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
