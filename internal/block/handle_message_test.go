package block

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// failingSyncProducer is a kafka.Publisher that fails on the Nth (0-indexed)
// Produce call. All earlier and later calls succeed (later calls aren't
// expected — the processor must stop on first failure).
type failingSyncProducer struct {
	mu       sync.Mutex
	messages []capturedMessage
	failAt   int // 0-indexed call to fail on; -1 means never fail
	failErr  error
	calls    int
}

func (f *failingSyncProducer) Produce(_ context.Context, key string, value []byte) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.calls
	f.calls++
	if f.failAt >= 0 && idx == f.failAt {
		return 0, 0, f.failErr
	}
	f.messages = append(f.messages, capturedMessage{Key: key, Value: value})
	return 0, int64(len(f.messages)), nil
}

func (f *failingSyncProducer) Close() error { return nil }

// fakeSubtreeCounter is an in-memory SubtreeCounterStore for tests. It records
// every call so tests can assert the order/count of Init invocations relative
// to publishing.
type fakeSubtreeCounter struct {
	mu        sync.Mutex
	values    map[string]int
	data      map[string]*store.BlockProcessedData
	initCalls int
	failNext  bool
}

func newFakeSubtreeCounter() *fakeSubtreeCounter {
	return &fakeSubtreeCounter{values: map[string]int{}, data: map[string]*store.BlockProcessedData{}}
}

func (f *fakeSubtreeCounter) Init(blockHash string, count int, data *store.BlockProcessedData) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initCalls++
	if f.failNext {
		f.failNext = false
		return errors.New("simulated counter init failure")
	}
	f.values[blockHash] = count
	f.data[blockHash] = data
	return nil
}

func (f *fakeSubtreeCounter) Decrement(blockHash string) (int, *store.BlockProcessedData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[blockHash]--
	if f.values[blockHash] <= 0 {
		return f.values[blockHash], f.data[blockHash], nil
	}
	return f.values[blockHash], nil, nil
}

func (f *fakeSubtreeCounter) get(blockHash string) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.values[blockHash]
	return v, ok
}

func (f *fakeSubtreeCounter) inits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.initCalls
}

// newBlockMessageBytes encodes a BlockMessage for use as a Kafka message value.
func newBlockMessageBytes(t *testing.T, hash, dataHubURL string) []byte {
	t.Helper()
	bm := &kafka.BlockMessage{
		Hash:       hash,
		Height:     200,
		DataHubURL: dataHubURL,
	}
	data, err := bm.Encode()
	if err != nil {
		t.Fatalf("encode block message: %v", err)
	}
	return data
}

// newDataHubServerWithSubtrees serves a binary block payload at /block/{hash}
// containing exactly the requested number of subtree slots.
func newDataHubServerWithSubtrees(t *testing.T, subtreeCount int) *httptest.Server {
	t.Helper()
	payload := buildBlockPayload(200, subtreeCount)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/block/") && !strings.HasSuffix(r.URL.Path, "/json") {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(payload)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

// buildProcessorWithProducer constructs a Processor wired up with the supplied
// sync producer and an in-memory dedup cache + counter for assertions.
func buildProcessorWithProducer(t *testing.T, sp kafka.Publisher) (*Processor, *fakeSubtreeCounter, *cache.DedupCache) {
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
		dedupCache:          dedup,
		dataHubClient:       datahub.NewClient(5, 0, logger),
	}
	p.InitBase("block-processor-test")
	p.Logger = logger
	return p, counter, dedup
}

// TestHandleMessage_HappyPath_AllPublished verifies the full success path:
// every subtree work message is published, the counter is initialized, and
// the block is added to the dedup cache.
func TestHandleMessage_HappyPath_AllPublished(t *testing.T) {
	mockProducer := &failingSyncProducer{failAt: -1}
	p, counter, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 3)
	defer server.Close()

	const blockHash = "block-happy"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected nil error on happy path, got: %v", err)
	}

	if got := len(mockProducer.messages); got != 3 {
		t.Errorf("expected 3 published messages, got %d", got)
	}
	if v, ok := counter.get(blockHash); !ok || v != 3 {
		t.Errorf("expected counter to be initialized to 3, got value=%d ok=%v", v, ok)
	}
	if !dedup.Contains(blockHash) {
		t.Errorf("expected block hash to be added to dedup cache after success")
	}
}

// TestHandleMessage_PublishFailureMidBatch_ReturnsErrorAndAttemptsAll verifies
// that when a publish fails partway through the batched fan-out:
//   - handleMessage returns a non-nil error,
//   - the block is NOT added to the dedup cache (so it can be retried),
//   - every record is still ATTEMPTED (batch semantics: kgo's ProduceSync
//     tries all records and reports the first failure; partially-landed work
//     is safe because the subtree-work pipeline is idempotent and the whole
//     batch re-publishes on redelivery).
func TestHandleMessage_PublishFailureMidBatch_ReturnsErrorAndAttemptsAll(t *testing.T) {
	// Fail on the 2nd publish (index 1): the other 3 of 4 records still send.
	mockProducer := &failingSyncProducer{
		failAt:  1,
		failErr: errors.New("kafka unavailable"),
	}
	p, counter, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 4)
	defer server.Close()

	const blockHash = "block-publish-fail"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	err := p.handleMessage(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected non-nil error when publish fails mid-batch")
	}
	if !strings.Contains(err.Error(), "publishing subtree work") {
		t.Errorf("expected wrapped publish error, got: %v", err)
	}

	if got := len(mockProducer.messages); got != 3 {
		t.Errorf("expected 3 successful sends (all 4 attempted, 1 injected failure), got %d", got)
	}
	if mockProducer.calls != 4 {
		t.Errorf("expected every record attempted (4 calls), got %d", mockProducer.calls)
	}
	if dedup.Contains(blockHash) {
		t.Errorf("block must NOT be in dedup cache when publish fails")
	}
	// Counter should have been initialized (so workers for the published
	// message can decrement); on retry, Init is upsert and overwrites cleanly.
	if v, ok := counter.get(blockHash); !ok || v != 4 {
		t.Errorf("expected counter initialized to 4, got value=%d ok=%v", v, ok)
	}
}

// TestHandleMessage_PublishFailureFirstMessage_NoDedup verifies that a
// failure on the very first record of the batch still surfaces an error and
// leaves the block uncommitted to dedup. The remaining records are still
// attempted (batch semantics) — partially-landed work is not a leak because
// the idempotent fan-out re-publishes the whole batch on redelivery.
func TestHandleMessage_PublishFailureFirstMessage_NoDedup(t *testing.T) {
	mockProducer := &failingSyncProducer{
		failAt:  0,
		failErr: errors.New("kafka unavailable"),
	}
	p, _, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 3)
	defer server.Close()

	const blockHash = "block-publish-fail-first"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err == nil {
		t.Fatalf("expected non-nil error when first publish fails")
	}
	if got := len(mockProducer.messages); got != 2 {
		t.Errorf("expected 2 successful sends (all 3 attempted, first injected to fail), got %d", got)
	}
	if dedup.Contains(blockHash) {
		t.Errorf("block must NOT be in dedup cache when fan-out fails")
	}
}

// TestHandleMessage_RetryAfterPublishFailure_Republishes verifies that after a
// publish failure, redelivering the same block message republishes the work
// and (on success) marks the block in dedup. The counter is re-initialized
// idempotently via the upsert semantics promised by the SubtreeCounterStore.
func TestHandleMessage_RetryAfterPublishFailure_Republishes(t *testing.T) {
	mockProducer := &failingSyncProducer{
		failAt:  1,
		failErr: errors.New("kafka unavailable"),
	}
	p, counter, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 3)
	defer server.Close()

	const blockHash = "block-retry"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err == nil {
		t.Fatalf("expected first attempt to fail")
	}
	if dedup.Contains(blockHash) {
		t.Fatalf("block must not be in dedup after failed attempt")
	}
	firstInits := counter.inits()

	// Producer is healthy on retry.
	mockProducer.failAt = -1

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if !dedup.Contains(blockHash) {
		t.Errorf("expected block in dedup after successful retry")
	}
	if counter.inits() <= firstInits {
		t.Errorf("expected counter to be re-initialized on retry")
	}
	// Total successful sends across both attempts: 2 on the failed attempt
	// (all 3 attempted, index 1 injected to fail) + 3 on the healthy retry.
	// The duplicates are exactly the documented idempotent-redelivery
	// property of the subtree-work pipeline.
	if mockProducer.calls != 6 || len(mockProducer.messages) != 5 {
		t.Errorf("unexpected producer state: calls=%d successful=%d, want calls=6 successful=5",
			mockProducer.calls, len(mockProducer.messages))
	}
}

// TestHandleMessage_CounterInitFailure_NoPublishNoDedup verifies that if the
// subtree counter cannot be initialized, no messages are published and the
// block is not marked in the dedup cache (so the block is retried).
func TestHandleMessage_CounterInitFailure_NoPublishNoDedup(t *testing.T) {
	mockProducer := &failingSyncProducer{failAt: -1}
	p, counter, dedup := buildProcessorWithProducer(t, mockProducer)
	counter.failNext = true

	server := newDataHubServerWithSubtrees(t, 2)
	defer server.Close()

	const blockHash = "block-counter-fail"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	err := p.handleMessage(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected non-nil error when counter init fails")
	}
	if len(mockProducer.messages) != 0 {
		t.Errorf("expected no publishes when counter init fails, got %d", len(mockProducer.messages))
	}
	if dedup.Contains(blockHash) {
		t.Errorf("block must not be in dedup when counter init fails")
	}
}

// TestHandleMessage_NoSubtrees_DedupAdded verifies that a block with zero
// subtrees is still recorded in dedup so a redelivery is fast-skipped.
func TestHandleMessage_NoSubtrees_DedupAdded(t *testing.T) {
	mockProducer := &failingSyncProducer{failAt: -1}
	p, _, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 0)
	defer server.Close()

	const blockHash = "block-empty"
	msg := &kafka.Message{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected nil error for empty block, got: %v", err)
	}
	if !dedup.Contains(blockHash) {
		t.Errorf("expected empty block to still be added to dedup cache")
	}
	if len(mockProducer.messages) != 0 {
		t.Errorf("expected no publishes for empty block, got %d", len(mockProducer.messages))
	}
}

// newEmptyBlockMessage encodes a BlockMessage for an empty (coinbase-only)
// block. Optional overrideURL/Token + bypassDedup mirror the /reprocess
// payload arcade sends.
func newEmptyBlockMessage(t *testing.T, hash, dataHubURL, overrideURL, overrideToken string, bypassDedup bool) []byte {
	t.Helper()
	bm := &kafka.BlockMessage{
		Hash:                  hash,
		Height:                200,
		DataHubURL:            dataHubURL,
		OverrideCallbackURL:   overrideURL,
		OverrideCallbackToken: overrideToken,
		BypassDedup:           bypassDedup,
	}
	data, err := bm.Encode()
	if err != nil {
		t.Fatalf("encode block message: %v", err)
	}
	return data
}

// TestHandleMessage_EmptyBlock_ReprocessEmitsBlockProcessed asserts that
// a coinbase-only block delivered via /reprocess (BypassDedup=true,
// OverrideCallbackURL set) emits exactly one BLOCK_PROCESSED callback to
// the override URL with the override token. Without this fix, arcade
// would never get a callback for empty reprocessed blocks and would
// retry /reprocess forever.
func TestHandleMessage_EmptyBlock_ReprocessEmitsBlockProcessed(t *testing.T) {
	subtreeWorkProducer := &failingSyncProducer{failAt: -1}
	callbackProducer := &failingSyncProducer{failAt: -1}
	p, _, dedup := buildProcessorWithProducer(t, subtreeWorkProducer)
	p.callbackProducer = kafka.NewTestProducer(callbackProducer, "callback-test", testLogger())

	server := newDataHubServerWithSubtrees(t, 0)
	defer server.Close()

	const (
		blockHash     = "block-empty-reprocess"
		overrideURL   = "https://arcade.example/api/v1/cb"
		overrideToken = "tok-arcade-1"
	)
	msg := &kafka.Message{
		Value: newEmptyBlockMessage(t, blockHash, server.URL, overrideURL, overrideToken, true),
	}

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if got := len(subtreeWorkProducer.messages); got != 0 {
		t.Errorf("expected no subtree-work publishes for empty block, got %d", got)
	}
	if got := len(callbackProducer.messages); got != 1 {
		t.Fatalf("expected exactly 1 BLOCK_PROCESSED callback publish, got %d", got)
	}

	// Decode and assert the callback message points at the override URL,
	// carries the override token, and is typed BLOCK_PROCESSED.
	payload := callbackProducer.messages[0].Value
	decoded, err := kafka.DecodeCallbackTopicMessage(payload)
	if err != nil {
		t.Fatalf("decode callback message: %v", err)
	}
	if decoded.Type != kafka.CallbackBlockProcessed {
		t.Errorf("expected type BLOCK_PROCESSED, got %q", decoded.Type)
	}
	if decoded.BlockHash != blockHash {
		t.Errorf("expected blockHash %q, got %q", blockHash, decoded.BlockHash)
	}
	if decoded.CallbackURL != overrideURL {
		t.Errorf("expected override URL %q, got %q", overrideURL, decoded.CallbackURL)
	}
	if decoded.CallbackToken != overrideToken {
		t.Errorf("expected override token %q, got %q", overrideToken, decoded.CallbackToken)
	}

	// Reprocess uses BypassDedup=true; the live-vs-reprocess contract says
	// reprocess must NOT pollute the dedup cache so a future live
	// announcement of the same hash still flows through normally.
	if dedup.Contains(blockHash) {
		t.Errorf("reprocess must not record empty block in dedup cache")
	}
}

// TestHandleMessage_EmptyBlock_LiveEmitsBlockProcessedToAllRegistered
// asserts that a live (non-reprocess) empty-block announcement broadcasts
// BLOCK_PROCESSED to every URL in the callback URL registry. The dedup
// cache is updated on the live path so a redelivered live announcement is
// fast-skipped.
func TestHandleMessage_EmptyBlock_LiveEmitsBlockProcessedToAllRegistered(t *testing.T) {
	subtreeWorkProducer := &failingSyncProducer{failAt: -1}
	callbackProducer := &failingSyncProducer{failAt: -1}
	p, _, dedup := buildProcessorWithProducer(t, subtreeWorkProducer)
	p.callbackProducer = kafka.NewTestProducer(callbackProducer, "callback-test", testLogger())
	p.urlRegistry = &fakeURLRegistry{
		urls: []string{
			"https://arcade-a.example/cb",
			"https://arcade-b.example/cb",
		},
		tokens: map[string]string{
			"https://arcade-a.example/cb": "tok-a",
		},
	}

	server := newDataHubServerWithSubtrees(t, 0)
	defer server.Close()

	const blockHash = "block-empty-live"
	msg := &kafka.Message{
		Value: newEmptyBlockMessage(t, blockHash, server.URL, "", "", false),
	}

	if err := p.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if got := len(subtreeWorkProducer.messages); got != 0 {
		t.Errorf("expected no subtree-work publishes for empty block, got %d", got)
	}
	if got := len(callbackProducer.messages); got != 2 {
		t.Fatalf("expected 2 BLOCK_PROCESSED publishes (one per registered URL), got %d", got)
	}

	// Verify each publish is a BLOCK_PROCESSED for blockHash and that both
	// registered URLs are represented.
	seen := map[string]string{}
	for _, m := range callbackProducer.messages {
		payload := m.Value
		decoded, err := kafka.DecodeCallbackTopicMessage(payload)
		if err != nil {
			t.Fatalf("decode callback message: %v", err)
		}
		if decoded.Type != kafka.CallbackBlockProcessed {
			t.Errorf("expected type BLOCK_PROCESSED, got %q", decoded.Type)
		}
		if decoded.BlockHash != blockHash {
			t.Errorf("blockHash mismatch: %q", decoded.BlockHash)
		}
		seen[decoded.CallbackURL] = decoded.CallbackToken
	}
	if _, ok := seen["https://arcade-a.example/cb"]; !ok {
		t.Errorf("arcade-a callback not published; got %v", seen)
	}
	if _, ok := seen["https://arcade-b.example/cb"]; !ok {
		t.Errorf("arcade-b callback not published; got %v", seen)
	}
	if seen["https://arcade-a.example/cb"] != "tok-a" {
		t.Errorf("arcade-a token mismatch: %q", seen["https://arcade-a.example/cb"])
	}

	if !dedup.Contains(blockHash) {
		t.Errorf("live empty-block announcement must record dedup")
	}
}
