package block

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// failingSyncProducer is a sarama.SyncProducer that fails on the Nth (0-indexed)
// SendMessage call. All earlier and later calls succeed (later calls aren't
// expected — the processor must stop on first failure).
type failingSyncProducer struct {
	mu       sync.Mutex
	messages []*sarama.ProducerMessage
	failAt   int // 0-indexed call to fail on; -1 means never fail
	failErr  error
	calls    int
}

func (f *failingSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.calls
	f.calls++
	if f.failAt >= 0 && idx == f.failAt {
		return 0, 0, f.failErr
	}
	f.messages = append(f.messages, msg)
	return 0, int64(len(f.messages)), nil
}

func (f *failingSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, m := range msgs {
		if _, _, err := f.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (f *failingSyncProducer) Close() error          { return nil }
func (f *failingSyncProducer) IsTransactional() bool { return false }
func (f *failingSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}
func (f *failingSyncProducer) BeginTxn() error  { return nil }
func (f *failingSyncProducer) CommitTxn() error { return nil }
func (f *failingSyncProducer) AbortTxn() error  { return nil }
func (f *failingSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (f *failingSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

// fakeSubtreeCounter is an in-memory SubtreeCounterStore for tests. It records
// every call so tests can assert the order/count of Init invocations relative
// to publishing.
type fakeSubtreeCounter struct {
	mu        sync.Mutex
	values    map[string]int
	initCalls int
	failNext  bool
}

func newFakeSubtreeCounter() *fakeSubtreeCounter {
	return &fakeSubtreeCounter{values: map[string]int{}}
}

func (f *fakeSubtreeCounter) Init(blockHash string, count int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initCalls++
	if f.failNext {
		f.failNext = false
		return errors.New("simulated counter init failure")
	}
	f.values[blockHash] = count
	return nil
}

func (f *fakeSubtreeCounter) Decrement(blockHash string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[blockHash]--
	return f.values[blockHash], nil
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

// newBlockMessageBytes encodes a BlockMessage for use as a sarama message value.
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
func buildProcessorWithProducer(t *testing.T, sp sarama.SyncProducer) (*Processor, *fakeSubtreeCounter, *cache.DedupCache) {
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

	blockHash := testHashFromLabel("block-happy")
	msg := &sarama.ConsumerMessage{
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

// TestHandleMessage_PublishFailureMidLoop_StopsAndReturnsError verifies that
// when a publish fails partway through the fan-out:
//   - handleMessage returns a non-nil error,
//   - the block is NOT added to the dedup cache (so it can be retried),
//   - publishing stops at the failing message (later messages are NOT sent).
func TestHandleMessage_PublishFailureMidLoop_StopsAndReturnsError(t *testing.T) {
	// Fail on the 2nd publish (index 1), so we expect exactly 1 successful
	// send before the failure aborts the loop.
	mockProducer := &failingSyncProducer{
		failAt:  1,
		failErr: errors.New("kafka unavailable"),
	}
	p, counter, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 4)
	defer server.Close()

	blockHash := testHashFromLabel("block-publish-fail")
	msg := &sarama.ConsumerMessage{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	err := p.handleMessage(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected non-nil error when publish fails mid-loop")
	}
	if !strings.Contains(err.Error(), "publishing subtree work") {
		t.Errorf("expected wrapped publish error, got: %v", err)
	}

	if got := len(mockProducer.messages); got != 1 {
		t.Errorf("expected exactly 1 successful send before failure, got %d", got)
	}
	if mockProducer.calls != 2 {
		t.Errorf("expected loop to stop after first failure (2 calls total), got %d", mockProducer.calls)
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

// TestHandleMessage_PublishFailureFirstMessage_NoMessagesLeak verifies that a
// failure on the very first publish leaves no messages dispatched and the
// block uncommitted to dedup.
func TestHandleMessage_PublishFailureFirstMessage_NoMessagesLeak(t *testing.T) {
	mockProducer := &failingSyncProducer{
		failAt:  0,
		failErr: errors.New("kafka unavailable"),
	}
	p, _, dedup := buildProcessorWithProducer(t, mockProducer)

	server := newDataHubServerWithSubtrees(t, 3)
	defer server.Close()

	blockHash := testHashFromLabel("block-publish-fail-first")
	msg := &sarama.ConsumerMessage{
		Value: newBlockMessageBytes(t, blockHash, server.URL),
	}

	if err := p.handleMessage(context.Background(), msg); err == nil {
		t.Fatalf("expected non-nil error when first publish fails")
	}
	if got := len(mockProducer.messages); got != 0 {
		t.Errorf("expected zero successful sends, got %d", got)
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

	blockHash := testHashFromLabel("block-retry")
	msg := &sarama.ConsumerMessage{
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
	// Total successful sends across both attempts: 1 (before failure) + 3 (retry).
	if mockProducer.calls < 5 || len(mockProducer.messages) != 4 {
		t.Errorf("unexpected producer state: calls=%d successful=%d",
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

	blockHash := testHashFromLabel("block-counter-fail")
	msg := &sarama.ConsumerMessage{
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

	blockHash := testHashFromLabel("block-empty")
	msg := &sarama.ConsumerMessage{
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
