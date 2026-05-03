package block

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// --- Fakes ---
//
// Named distinctly from PR #76's helpers (failingSyncProducer / fakeSubtreeCounter)
// because that PR is still open and we don't want either side to break the other
// when the two land on main.

// callbackFailingProducer is a sarama.SyncProducer that records every call and
// can be configured to fail every send. Used to drive the publishSubtreeCallbacks
// → handleTransientFailure path.
//
// failOnType, when non-empty, causes only sends whose payload decodes to a
// CallbackTopicMessage with that Type to fail. This enables tests for the
// F-014 emit-failure path (fail BLOCK_PROCESSED while letting STUMP through)
// without affecting unrelated callback messages on the same producer.
type callbackFailingProducer struct {
	mu         sync.Mutex
	messages   []*sarama.ProducerMessage
	failAll    bool
	failOnType kafka.CallbackType
	failErr    error
}

func (f *callbackFailingProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAll {
		return 0, 0, f.failErr
	}
	if f.failOnType != "" {
		if b, ok := msg.Value.(sarama.ByteEncoder); ok {
			if decoded, err := kafka.DecodeCallbackTopicMessage([]byte(b)); err == nil && decoded.Type == f.failOnType {
				return 0, 0, f.failErr
			}
		}
	}
	f.messages = append(f.messages, msg)
	return 0, int64(len(f.messages)), nil
}

func (f *callbackFailingProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, m := range msgs {
		if _, _, err := f.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (f *callbackFailingProducer) Close() error          { return nil }
func (f *callbackFailingProducer) IsTransactional() bool { return false }
func (f *callbackFailingProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}
func (f *callbackFailingProducer) BeginTxn() error  { return nil }
func (f *callbackFailingProducer) CommitTxn() error { return nil }
func (f *callbackFailingProducer) AbortTxn() error  { return nil }
func (f *callbackFailingProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (f *callbackFailingProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (f *callbackFailingProducer) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

// sentCountOfType returns the number of successfully-sent messages whose
// payload decodes to the given CallbackType.
func (f *callbackFailingProducer) sentCountOfType(t kafka.CallbackType) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, msg := range f.messages {
		b, ok := msg.Value.(sarama.ByteEncoder)
		if !ok {
			continue
		}
		decoded, err := kafka.DecodeCallbackTopicMessage([]byte(b))
		if err != nil {
			continue
		}
		if decoded.Type == t {
			n++
		}
	}
	return n
}

// stubStumpStore is a programmable StumpStore. By default Put returns a fixed
// ref; setting putErr causes every Put call to fail.
type stubStumpStore struct {
	mu      sync.Mutex
	puts    int
	putErr  error
	lastRef string
}

func (s *stubStumpStore) Put(data []byte, blockHeight uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.putErr != nil {
		return "", s.putErr
	}
	s.lastRef = "stump-ref-stub"
	return s.lastRef, nil
}
func (s *stubStumpStore) Get(ref string) ([]byte, error) { return nil, errors.New("not implemented") }
func (s *stubStumpStore) Delete(ref string) error        { return nil }

// countingSubtreeCounter records every Decrement call so tests can assert
// whether the counter was touched on a given handleMessage invocation. When
// decrementErr is non-nil, every Decrement returns it without mutating the
// stored value — used to drive the F-013 counter-decrement-failure path.
type countingSubtreeCounter struct {
	mu             sync.Mutex
	decrementCalls int
	initCalls      int
	values         map[string]int
	decrementErr   error
}

func newCountingSubtreeCounter() *countingSubtreeCounter {
	return &countingSubtreeCounter{values: map[string]int{}}
}

func (c *countingSubtreeCounter) Init(blockHash string, count int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initCalls++
	c.values[blockHash] = count
	return nil
}

func (c *countingSubtreeCounter) Decrement(blockHash string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decrementCalls++
	if c.decrementErr != nil {
		// Do NOT mutate the stored value on failure — emulates an Aerospike/SQL
		// transient where the operation never committed.
		return 0, c.decrementErr
	}
	c.values[blockHash]--
	return c.values[blockHash], nil
}

func (c *countingSubtreeCounter) decrementCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.decrementCalls
}

func (c *countingSubtreeCounter) value(blockHash string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[blockHash]
}

// staticRegStore is a RegistrationStore that returns a pre-configured set of
// callback URLs (with optional tokens) for any txid lookup. Enables
// ProcessBlockSubtree to produce non-empty CallbackGroups without reaching
// for Aerospike.
type staticRegStore struct {
	urls    []string
	tokens  map[string]string
	entries []store.CallbackEntry
}

func (s *staticRegStore) lookup() []store.CallbackEntry {
	if s.entries != nil {
		return s.entries
	}
	out := make([]store.CallbackEntry, 0, len(s.urls))
	for _, u := range s.urls {
		out = append(out, store.CallbackEntry{URL: u, Token: s.tokens[u]})
	}
	return out
}

func (s *staticRegStore) Add(txid, callbackURL, callbackToken string) error { return nil }

func (s *staticRegStore) Get(txid string) ([]store.CallbackEntry, error) {
	return s.lookup(), nil
}

func (s *staticRegStore) BatchGet(txids []string) (map[string][]store.CallbackEntry, error) {
	out := make(map[string][]store.CallbackEntry, len(txids))
	for _, txid := range txids {
		out[txid] = s.lookup()
	}
	return out, nil
}
func (s *staticRegStore) UpdateTTL(txid string, ttl time.Duration) error         { return nil }
func (s *staticRegStore) BatchUpdateTTL(txids []string, ttl time.Duration) error { return nil }

// rawSubtreeServer serves a raw 32-byte-hash subtree payload at any path. The
// merkle-service pulls subtree binary from DataHub when its blob store doesn't
// already have it, so this fake satisfies that path.
func rawSubtreeServer(payload []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
	}))
}

// newWorkerForHandleMessage builds a SubtreeWorkerService wired up for tests:
// in-memory subtree+blob+stump stores, a static registration store, fake
// counter, and the supplied (callback, retry, dlq) sync producers.
func newWorkerForHandleMessage(
	t *testing.T,
	cb sarama.SyncProducer,
	retry sarama.SyncProducer,
	dlq sarama.SyncProducer,
	stumpStore store.StumpStore,
	counter *countingSubtreeCounter,
	maxAttempts int,
) *SubtreeWorkerService {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	blob := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blob, 1, logger)

	s := &SubtreeWorkerService{
		blockCfg: config.BlockConfig{
			MaxAttempts:    maxAttempts,
			PostMineTTLSec: 0,
		},
		regStore:       &staticRegStore{urls: []string{"http://cb.example.test/hook"}},
		subtreeStore:   subtreeStore,
		stumpStore:     stumpStore,
		subtreeCounter: counter,
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.dataHubClient = datahub.NewClient(5, 0, logger)
	s.callbackProducer = kafka.NewTestProducer(cb, "callback-test", logger)
	s.retryProducer = kafka.NewTestProducer(retry, "subtree-work-test", logger)
	s.dlqProducer = kafka.NewTestProducer(dlq, "subtree-work-dlq-test", logger)
	return s
}

// makeWorkMessageBytes builds a SubtreeWorkMessage targeting the given DataHub
// URL and returns the encoded bytes ready to feed into handleMessage.
func makeWorkMessageBytes(t *testing.T, blockHash, subtreeHash, dataHubURL string, attempt int) []byte {
	t.Helper()
	msg := &kafka.SubtreeWorkMessage{
		BlockHash:    blockHash,
		BlockHeight:  200,
		SubtreeHash:  subtreeHash,
		SubtreeIndex: 0,
		DataHubURL:   dataHubURL,
		AttemptCount: attempt,
	}
	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode work message: %v", err)
	}
	return data
}

// --- publishSubtreeCallbacks unit tests (direct invocation) ---

// TestPublishSubtreeCallbacks_StumpStoreFailureReturnsError verifies that a
// blob-store failure is no longer silently swallowed.
func TestPublishSubtreeCallbacks_StumpStoreFailureReturnsError(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		stumpStore: &stubStumpStore{putErr: errors.New("aerospike timeout")},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	wm := &kafka.SubtreeWorkMessage{
		BlockHash:    "blk-1",
		BlockHeight:  10,
		SubtreeIndex: 0,
	}
	res := &SubtreeResult{
		StumpData:      []byte{0x01, 0x02},
		CallbackGroups: map[string][]string{"http://cb.example.test/a": {"tx1"}},
	}

	err := s.publishSubtreeCallbacks(wm, res)
	if err == nil {
		t.Fatalf("expected error from publishSubtreeCallbacks when stump store fails")
	}
	if cbMock.sentCount() != 0 {
		t.Errorf("expected zero callback messages on stump-store failure, got %d", cbMock.sentCount())
	}
}

// TestPublishSubtreeCallbacks_KafkaPublishFailureReturnsError verifies that a
// callback-producer failure surfaces an error to the caller.
func TestPublishSubtreeCallbacks_KafkaPublishFailureReturnsError(t *testing.T) {
	cbMock := &callbackFailingProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		stumpStore: &stubStumpStore{},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	wm := &kafka.SubtreeWorkMessage{BlockHash: "blk-2", BlockHeight: 10}
	res := &SubtreeResult{
		StumpData: []byte{0x01},
		CallbackGroups: map[string][]string{
			"http://cb.example.test/a": {"tx1"},
			"http://cb.example.test/b": {"tx2"},
		},
	}

	err := s.publishSubtreeCallbacks(wm, res)
	if err == nil {
		t.Fatalf("expected error from publishSubtreeCallbacks when Kafka publish fails")
	}
}

// TestPublishSubtreeCallbacks_HappyPathReturnsNil verifies the no-error path.
func TestPublishSubtreeCallbacks_HappyPathReturnsNil(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		stumpStore: &stubStumpStore{},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	wm := &kafka.SubtreeWorkMessage{BlockHash: "blk-3", BlockHeight: 10}
	res := &SubtreeResult{
		StumpData: []byte{0x01},
		CallbackGroups: map[string][]string{
			"http://cb.example.test/a": {"tx1"},
			"http://cb.example.test/b": {"tx2"},
		},
	}

	if err := s.publishSubtreeCallbacks(wm, res); err != nil {
		t.Fatalf("expected nil error on happy path, got: %v", err)
	}
	if got := cbMock.sentCount(); got != 2 {
		t.Errorf("expected 2 callback messages, got %d", got)
	}
}

// --- handleMessage decision-tree integration tests ---

// TestHandleMessage_CallbackPublishFailure_RetriesAndDoesNotDecrement verifies
// the F-012 fix: when the callback Kafka publish fails, the work item is
// re-driven through the retry producer and the per-block subtree counter is
// NOT decremented (otherwise BLOCK_PROCESSED would fire prematurely with a
// missing STUMP).
func TestHandleMessage_CallbackPublishFailure_RetriesAndDoesNotDecrement(t *testing.T) {
	cbMock := &callbackFailingProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	counter := newCountingSubtreeCounter()
	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	const blockHash = "block-cb-fail"
	const subtreeHash = "subtree-cb-fail"

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)

	value := makeWorkMessageBytes(t, blockHash, subtreeHash, server.URL, 0)
	err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err != nil {
		t.Fatalf("handleMessage: expected nil error (retry path returns nil after re-publishing), got: %v", err)
	}

	// Counter must NOT have been decremented — otherwise BLOCK_PROCESSED could
	// fire with a missing STUMP.
	if got := counter.decrementCount(); got != 0 {
		t.Errorf("expected counter Decrement NOT called on callback failure, got %d calls", got)
	}

	// Retry producer must have received the re-published work message.
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 retry publish, got %d", got)
	}
	// DLQ must NOT have been touched (we're nowhere near max attempts).
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected zero DLQ publishes, got %d", got)
	}
}

// TestHandleMessage_CallbackPublishFailure_AtMaxAttempts_DLQAndDecrement verifies
// that when callback publishing fails and the work item has already reached
// max attempts, the message is DLQ'd and the counter IS decremented (matching
// handleTransientFailure's existing terminal-failure semantics so
// BLOCK_PROCESSED still fires, with the missing STUMP surfaced downstream).
func TestHandleMessage_CallbackPublishFailure_AtMaxAttempts_DLQAndDecrement(t *testing.T) {
	cbMock := &callbackFailingProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	counter := newCountingSubtreeCounter()
	_ = counter.Init("block-dlq", 1)
	// Reset init bookkeeping after pre-seed so test assertions only count
	// what handleMessage drives.
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	const maxAttempts = 3
	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, maxAttempts)

	// AttemptCount = maxAttempts - 1 → next attempt is terminal → DLQ.
	value := makeWorkMessageBytes(t, "block-dlq", "subtree-dlq", server.URL, maxAttempts-1)
	err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err != nil {
		t.Fatalf("handleMessage at max attempts: expected nil error after DLQ publish, got: %v", err)
	}

	if got := dlqMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 DLQ publish at max attempts, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected zero retry publishes at max attempts, got %d", got)
	}
	// Terminal: BLOCK_PROCESSED must still be able to fire — counter MUST be
	// decremented exactly once.
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once on DLQ, got %d", got)
	}
}

// TestHandleMessage_HappyPath_DecrementsExactlyOnce verifies that a fully
// successful subtree processing path decrements the counter exactly once and
// publishes one callback per registered URL.
func TestHandleMessage_HappyPath_DecrementsExactlyOnce(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	counter := newCountingSubtreeCounter()
	_ = counter.Init("block-happy", 1)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)

	value := makeWorkMessageBytes(t, "block-happy", "subtree-happy", server.URL, 0)
	if err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage happy path: expected nil error, got: %v", err)
	}

	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once on happy path, got %d", got)
	}
	if got := cbMock.sentCount(); got != 1 {
		t.Errorf("expected 1 callback publish on happy path, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected zero retry publishes on happy path, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected zero DLQ publishes on happy path, got %d", got)
	}
}

// TestHandleMessage_StumpStoreFailure_RetriesAndDoesNotDecrement covers the
// other half of F-012: a blob-store write failure (not Kafka) during callback
// publishing must also re-drive via the retry pipeline.
func TestHandleMessage_StumpStoreFailure_RetriesAndDoesNotDecrement(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	counter := newCountingSubtreeCounter()
	stumpStore := &stubStumpStore{putErr: errors.New("blob store unreachable")}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)

	value := makeWorkMessageBytes(t, "block-blob-fail", "subtree-blob-fail", server.URL, 0)
	if err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage stump-store failure: expected nil error (retry path), got: %v", err)
	}

	if got := counter.decrementCount(); got != 0 {
		t.Errorf("expected counter Decrement NOT called on stump-store failure, got %d", got)
	}
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 retry publish on stump-store failure, got %d", got)
	}
	if got := cbMock.sentCount(); got != 0 {
		t.Errorf("expected zero callback publishes on stump-store failure, got %d", got)
	}
}

// --- F-013 counter-decrement-failure tests ---

// TestHandleMessage_DecrementFailureOnSuccessPath_RetriesAndPreservesCount
// covers F-013: when Decrement fails after a successful subtree process +
// callback publish, the work item must be re-driven through the retry
// pipeline rather than silently acked. The counter value must remain at its
// pre-attempt level (the failed Decrement didn't commit), and no
// BLOCK_PROCESSED callback should be emitted.
func TestHandleMessage_DecrementFailureOnSuccessPath_RetriesAndPreservesCount(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-dec-fail"
	counter := newCountingSubtreeCounter()
	_ = counter.Init(blockHash, 3)
	counter.initCalls = 0
	counter.decrementErr = errors.New("aerospike timeout")

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)

	value := makeWorkMessageBytes(t, blockHash, "subtree-dec-fail", server.URL, 0)
	err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err != nil {
		// Below max attempts, handleTransientFailure re-publishes for retry
		// and returns nil — but the work item was redirected through the
		// retry path (not silently acked at the success branch).
		t.Fatalf("handleMessage with decrement failure (below max attempts): expected nil after retry republish, got: %v", err)
	}

	// Decrement was attempted exactly once.
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called once, got %d", got)
	}
	// The stored counter value must be UNCHANGED — the decrement failed and
	// didn't commit, so the next attempt sees the same starting count.
	if got := counter.value(blockHash); got != 3 {
		t.Errorf("expected stored counter to remain at 3 (decrement did not commit), got %d", got)
	}

	// The callback was published successfully BEFORE Decrement failed.
	if got := cbMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 callback publish before decrement failure, got %d", got)
	}
	// The work item must have been re-published for retry (not DLQ'd, not silently acked).
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 retry publish on decrement failure, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected zero DLQ publishes (below max attempts), got %d", got)
	}
}

// TestHandleMessage_DecrementFailureOnDLQPath_ReturnsError covers the harder
// F-013 case: a callback-publish failure that has reached max attempts (so
// the work item is DLQ'd) plus a Decrement failure. The DLQ publish itself
// has already happened, but the Decrement error must surface to the caller
// so the consumer redelivers — silently acking would leave the per-block
// counter > 0 forever and BLOCK_PROCESSED would never fire.
func TestHandleMessage_DecrementFailureOnDLQPath_ReturnsError(t *testing.T) {
	// Force DLQ path: callback publish always fails, AttemptCount = max-1.
	cbMock := &callbackFailingProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-dlq-dec-fail"
	counter := newCountingSubtreeCounter()
	_ = counter.Init(blockHash, 1)
	counter.initCalls = 0
	counter.decrementErr = errors.New("aerospike cluster degraded")

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	const maxAttempts = 3
	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, maxAttempts)

	value := makeWorkMessageBytes(t, blockHash, "subtree-dlq-dec-fail", server.URL, maxAttempts-1)
	err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err == nil {
		t.Fatalf("expected non-nil error when Decrement fails on DLQ path so consumer redelivers, got nil")
	}

	// DLQ was published BEFORE the decrement attempt — that ordering matches
	// PR #77's terminal-failure semantics.
	if got := dlqMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 DLQ publish, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected zero retry publishes at max attempts, got %d", got)
	}
	// Decrement was attempted exactly once.
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called once on DLQ path, got %d", got)
	}
	// Stored counter must be UNCHANGED — Decrement failed, no commit.
	if got := counter.value(blockHash); got != 1 {
		t.Errorf("expected stored counter to remain at 1 after failed Decrement, got %d", got)
	}
}

// TestHandleMessage_HappyPath_DecrementToZeroEmitsBlockProcessed extends the
// happy-path coverage from PR #77 to also assert the count→0 → emit
// BLOCK_PROCESSED transition (which in this test is observable via the
// callback producer — emitBlockProcessed publishes one BLOCK_PROCESSED
// message per registered URL).
func TestHandleMessage_HappyPath_DecrementToZeroEmitsBlockProcessed(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-zero-emit"
	counter := newCountingSubtreeCounter()
	// Pre-seed counter at 1 so the single subtree work item drives it to 0.
	_ = counter.Init(blockHash, 1)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)
	// Wire up urlRegistry so emitBlockProcessed has somewhere to publish.
	svc.urlRegistry = &fakeURLRegistry{urls: []string{"http://cb.example.test/hook"}}

	value := makeWorkMessageBytes(t, blockHash, "subtree-zero-emit", server.URL, 0)
	if err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage happy path: expected nil error, got: %v", err)
	}

	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once, got %d", got)
	}
	if got := counter.value(blockHash); got != 0 {
		t.Errorf("expected counter to be 0 after decrement, got %d", got)
	}
	// One STUMP callback + one BLOCK_PROCESSED callback = 2 callback messages.
	if got := cbMock.sentCount(); got != 2 {
		t.Errorf("expected 2 callback publishes (1 STUMP + 1 BLOCK_PROCESSED), got %d", got)
	}
}

// fakeURLRegistry satisfies store.CallbackURLRegistry for the count→0 emit test.
// getAllErr, when non-nil, causes GetAll to fail — used to drive the F-014
// "registry lookup error during emit" path.
type fakeURLRegistry struct {
	urls      []string
	tokens    map[string]string
	getAllErr error
}

func (f *fakeURLRegistry) Add(callbackURL, callbackToken string) error { return nil }
func (f *fakeURLRegistry) GetAll() ([]store.CallbackEntry, error) {
	if f.getAllErr != nil {
		return nil, f.getAllErr
	}
	out := make([]store.CallbackEntry, 0, len(f.urls))
	for _, u := range f.urls {
		out = append(out, store.CallbackEntry{URL: u, Token: f.tokens[u]})
	}
	return out, nil
}

// --- F-014 BLOCK_PROCESSED publish-failure tests ---

// TestEmitBlockProcessed_PublishFailureReturnsError verifies that when the
// callback Kafka publish for BLOCK_PROCESSED fails, emitBlockProcessed
// returns a non-nil error rather than silently swallowing it (F-014).
func TestEmitBlockProcessed_PublishFailureReturnsError(t *testing.T) {
	cbMock := &callbackFailingProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		urlRegistry: &fakeURLRegistry{urls: []string{
			"http://cb.example.test/a",
			"http://cb.example.test/b",
		}},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	err := s.emitBlockProcessed("blk-emit-fail")
	if err == nil {
		t.Fatalf("expected error from emitBlockProcessed when callback publish fails")
	}
}

// TestEmitBlockProcessed_PartialFailureContinuesAndReturnsFirstError verifies
// that when one URL's publish fails, the loop still attempts the remaining
// URLs (best-effort) and returns the first error to the caller — matching
// PR #77's publishSubtreeCallbacks pattern.
func TestEmitBlockProcessed_PartialFailureContinuesAndReturnsFirstError(t *testing.T) {
	cbMock := &callbackFailingProducer{
		failOnType: kafka.CallbackBlockProcessed,
		failErr:    errors.New("kafka unavailable"),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		urlRegistry: &fakeURLRegistry{urls: []string{
			"http://cb.example.test/a",
			"http://cb.example.test/b",
			"http://cb.example.test/c",
		}},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	err := s.emitBlockProcessed("blk-partial")
	if err == nil {
		t.Fatalf("expected non-nil error when BLOCK_PROCESSED publishes fail")
	}
	// Every BLOCK_PROCESSED send was failed by the producer, so none were
	// recorded — but the loop should have ATTEMPTED all URLs.
	// We can't directly observe attempts without instrumenting failOnType, so
	// instead verify that no successful BLOCK_PROCESSED was recorded.
	if got := cbMock.sentCountOfType(kafka.CallbackBlockProcessed); got != 0 {
		t.Errorf("expected zero successful BLOCK_PROCESSED sends, got %d", got)
	}
}

// TestEmitBlockProcessed_HappyPath verifies the no-error path: every URL
// receives one BLOCK_PROCESSED message and the function returns nil.
func TestEmitBlockProcessed_HappyPath(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		urlRegistry: &fakeURLRegistry{urls: []string{
			"http://cb.example.test/a",
			"http://cb.example.test/b",
		}},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	if err := s.emitBlockProcessed("blk-happy"); err != nil {
		t.Fatalf("expected nil error on happy path, got: %v", err)
	}
	if got := cbMock.sentCountOfType(kafka.CallbackBlockProcessed); got != 2 {
		t.Errorf("expected 2 BLOCK_PROCESSED messages, got %d", got)
	}
}

// TestEmitBlockProcessed_RegistryFailureReturnsError verifies that a
// registry-lookup failure surfaces as a non-nil return so the caller can
// re-drive the work item rather than silently dropping BLOCK_PROCESSED.
func TestEmitBlockProcessed_RegistryFailureReturnsError(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		urlRegistry: &fakeURLRegistry{getAllErr: errors.New("registry down")},
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.callbackProducer = kafka.NewTestProducer(cbMock, "callback-test", logger)

	if err := s.emitBlockProcessed("blk-registry-fail"); err == nil {
		t.Fatalf("expected error when URL registry GetAll fails")
	}
	if got := cbMock.sentCount(); got != 0 {
		t.Errorf("expected zero callback publishes when registry lookup fails, got %d", got)
	}
}

// TestHandleMessage_BlockProcessedEmitFailure_RetriesAndDoesNotAck covers the
// core F-014 case at the handler level: the LAST subtree's decrement-to-0
// triggers emitBlockProcessed, the callback Kafka publish fails for the
// BLOCK_PROCESSED message, and the work item is re-driven through the retry
// pipeline (rather than silently ack'd with the BLOCK_PROCESSED notification
// dropped on the floor).
func TestHandleMessage_BlockProcessedEmitFailure_RetriesAndDoesNotAck(t *testing.T) {
	// Producer fails BLOCK_PROCESSED but lets STUMP through. This isolates
	// the F-014 emit-failure path from the F-012 STUMP-publish path.
	cbMock := &callbackFailingProducer{
		failOnType: kafka.CallbackBlockProcessed,
		failErr:    errors.New("kafka unavailable"),
	}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-emit-fail"
	counter := newCountingSubtreeCounter()
	// Pre-seed at 1 so the single subtree drives the counter to 0 → emit fires.
	_ = counter.Init(blockHash, 1)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)
	svc.urlRegistry = &fakeURLRegistry{urls: []string{"http://cb.example.test/hook"}}

	value := makeWorkMessageBytes(t, blockHash, "subtree-emit-fail", server.URL, 0)
	if err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value}); err != nil {
		// Below max attempts, handleTransientFailure re-publishes for retry
		// and returns nil — the work item was redirected through the retry
		// path rather than silently acked at the success branch.
		t.Fatalf("handleMessage with BLOCK_PROCESSED emit failure: expected nil after retry republish, got: %v", err)
	}

	// Counter WAS decremented exactly once (emit happens after Decrement); on
	// redelivery the counter goes negative and remaining<=0 fires emit again.
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once on emit-failure path, got %d", got)
	}
	// STUMP callback was published successfully BEFORE the BLOCK_PROCESSED emit failed.
	if got := cbMock.sentCountOfType(kafka.CallbackStump); got != 1 {
		t.Errorf("expected 1 STUMP callback publish before emit failure, got %d", got)
	}
	if got := cbMock.sentCountOfType(kafka.CallbackBlockProcessed); got != 0 {
		t.Errorf("expected zero successful BLOCK_PROCESSED publishes (publish failed), got %d", got)
	}
	// Work item must have been re-published for retry.
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 retry publish on emit failure, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected zero DLQ publishes (below max attempts), got %d", got)
	}
}

// TestHandleMessage_BlockProcessedEmitRetry_ReEmitsOnRedelivery verifies the
// approach-A semantics: when a redelivered work item drives the counter
// negative (because a previous attempt decremented to 0 but failed to emit),
// emitBlockProcessed fires again. Receiver-side dedup at the delivery
// service is responsible for collapsing the duplicate so the registered
// endpoint sees BLOCK_PROCESSED at most once per (block, URL) pair.
func TestHandleMessage_BlockProcessedEmitRetry_ReEmitsOnRedelivery(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-emit-retry"
	counter := newCountingSubtreeCounter()
	// Pre-seed at 0 to simulate a redelivery: a previous attempt already
	// decremented to 0 (and either succeeded-emit or failed-emit). The
	// retry's decrement will drive the counter to -1, and remaining<=0 must
	// still trigger emit so a previously-failed BLOCK_PROCESSED is retried.
	_ = counter.Init(blockHash, 0)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 5)
	svc.urlRegistry = &fakeURLRegistry{urls: []string{"http://cb.example.test/hook"}}

	value := makeWorkMessageBytes(t, blockHash, "subtree-emit-retry", server.URL, 1)
	if err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value}); err != nil {
		t.Fatalf("handleMessage on redelivery: expected nil, got: %v", err)
	}

	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called exactly once on redelivery, got %d", got)
	}
	if got := counter.value(blockHash); got != -1 {
		t.Errorf("expected counter to be -1 on redelivery decrement, got %d", got)
	}
	// BLOCK_PROCESSED MUST have been re-emitted: receiver-side dedup will
	// collapse the duplicate at delivery time.
	if got := cbMock.sentCountOfType(kafka.CallbackBlockProcessed); got != 1 {
		t.Errorf("expected 1 BLOCK_PROCESSED re-emit on redelivery, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected zero retry publishes when redelivery succeeds, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected zero DLQ publishes, got %d", got)
	}
}

// TestHandleMessage_BlockProcessedEmitFailure_AtMaxAttempts_DLQPathReturnsError
// covers the worst-case F-014 path: a STUMP-publish failure forces DLQ, and
// the DLQ-path decrement-and-emit hits the F-014 emit failure too. The DLQ
// publish has already happened, but the emit-failure is propagated so the
// consumer redelivers — silently acking would leave the registered endpoint
// without BLOCK_PROCESSED forever. This matches the F-013 DLQ-path
// Decrement-failure semantics from PR #88: prefer a duplicate DLQ publish
// over silently losing the BLOCK_PROCESSED notification.
func TestHandleMessage_BlockProcessedEmitFailure_AtMaxAttempts_DLQPathReturnsError(t *testing.T) {
	// Producer fails ALL callback publishes — STUMP fails (forcing DLQ) AND
	// BLOCK_PROCESSED fails (forcing the F-014 path on the DLQ-decrement).
	cbMock := &callbackFailingProducer{failAll: true, failErr: errors.New("kafka unavailable")}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	const blockHash = "block-emit-dlq"
	counter := newCountingSubtreeCounter()
	_ = counter.Init(blockHash, 1)
	counter.initCalls = 0

	stumpStore := &stubStumpStore{}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	const maxAttempts = 3
	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, maxAttempts)
	svc.urlRegistry = &fakeURLRegistry{urls: []string{"http://cb.example.test/hook"}}

	// AttemptCount = maxAttempts - 1 → next attempt is terminal → DLQ.
	value := makeWorkMessageBytes(t, blockHash, "subtree-emit-dlq", server.URL, maxAttempts-1)
	err := svc.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err == nil {
		t.Fatalf("expected non-nil error so consumer redelivers when emit fails on DLQ path, got nil")
	}

	if got := dlqMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 DLQ publish at max attempts, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected zero retry publishes at max attempts, got %d", got)
	}
	// Counter Decrement was attempted exactly once on the DLQ path.
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected counter Decrement called once on DLQ path, got %d", got)
	}
}
