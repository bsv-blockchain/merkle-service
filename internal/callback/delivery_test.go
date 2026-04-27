package callback

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// mockSyncProducer implements sarama.SyncProducer for testing.
type mockSyncProducer struct {
	mu       sync.Mutex
	messages []*sarama.ProducerMessage
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
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

func (m *mockSyncProducer) Close() error { return nil }

func (m *mockSyncProducer) IsTransactional() bool { return false }

func (m *mockSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}

func (m *mockSyncProducer) BeginTxn() error   { return nil }
func (m *mockSyncProducer) CommitTxn() error   { return nil }
func (m *mockSyncProducer) AbortTxn() error    { return nil }
func (m *mockSyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}
func (m *mockSyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

func (m *mockSyncProducer) getMessages() []*sarama.ProducerMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*sarama.ProducerMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

// decodePublishedCallbackMessage extracts the CallbackTopicMessage from a captured ProducerMessage.
func decodePublishedCallbackMessage(t *testing.T, pm *sarama.ProducerMessage) *kafka.CallbackTopicMessage {
	t.Helper()
	valueBytes, err := pm.Value.Encode()
	if err != nil {
		t.Fatalf("failed to encode producer message value: %v", err)
	}
	msg, err := kafka.DecodeCallbackTopicMessage(valueBytes)
	if err != nil {
		t.Fatalf("failed to decode callback message from producer: %v", err)
	}
	return msg
}

// newTestDeliveryService creates a DeliveryService wired with mock producers and a custom HTTP client.
// The returned StumpStore is backed by an in-memory blob store — tests that
// want to exercise STUMP delivery should Put bytes into it and reference them
// via CallbackTopicMessage.StumpRef.
func newTestDeliveryService(t *testing.T, cfg *config.Config, httpClient *http.Client) (*DeliveryService, *mockSyncProducer, *mockSyncProducer) {
	ds, retry, dlq, _ := newTestDeliveryServiceWithStumps(t, cfg, httpClient)
	return ds, retry, dlq
}

// newTestDeliveryServiceWithStumps returns a DeliveryService plus two
// mockSyncProducers. The first return value is retained for backwards
// compatibility with tests that still reference a "retry" producer mock, but
// retries are now parked in-process rather than republished to Kafka — so
// that mock should always observe zero messages. Tests that previously
// asserted on retry-producer publishes have been updated to assert on
// DeliveryService counters / message mutation instead.
func newTestDeliveryServiceWithStumps(t *testing.T, cfg *config.Config, httpClient *http.Client) (*DeliveryService, *mockSyncProducer, *mockSyncProducer, store.StumpStore) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	unusedRetryProducer := &mockSyncProducer{}
	mockDLQProducer := &mockSyncProducer{}
	stumpStore := store.NewStumpStore(store.NewMemoryBlobStore(), 0, logger)

	ds := &DeliveryService{
		cfg:         cfg,
		httpClient:  httpClient,
		dlqProducer: kafka.NewTestProducer(mockDLQProducer, cfg.Kafka.CallbackDLQTopic, logger),
		stumpStore:  stumpStore,
		workCh:      make(chan *kafka.CallbackTopicMessage, 64),
		stumpGate:   newStumpGate(),
	}
	ds.InitBase("callback-delivery-test")
	ds.Logger = logger

	// Start workers for handleMessage dispatch.
	workers := 4
	ds.workerWg.Add(workers)
	for i := 0; i < workers; i++ {
		go ds.deliveryWorker(context.Background())
	}

	t.Cleanup(func() {
		ds.shuttingDown.Store(true)
		ds.retryTimerWg.Wait()
		close(ds.workCh)
		ds.workerWg.Wait()
	})

	return ds, unusedRetryProducer, mockDLQProducer, stumpStore
}

// waitForCondition polls until condition returns true or timeout expires.
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// defaultTestConfig returns a config suitable for testing.
func defaultTestConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			CallbackTopic:    "callback-test",
			CallbackDLQTopic: "callback-dlq-test",
		},
		Callback: config.CallbackConfig{
			MaxRetries:     3,
			BackoffBaseSec: 10,
			TimeoutSec:     5,
		},
	}
}

func TestDeliverCallback_StumpSuccess(t *testing.T) {
	var receivedBody []byte
	var receivedContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _, stumpStore := newTestDeliveryServiceWithStumps(t, cfg, server.Client())

	stumpData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	stumpRef, err := stumpStore.Put(stumpData, 0)
	if err != nil {
		t.Fatalf("failed to put stump: %v", err)
	}
	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash456",
		SubtreeIndex: 3,
		StumpRef:     stumpRef,
	}

	if err := ds.deliverCallback(context.Background(), msg); err != nil {
		t.Fatalf("expected successful delivery, got error: %v", err)
	}

	if receivedContentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", receivedContentType)
	}

	var payload callbackPayload
	if err := json.Unmarshal(receivedBody, &payload); err != nil {
		t.Fatalf("failed to unmarshal received payload: %v", err)
	}

	if payload.TxID != "" {
		t.Errorf("expected empty txid, got %q", payload.TxID)
	}
	if payload.Type != "STUMP" {
		t.Errorf("expected type 'STUMP', got %q", payload.Type)
	}
	if payload.BlockHash != "blockhash456" {
		t.Errorf("expected blockHash 'blockhash456', got %q", payload.BlockHash)
	}
	if payload.SubtreeIndex != 3 {
		t.Errorf("expected subtreeIndex 3, got %d", payload.SubtreeIndex)
	}

	expectedStump := hex.EncodeToString(stumpData)
	if payload.Stump != expectedStump {
		t.Errorf("expected stump %q, got %q", expectedStump, payload.Stump)
	}
}

func TestDeliverCallback_SeenOnNetwork(t *testing.T) {
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/callback",
		Type:        kafka.CallbackSeenOnNetwork,
		TxIDs:       []string{"tx-1", "tx-2", "tx-3"},
	}

	err := ds.deliverCallback(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected successful delivery, got error: %v", err)
	}

	var payload callbackPayload
	if err := json.Unmarshal(receivedBody, &payload); err != nil {
		t.Fatalf("failed to unmarshal received payload: %v", err)
	}

	if payload.Stump != "" {
		t.Errorf("expected empty stump for SEEN_ON_NETWORK, got %q", payload.Stump)
	}
	if payload.Type != "SEEN_ON_NETWORK" {
		t.Errorf("expected type 'SEEN_ON_NETWORK', got %q", payload.Type)
	}
	if len(payload.TxIDs) != 3 {
		t.Errorf("expected 3 txids, got %d", len(payload.TxIDs))
	}
	if payload.TxID != "" {
		t.Errorf("expected empty scalar txid, got %q", payload.TxID)
	}
}

func TestDeliverCallback_Non2xxReturnsError(t *testing.T) {
	statusCodes := []int{
		http.StatusBadRequest,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
		http.StatusForbidden,
	}

	for _, code := range statusCodes {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer server.Close()

			cfg := defaultTestConfig()
			ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

			msg := &kafka.CallbackTopicMessage{
				CallbackURL:  server.URL + "/callback",
				Type:         kafka.CallbackStump,
				BlockHash:    "blockhash",
				SubtreeIndex: 1,
			}

			err := ds.deliverCallback(context.Background(), msg)
			if err == nil {
				t.Fatalf("expected error for status code %d, got nil", code)
			}
		})
	}
}

func TestDeliverCallback_2xxStatusesSucceed(t *testing.T) {
	statusCodes := []int{200, 201, 202, 204}

	for _, code := range statusCodes {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer server.Close()

			cfg := defaultTestConfig()
			ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

			msg := &kafka.CallbackTopicMessage{
				CallbackURL:  server.URL + "/callback",
				Type:         kafka.CallbackStump,
				BlockHash:    "blockhash",
				SubtreeIndex: 1,
			}

			err := ds.deliverCallback(context.Background(), msg)
			if err != nil {
				t.Fatalf("expected success for status code %d, got error: %v", code, err)
			}
		})
	}
}

func TestProcessDelivery_RetriesOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash",
		SubtreeIndex: 1,
		RetryCount:   0,
	}

	ds.processDelivery(context.Background(), msg)

	// Retries are parked in-process now — Kafka is not touched for a
	// not-yet-exhausted retry. The message itself is mutated in-place with
	// the bumped RetryCount and the scheduled NextRetryAt.
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 Kafka retry publishes (retries are parked in-process), got %d", got)
	}
	if msg.RetryCount != 1 {
		t.Errorf("expected retry count 1 on in-place msg, got %d", msg.RetryCount)
	}
	if msg.NextRetryAt.IsZero() {
		t.Error("expected NextRetryAt to be set on in-place msg")
	}
	if ds.messagesRetried.Load() != 1 {
		t.Errorf("expected messagesRetried=1, got %d", ds.messagesRetried.Load())
	}
}

func TestProcessDelivery_PublishesToDLQAfterMaxRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	cfg.Callback.MaxRetries = 3
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash",
		SubtreeIndex: 1,
		RetryCount:   3, // Already at max retries.
	}

	ds.processDelivery(context.Background(), msg)

	// No retry should happen.
	retryMsgs := retryMock.getMessages()
	if len(retryMsgs) != 0 {
		t.Errorf("expected 0 retry messages after max retries, got %d", len(retryMsgs))
	}

	// Should be published to DLQ.
	dlqMsgs := dlqMock.getMessages()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlqMsgs))
	}
}

// TestProcessDelivery_MissingStumpBlobGoesStraightToDLQ asserts that when a
// STUMP message's StumpRef resolves to no blob (e.g. pruned past DAH), the
// delivery is marked permanent and routed to the DLQ without consuming the
// retry budget — retrying cannot recover a missing blob.
func TestProcessDelivery_MissingStumpBlobGoesStraightToDLQ(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("callback URL should never be hit when STUMP blob is missing")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, dlqMock, _ := newTestDeliveryServiceWithStumps(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-missing",
		SubtreeIndex: 7,
		StumpRef:     "ref-that-was-never-stored",
		RetryCount:   0,
	}

	ds.processDelivery(context.Background(), msg)

	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry messages for missing blob, got %d", got)
	}
	dlqMsgs := dlqMock.getMessages()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlqMsgs))
	}
	if ds.messagesFailed.Load() != 1 {
		t.Errorf("expected messagesFailed=1, got %d", ds.messagesFailed.Load())
	}
}

func TestProcessDelivery_SuccessIncrementsCounter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/callback",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-counter",
	}

	ds.processDelivery(context.Background(), msg)

	if ds.messagesProcessed.Load() != 1 {
		t.Errorf("expected messagesProcessed=1, got %d", ds.messagesProcessed.Load())
	}
}

func TestProcessDelivery_DedupSkipsDuplicate(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())
	ds.dedupStore = &mockDedupStore{exists: true}

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash",
		SubtreeIndex: 3,
	}

	ds.processDelivery(context.Background(), msg)

	if requestCount.Load() != 0 {
		t.Errorf("expected no HTTP requests for dedup hit, got %d", requestCount.Load())
	}
	if ds.messagesDedupe.Load() != 1 {
		t.Errorf("expected messagesDedupe=1, got %d", ds.messagesDedupe.Load())
	}
}

func TestHandleMessage_DispatchesToWorker(t *testing.T) {
	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, &http.Client{Timeout: time.Second})

	var delivered atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ds.httpClient = server.Client()

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/callback",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-dispatch",
	}
	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	consumerMsg := &sarama.ConsumerMessage{
		Value: data,
	}

	if err := ds.handleMessage(context.Background(), consumerMsg); err != nil {
		t.Fatalf("handleMessage failed: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return delivered.Load() > 0
	})
}

func TestHandleMessage_ParksRetryInProcess(t *testing.T) {
	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, &http.Client{Timeout: time.Second})

	var delivered atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ds.httpClient = server.Client()

	delay := 200 * time.Millisecond
	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/callback",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-parked",
		RetryCount:  1,
		NextRetryAt: time.Now().Add(delay),
	}
	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	start := time.Now()
	if err := ds.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: data}); err != nil {
		t.Fatalf("handleMessage failed: %v", err)
	}

	// Immediately after handleMessage: message should be parked, nothing
	// published to Kafka, no HTTP delivery yet.
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry-producer publishes while parked, got %d", got)
	}
	if got := ds.messagesParked.Load(); got != 1 {
		t.Errorf("expected messagesParked=1 while parked, got %d", got)
	}
	if delivered.Load() != 0 {
		t.Errorf("expected 0 HTTP deliveries before delay elapses, got %d", delivered.Load())
	}

	// After the delay, the worker should pick the message off workCh and
	// deliver it.
	waitForCondition(t, 2*time.Second, func() bool {
		return delivered.Load() > 0
	})
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("delivery fired before scheduled delay: elapsed %v < delay %v", elapsed, delay)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry-producer publishes after worker ran, got %d", got)
	}
}

func TestBuildIdempotencyKey(t *testing.T) {
	tests := []struct {
		name     string
		msg      *kafka.CallbackTopicMessage
		expected string
	}{
		{
			name: "BLOCK_PROCESSED uses blockHash",
			msg: &kafka.CallbackTopicMessage{
				Type:      kafka.CallbackBlockProcessed,
				BlockHash: "blockhash123",
			},
			expected: "blockhash123:BLOCK_PROCESSED",
		},
		{
			name: "STUMP uses blockHash and subtreeIndex",
			msg: &kafka.CallbackTopicMessage{
				Type:         kafka.CallbackStump,
				BlockHash:    "blockhash",
				SubtreeIndex: 3,
			},
			expected: "blockhash:3:STUMP",
		},
		{
			name: "batched SEEN_ON_NETWORK uses TxIDs hash",
			msg: &kafka.CallbackTopicMessage{
				Type:  kafka.CallbackSeenOnNetwork,
				TxIDs: []string{"tx1", "tx2"},
			},
			expected: hashTxIDs([]string{"tx1", "tx2"}) + ":SEEN_ON_NETWORK",
		},
		{
			name: "batched TxIDs hash is order-independent",
			msg: &kafka.CallbackTopicMessage{
				Type:  kafka.CallbackSeenOnNetwork,
				TxIDs: []string{"tx2", "tx1"},
			},
			expected: hashTxIDs([]string{"tx1", "tx2"}) + ":SEEN_ON_NETWORK",
		},
		{
			name: "scalar SEEN_ON_NETWORK uses txid",
			msg: &kafka.CallbackTopicMessage{
				Type: kafka.CallbackSeenOnNetwork,
				TxID: "txid789",
			},
			expected: "txid789:SEEN_ON_NETWORK",
		},
		{
			name: "empty txid returns empty for non-STUMP",
			msg: &kafka.CallbackTopicMessage{
				Type: kafka.CallbackSeenOnNetwork,
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildIdempotencyKey(tt.msg)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestDedupKeyForMessage(t *testing.T) {
	tests := []struct {
		name     string
		msg      *kafka.CallbackTopicMessage
		expected string
	}{
		{
			name: "BLOCK_PROCESSED uses blockHash",
			msg: &kafka.CallbackTopicMessage{
				Type:      kafka.CallbackBlockProcessed,
				BlockHash: "block123",
			},
			expected: "block123",
		},
		{
			name: "STUMP uses blockHash and subtreeIndex",
			msg: &kafka.CallbackTopicMessage{
				Type:         kafka.CallbackStump,
				BlockHash:    "blockhash",
				SubtreeIndex: 3,
			},
			expected: "blockhash:3",
		},
		{
			name: "batched SEEN uses TxIDs hash",
			msg: &kafka.CallbackTopicMessage{
				Type:  kafka.CallbackSeenOnNetwork,
				TxIDs: []string{"tx1", "tx2"},
			},
			expected: hashTxIDs([]string{"tx1", "tx2"}),
		},
		{
			name: "empty txid and txids returns empty",
			msg: &kafka.CallbackTopicMessage{
				Type: kafka.CallbackSeenOnNetwork,
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedupKeyForMessage(tt.msg)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestDeliverCallback_BlockProcessedPayload(t *testing.T) {
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/callback",
		Type:        kafka.CallbackBlockProcessed,
		BlockHash:   "block-abc",
	}

	err := ds.deliverCallback(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected successful delivery, got error: %v", err)
	}

	var payload callbackPayload
	if err := json.Unmarshal(receivedBody, &payload); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if payload.Type != "BLOCK_PROCESSED" {
		t.Errorf("expected type BLOCK_PROCESSED, got %q", payload.Type)
	}
	if payload.BlockHash != "block-abc" {
		t.Errorf("expected blockHash block-abc, got %q", payload.BlockHash)
	}
	if payload.TxID != "" {
		t.Errorf("expected empty txid, got %q", payload.TxID)
	}
}

func TestConcurrentDelivery(t *testing.T) {
	var deliveryCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveryCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())
	ds.httpClient = server.Client()

	// Dispatch multiple messages.
	for i := 0; i < 10; i++ {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL: server.URL + "/callback",
			Type:        kafka.CallbackSeenOnNetwork,
			TxID:        fmt.Sprintf("tx-%d", i),
		}
		ds.workCh <- msg
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return deliveryCount.Load() >= 10
	})

	if deliveryCount.Load() != 10 {
		t.Errorf("expected 10 deliveries, got %d", deliveryCount.Load())
	}
}

func TestBlockProcessedWaitsForStumps(t *testing.T) {
	// Track delivery order to verify STUMPs arrive before BLOCK_PROCESSED.
	var mu sync.Mutex
	var deliveryOrder []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload callbackPayload
		_ = json.Unmarshal(body, &payload)

		mu.Lock()
		deliveryOrder = append(deliveryOrder, payload.Type)
		mu.Unlock()

		// Simulate slow STUMP delivery so BLOCK_PROCESSED has a chance to race ahead.
		if payload.Type == "STUMP" {
			time.Sleep(50 * time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _, stumpStore := newTestDeliveryServiceWithStumps(t, cfg, server.Client())

	callbackURL := server.URL + "/callback"
	blockHash := "block-gate-test"

	mustPutStump := func(b []byte) string {
		ref, err := stumpStore.Put(b, 0)
		if err != nil {
			t.Fatalf("failed to put stump: %v", err)
		}
		return ref
	}

	// Simulate handleMessage ordering: register STUMPs with the gate, then dispatch all.
	stumps := []*kafka.CallbackTopicMessage{
		{CallbackURL: callbackURL, Type: kafka.CallbackStump, BlockHash: blockHash, SubtreeIndex: 0, StumpRef: mustPutStump([]byte{0x01})},
		{CallbackURL: callbackURL, Type: kafka.CallbackStump, BlockHash: blockHash, SubtreeIndex: 1, StumpRef: mustPutStump([]byte{0x02})},
		{CallbackURL: callbackURL, Type: kafka.CallbackStump, BlockHash: blockHash, SubtreeIndex: 2, StumpRef: mustPutStump([]byte{0x03})},
	}
	bp := &kafka.CallbackTopicMessage{CallbackURL: callbackURL, Type: kafka.CallbackBlockProcessed, BlockHash: blockHash}

	// Register STUMPs with the gate (simulates handleMessage sequential dispatch).
	for _, msg := range stumps {
		ds.stumpGate.Add(msg.BlockHash, msg.CallbackURL)
	}

	// Dispatch BLOCK_PROCESSED first to the worker pool to maximize race opportunity.
	ds.workCh <- bp
	for _, msg := range stumps {
		ds.workCh <- msg
	}

	waitForCondition(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveryOrder) == 4
	})

	mu.Lock()
	defer mu.Unlock()

	// BLOCK_PROCESSED must be last.
	if deliveryOrder[len(deliveryOrder)-1] != "BLOCK_PROCESSED" {
		t.Errorf("expected BLOCK_PROCESSED last, got delivery order: %v", deliveryOrder)
	}
	for i := 0; i < 3; i++ {
		if deliveryOrder[i] != "STUMP" {
			t.Errorf("expected STUMP at position %d, got %q (order: %v)", i, deliveryOrder[i], deliveryOrder)
		}
	}
}

func TestBlockProcessedProceedsWithoutStumps(t *testing.T) {
	var delivered atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	// Dispatch BLOCK_PROCESSED with no prior STUMPs — should proceed immediately.
	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/callback",
		Type:        kafka.CallbackBlockProcessed,
		BlockHash:   "block-no-stumps",
	}
	ds.workCh <- msg

	waitForCondition(t, 2*time.Second, func() bool {
		return delivered.Load() == 1
	})
}

// mockDedupStore implements CallbackDeduper for testing.
type mockDedupStore struct {
	exists bool
}

func (m *mockDedupStore) Exists(txid, callbackURL, statusType string) (bool, error) {
	return m.exists, nil
}

func (m *mockDedupStore) Record(txid, callbackURL, statusType string, ttl time.Duration) error {
	return nil
}
