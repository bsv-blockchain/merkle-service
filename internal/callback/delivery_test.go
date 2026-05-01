package callback

import (
	"context"
	"encoding/hex"
	"encoding/json"
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
	// failNext, when > 0, makes the next N SendMessage calls fail with sendErr.
	// Test-only knob for asserting the durability contract on publish failure.
	failNext int
	sendErr  error
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext > 0 {
		m.failNext--
		err := m.sendErr
		if err == nil {
			err = errMockSendFailure
		}
		return 0, 0, err
	}
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

func (m *mockSyncProducer) BeginTxn() error { return nil }
func (m *mockSyncProducer) CommitTxn() error { return nil }
func (m *mockSyncProducer) AbortTxn() error { return nil }
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

// errMockSendFailure is the default error returned by mockSyncProducer when
// failNext is set and sendErr is nil — expressive enough for tests that just
// want to assert the durability contract without crafting a custom error.
var errMockSendFailure = mockSendError("mock send failure")

type mockSendError string

func (e mockSendError) Error() string { return string(e) }

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
// The returned tuple is (service, retryProducer mock, dlqProducer mock).
func newTestDeliveryService(t *testing.T, cfg *config.Config, httpClient *http.Client) (*DeliveryService, *mockSyncProducer, *mockSyncProducer) {
	ds, retry, dlq, _ := newTestDeliveryServiceWithStumps(t, cfg, httpClient)
	return ds, retry, dlq
}

// newTestDeliveryServiceWithStumps returns a DeliveryService plus two
// mockSyncProducers (retry, dlq) and a StumpStore. The retry producer is the
// one used by handleMessage to republish not-yet-delivered messages back to
// the callback topic — it is the durable side-effect that has to succeed
// before handleMessage returns nil.
func newTestDeliveryServiceWithStumps(t *testing.T, cfg *config.Config, httpClient *http.Client) (*DeliveryService, *mockSyncProducer, *mockSyncProducer, store.StumpStore) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mockRetryProducer := &mockSyncProducer{}
	mockDLQProducer := &mockSyncProducer{}
	stumpStore := store.NewStumpStore(store.NewMemoryBlobStore(), 0, logger)

	ds := &DeliveryService{
		cfg:           cfg,
		httpClient:    httpClient,
		dlqProducer:   kafka.NewTestProducer(mockDLQProducer, cfg.Kafka.CallbackDLQTopic, logger),
		retryProducer: kafka.NewTestProducer(mockRetryProducer, cfg.Kafka.CallbackTopic, logger),
		stumpStore:    stumpStore,
	}
	ds.InitBase("callback-delivery-test")
	ds.Logger = logger

	return ds, mockRetryProducer, mockDLQProducer, stumpStore
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

// TestProcessDelivery_RetriesViaKafkaRepublish asserts that an HTTP failure
// with retries available causes the message to be republished to the
// callback topic (the durable side-effect) before processDelivery returns
// nil. The original behaviour parked retries in a time.AfterFunc — that's
// what F-021 is fixing.
func TestProcessDelivery_RetriesViaKafkaRepublish(t *testing.T) {
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

	if err := ds.processDelivery(context.Background(), msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

	retryMsgs := retryMock.getMessages()
	if len(retryMsgs) != 1 {
		t.Fatalf("expected 1 retry republish, got %d", len(retryMsgs))
	}
	republished := decodePublishedCallbackMessage(t, retryMsgs[0])
	if republished.RetryCount != 1 {
		t.Errorf("expected republished RetryCount=1, got %d", republished.RetryCount)
	}
	if republished.NextRetryAt.IsZero() {
		t.Error("expected republished NextRetryAt to be set")
	}
	if ds.messagesRetried.Load() != 1 {
		t.Errorf("expected messagesRetried=1, got %d", ds.messagesRetried.Load())
	}
}

// TestProcessDelivery_RetryRepublishFailureSurfacesError ensures that when
// the durable side-effect (retry republish) fails, processDelivery returns a
// non-nil error so the caller (handleMessage → consumer-group loop) leaves
// the Kafka offset uncommitted. This is the core F-021 contract.
func TestProcessDelivery_RetryRepublishFailureSurfacesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, server.Client())
	retryMock.failNext = 1 // first retry republish fails

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash",
		SubtreeIndex: 1,
	}

	err := ds.processDelivery(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error from processDelivery when retry republish fails")
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry messages captured (mock failed), got %d", got)
	}
	if ds.messagesRetried.Load() != 0 {
		t.Errorf("expected messagesRetried=0 when republish fails, got %d", ds.messagesRetried.Load())
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

	if err := ds.processDelivery(context.Background(), msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

	retryMsgs := retryMock.getMessages()
	if len(retryMsgs) != 0 {
		t.Errorf("expected 0 retry messages after max retries, got %d", len(retryMsgs))
	}

	dlqMsgs := dlqMock.getMessages()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlqMsgs))
	}
	if ds.messagesFailed.Load() != 1 {
		t.Errorf("expected messagesFailed=1, got %d", ds.messagesFailed.Load())
	}
}

// TestProcessDelivery_DLQPublishFailureSurfacesError asserts that when the
// DLQ publish exhausts its in-call retries, processDelivery returns a
// non-nil error so the offset stays put — preferring redelivery on next
// session over silent loss.
func TestProcessDelivery_DLQPublishFailureSurfacesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	cfg.Callback.MaxRetries = 0 // any failure goes straight to DLQ
	ds, _, dlqMock := newTestDeliveryService(t, cfg, server.Client())
	dlqMock.failNext = 100 // fail every DLQ attempt within the in-call retry budget

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash",
		SubtreeIndex: 1,
	}

	start := time.Now()
	err := ds.processDelivery(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error from processDelivery when DLQ publish fails")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("DLQ retries took too long: %v", elapsed)
	}
	if ds.messagesFailed.Load() != 0 {
		t.Errorf("expected messagesFailed=0 when DLQ publish never succeeded, got %d", ds.messagesFailed.Load())
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

	if err := ds.processDelivery(context.Background(), msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

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

	if err := ds.processDelivery(context.Background(), msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

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

	if err := ds.processDelivery(context.Background(), msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

	if requestCount.Load() != 0 {
		t.Errorf("expected no HTTP requests for dedup hit, got %d", requestCount.Load())
	}
	if ds.messagesDedupe.Load() != 1 {
		t.Errorf("expected messagesDedupe=1, got %d", ds.messagesDedupe.Load())
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
