package p2p

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// mockSyncProducer implements sarama.SyncProducer for testing.
type mockSyncProducer struct {
	mu       sync.Mutex
	messages []*sarama.ProducerMessage
	failErr  error // if set, SendMessage returns this error
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failErr != nil {
		return 0, 0, m.failErr
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

func (m *mockSyncProducer) Close() error          { return nil }
func (m *mockSyncProducer) IsTransactional() bool { return false }
func (m *mockSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}
func (m *mockSyncProducer) BeginTxn() error  { return nil }
func (m *mockSyncProducer) CommitTxn() error { return nil }
func (m *mockSyncProducer) AbortTxn() error  { return nil }
func (m *mockSyncProducer) AddOffsetsToTxn(_ map[string][]*sarama.PartitionOffsetMetadata, _ string) error {
	return nil
}

func (m *mockSyncProducer) AddMessageToTxn(_ *sarama.ConsumerMessage, _ string, _ *string) error {
	return nil
}

func (m *mockSyncProducer) getMessages() []*sarama.ProducerMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*sarama.ProducerMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

// newTestClient creates a Client wired with mock Kafka producers for testing.
func newTestClient(t *testing.T) (*Client, *mockSyncProducer, *mockSyncProducer) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mockSubtreeProducer := &mockSyncProducer{}
	mockBlockProducer := &mockSyncProducer{}

	cfg := config.P2PConfig{
		Network:     "main",
		StoragePath: t.TempDir(),
	}

	client := NewClient(
		cfg,
		kafka.NewTestProducer(mockSubtreeProducer, "subtree-kafka-topic", logger),
		kafka.NewTestProducer(mockBlockProducer, "block-kafka-topic", logger),
		logger,
	)

	return client, mockSubtreeProducer, mockBlockProducer
}

// --- handleSubtreeMessage tests ---

func TestHandleSubtreeMessage_ValidMessage(t *testing.T) {
	client, mockProducer, _ := newTestClient(t)

	msg := teranode.SubtreeMessage{
		Hash:       "subtree-abc",
		DataHubURL: "https://datahub.example.com/subtree/abc",
		PeerID:     "peer1",
		ClientName: "teranode-v1",
	}

	if err := client.handleSubtreeMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	published := mockProducer.getMessages()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}

	// Verify the key is the subtree hash.
	keyBytes, err := published[0].Key.Encode()
	if err != nil {
		t.Fatalf("failed to encode key: %v", err)
	}
	if string(keyBytes) != "subtree-abc" {
		t.Errorf("expected key 'subtree-abc', got %q", string(keyBytes))
	}

	// Verify the value deserializes back correctly.
	valueBytes, err := published[0].Value.Encode()
	if err != nil {
		t.Fatalf("failed to encode value: %v", err)
	}
	decoded, err := kafka.DecodeSubtreeMessage(valueBytes)
	if err != nil {
		t.Fatalf("failed to decode published subtree message: %v", err)
	}
	if decoded.Hash != "subtree-abc" {
		t.Errorf("expected hash 'subtree-abc', got %q", decoded.Hash)
	}
	if decoded.DataHubURL != "https://datahub.example.com/subtree/abc" {
		t.Errorf("expected dataHubUrl, got %q", decoded.DataHubURL)
	}
	if decoded.PeerID != "peer1" {
		t.Errorf("expected peerId 'peer1', got %q", decoded.PeerID)
	}
}

func TestHandleSubtreeMessage_EmptyHash(t *testing.T) {
	client, mockProducer, _ := newTestClient(t)

	msg := teranode.SubtreeMessage{
		Hash:       "",
		DataHubURL: "https://datahub.example.com/subtree/empty",
	}

	if err := client.handleSubtreeMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still publish (empty hash is valid from the P2P layer perspective).
	published := mockProducer.getMessages()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
}

// --- handleBlockMessage tests ---

func TestHandleBlockMessage_ValidMessage(t *testing.T) {
	client, _, mockProducer := newTestClient(t)

	msg := teranode.BlockMessage{
		Hash:       "00000000abc123",
		Height:     800000,
		Header:     "0100000000000000",
		Coinbase:   "01000000010000",
		DataHubURL: "https://datahub.example.com/block/abc123",
		PeerID:     "peer2",
		ClientName: "teranode-v1",
	}

	if err := client.handleBlockMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	published := mockProducer.getMessages()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}

	// Verify the key is the block hash.
	keyBytes, err := published[0].Key.Encode()
	if err != nil {
		t.Fatalf("failed to encode key: %v", err)
	}
	if string(keyBytes) != "00000000abc123" {
		t.Errorf("expected key '00000000abc123', got %q", string(keyBytes))
	}

	// Verify the value deserializes back correctly.
	valueBytes, err := published[0].Value.Encode()
	if err != nil {
		t.Fatalf("failed to encode value: %v", err)
	}
	decoded, err := kafka.DecodeBlockMessage(valueBytes)
	if err != nil {
		t.Fatalf("failed to decode published block message: %v", err)
	}
	if decoded.Hash != "00000000abc123" {
		t.Errorf("expected hash '00000000abc123', got %q", decoded.Hash)
	}
	if decoded.Height != 800000 {
		t.Errorf("expected height 800000, got %d", decoded.Height)
	}
	if decoded.Header != "0100000000000000" {
		t.Errorf("expected header '0100000000000000', got %q", decoded.Header)
	}
	if decoded.Coinbase != "01000000010000" {
		t.Errorf("expected coinbase '01000000010000', got %q", decoded.Coinbase)
	}
	if decoded.DataHubURL != "https://datahub.example.com/block/abc123" {
		t.Errorf("expected dataHubUrl, got %q", decoded.DataHubURL)
	}
}

func TestHandleBlockMessage_ZeroHeight(t *testing.T) {
	client, _, mockProducer := newTestClient(t)

	msg := teranode.BlockMessage{
		Hash:   "genesis-hash",
		Height: 0,
	}

	if err := client.handleBlockMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	published := mockProducer.getMessages()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
}

// --- Health tests (does not require libp2p) ---

func TestHealth_InitialState(t *testing.T) {
	client, _, _ := newTestClient(t)

	health := client.Health()
	if health.Name != "p2p-client" {
		t.Errorf("expected name 'p2p-client', got %q", health.Name)
	}
	// Initially not connected and no peers, so should be unhealthy or degraded.
	if health.Status == "healthy" {
		t.Error("expected non-healthy status for unconnected client")
	}
}

func TestHealth_Connected_NoPeers(t *testing.T) {
	client, _, _ := newTestClient(t)
	client.setConnected(true)

	health := client.Health()
	if health.Status != "degraded" {
		t.Errorf("expected status 'degraded' with 0 peers, got %q", health.Status)
	}
}

func TestHealth_Disconnected(t *testing.T) {
	client, _, _ := newTestClient(t)
	client.setConnected(false)

	health := client.Health()
	if health.Details["connection"] != "disconnected" {
		t.Errorf("expected connection 'disconnected', got %q", health.Details["connection"])
	}
}

// --- Init tests ---

func TestInit_MissingSubtreeProducer(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{}

	client := NewClient(
		config.P2PConfig{Network: "main"},
		nil, // missing subtree producer
		kafka.NewTestProducer(mockProducer, "block", logger),
		logger,
	)

	err := client.Init(nil)
	if err == nil {
		t.Fatal("expected error for nil subtree producer")
	}
}

func TestInit_MissingBlockProducer(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{}

	client := NewClient(
		config.P2PConfig{Network: "main"},
		kafka.NewTestProducer(mockProducer, "subtree", logger),
		nil, // missing block producer
		logger,
	)

	err := client.Init(nil)
	if err == nil {
		t.Fatal("expected error for nil block producer")
	}
}

func TestInit_Success(t *testing.T) {
	client, _, _ := newTestClient(t)

	err := client.Init(nil)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

// --- MsgBus config forwarding tests ---

func TestInit_ZeroValueMsgBusSucceeds(t *testing.T) {
	// Task 4.1: Init succeeds when MsgBus fields are zero-value (relying on config defaults).
	// The existing TestInit_Success covers this, but this test makes the intent explicit.
	client, _, _ := newTestClient(t)

	err := client.Init(nil)
	if err != nil {
		t.Fatalf("expected no error with zero-value MsgBus config, got: %v", err)
	}
}

func TestNewClient_MsgBusFieldsStored(t *testing.T) {
	// Task 4.2: Verify P2PMsgBusConfig fields are stored on the client cfg so they will
	// be forwarded into p2p.Config.MsgBus when Start() constructs the library config.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockSubtree := &mockSyncProducer{}
	mockBlock := &mockSyncProducer{}

	cfg := config.P2PConfig{
		Network:     "test",
		StoragePath: t.TempDir(),
		MsgBus: config.P2PMsgBusConfig{
			DHTMode:        "server",
			Port:           9000,
			MaxConnections: 50,
			MinConnections: 40,
			EnableNAT:      true,
			EnableMDNS:     false,
		},
	}

	client := NewClient(
		cfg,
		kafka.NewTestProducer(mockSubtree, "subtree", logger),
		kafka.NewTestProducer(mockBlock, "block", logger),
		logger,
	)

	if client.cfg.MsgBus.DHTMode != "server" {
		t.Errorf("expected DHTMode %q, got %q", "server", client.cfg.MsgBus.DHTMode)
	}
	if client.cfg.MsgBus.Port != 9000 {
		t.Errorf("expected Port 9000, got %d", client.cfg.MsgBus.Port)
	}
	if client.cfg.MsgBus.MaxConnections != 50 {
		t.Errorf("expected MaxConnections 50, got %d", client.cfg.MsgBus.MaxConnections)
	}
	if !client.cfg.MsgBus.EnableNAT {
		t.Error("expected EnableNAT true")
	}
}

// --- Multiple messages test ---

func TestHandleSubtreeMessage_MultipleMessages(t *testing.T) {
	client, mockProducer, _ := newTestClient(t)

	hashes := []string{"hash-1", "hash-2", "hash-3"}
	for _, hash := range hashes {
		msg := teranode.SubtreeMessage{
			Hash:       hash,
			DataHubURL: "https://datahub.example.com/subtree/" + hash,
		}
		if err := client.handleSubtreeMessage(context.Background(), msg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	published := mockProducer.getMessages()
	if len(published) != 3 {
		t.Fatalf("expected 3 published messages, got %d", len(published))
	}

	// Verify keys match expected hashes.
	for i, pm := range published {
		keyBytes, err := pm.Key.Encode()
		if err != nil {
			t.Fatalf("failed to encode key %d: %v", i, err)
		}
		if string(keyBytes) != hashes[i] {
			t.Errorf("message %d: expected key %q, got %q", i, hashes[i], string(keyBytes))
		}
	}
}

func TestHandleBlockMessage_MultipleMessages(t *testing.T) {
	client, _, mockProducer := newTestClient(t)

	for i, hash := range []string{"hash-a", "hash-b"} {
		msg := teranode.BlockMessage{
			Hash:   hash,
			Height: uint32(i + 100),
		}
		if err := client.handleBlockMessage(context.Background(), msg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	published := mockProducer.getMessages()
	if len(published) != 2 {
		t.Fatalf("expected 2 published messages, got %d", len(published))
	}
}

// --- Publish-exhaustion / fatal-propagation tests (issue #7) ---

// withFastRetries swaps the package-level baseRetryDelay to something tiny so
// publish-failure paths complete quickly under -race. The original value is
// restored when the test ends.
func withFastRetries(t *testing.T) {
	t.Helper()
	prev := baseRetryDelay
	baseRetryDelay = time.Millisecond
	t.Cleanup(func() { baseRetryDelay = prev })
}

// TestPublishWithRetry_ExhaustionReturnsTerminalError verifies F-033: when the
// Kafka producer returns an error on every attempt, publishWithRetry must
// return ErrPublishExhausted instead of silently dropping the announcement.
func TestPublishWithRetry_ExhaustionReturnsTerminalError(t *testing.T) {
	withFastRetries(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{failErr: errors.New("simulated kafka outage")}
	producer := kafka.NewTestProducer(mockProducer, "subtree", logger)

	client := NewClient(config.P2PConfig{}, producer, producer, logger)

	err := client.publishWithRetry(context.Background(), producer, "hash-x", []byte("payload"), "subtree")
	if err == nil {
		t.Fatal("expected an error after exhausting retries, got nil")
	}
	if !errors.Is(err, ErrPublishExhausted) {
		t.Fatalf("expected ErrPublishExhausted, got %v", err)
	}
}

// TestPublishWithRetry_SucceedsBeforeExhaustion verifies that a transient
// failure followed by success still returns nil (no terminal error) and
// continues processing.
func TestPublishWithRetry_SucceedsBeforeExhaustion(t *testing.T) {
	withFastRetries(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Producer that fails the first two attempts, then succeeds.
	flaky := &flakyProducer{failuresRemaining: 2}
	producer := kafka.NewTestProducer(flaky, "subtree", logger)
	client := NewClient(config.P2PConfig{}, producer, producer, logger)

	err := client.publishWithRetry(context.Background(), producer, "hash-y", []byte("payload"), "subtree")
	if err != nil {
		t.Fatalf("expected nil error after eventual success, got %v", err)
	}
	if flaky.attempts != 3 {
		t.Errorf("expected 3 attempts (2 failures + success), got %d", flaky.attempts)
	}
}

// TestHandleSubtreeMessage_PropagatesTerminalError verifies that the message
// handler bubbles ErrPublishExhausted up so the run-loop can shut down.
func TestHandleSubtreeMessage_PropagatesTerminalError(t *testing.T) {
	withFastRetries(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{failErr: errors.New("kafka down")}
	producer := kafka.NewTestProducer(mockProducer, "subtree", logger)

	client := NewClient(config.P2PConfig{}, producer, producer, logger)

	err := client.handleSubtreeMessage(context.Background(), teranode.SubtreeMessage{Hash: "h"})
	if !errors.Is(err, ErrPublishExhausted) {
		t.Fatalf("expected ErrPublishExhausted, got %v", err)
	}
}

// TestHandleBlockMessage_PropagatesTerminalError verifies the block handler
// bubbles ErrPublishExhausted up.
func TestHandleBlockMessage_PropagatesTerminalError(t *testing.T) {
	withFastRetries(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{failErr: errors.New("kafka down")}
	producer := kafka.NewTestProducer(mockProducer, "block", logger)

	client := NewClient(config.P2PConfig{}, producer, producer, logger)

	err := client.handleBlockMessage(context.Background(), teranode.BlockMessage{Hash: "b", Height: 1})
	if !errors.Is(err, ErrPublishExhausted) {
		t.Fatalf("expected ErrPublishExhausted, got %v", err)
	}
}

// TestSignalFatal_PropagatesToRun verifies that a fatal error signaled from
// any publish path bubbles up out of Run so the entrypoint can exit non-zero.
func TestSignalFatal_PropagatesToRun(t *testing.T) {
	client, _, _ := newTestClient(t)

	// Pretend Start has run (so Run does not try to spin up a real libp2p host).
	client.SetStarted(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Also wire a cancel function so signalFatal's cancel call is a no-op-safe.
	client.cancel = cancel

	go func() {
		// Simulate a publish loop hitting exhaustion.
		client.signalFatal(ErrPublishExhausted)
	}()

	err := client.Run(ctx)
	if !errors.Is(err, ErrPublishExhausted) {
		t.Fatalf("expected Run to return ErrPublishExhausted, got %v", err)
	}
}

// TestRun_ContextCancellationReturnsNil verifies that a clean shutdown via
// context cancellation returns a nil error (i.e., the entry point exits 0).
func TestRun_ContextCancellationReturnsNil(t *testing.T) {
	client, _, _ := newTestClient(t)
	client.SetStarted(true)

	ctx, cancel := context.WithCancel(context.Background())
	client.cancel = cancel

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	if err := client.Run(ctx); err != nil {
		t.Fatalf("expected nil on clean shutdown, got %v", err)
	}
}

// TestPublishWithRetry_ContextCancelledDuringBackoffIsNotFatal verifies that
// shutting down mid-retry does not produce a spurious fatal error.
func TestPublishWithRetry_ContextCancelledDuringBackoffIsNotFatal(t *testing.T) {
	// Use the default retry delay so we have time to cancel mid-backoff.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{failErr: errors.New("kafka down")}
	producer := kafka.NewTestProducer(mockProducer, "subtree", logger)
	client := NewClient(config.P2PConfig{}, producer, producer, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := client.publishWithRetry(ctx, producer, "hash-z", []byte("payload"), "subtree")
	if err != nil {
		t.Fatalf("expected nil on context cancellation, got %v", err)
	}
}

// flakyProducer fails the first failuresRemaining SendMessage calls, then succeeds.
type flakyProducer struct {
	mu                sync.Mutex
	failuresRemaining int
	attempts          int
}

func (f *flakyProducer) SendMessage(_ *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.failuresRemaining > 0 {
		f.failuresRemaining--
		return 0, 0, errors.New("transient kafka failure")
	}
	return 0, int64(f.attempts), nil
}

func (f *flakyProducer) SendMessages(_ []*sarama.ProducerMessage) error { return nil }
func (f *flakyProducer) Close() error                                   { return nil }
func (f *flakyProducer) IsTransactional() bool                          { return false }
func (f *flakyProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}
func (f *flakyProducer) BeginTxn() error  { return nil }
func (f *flakyProducer) CommitTxn() error { return nil }
func (f *flakyProducer) AbortTxn() error  { return nil }
func (f *flakyProducer) AddOffsetsToTxn(_ map[string][]*sarama.PartitionOffsetMetadata, _ string) error {
	return nil
}

func (f *flakyProducer) AddMessageToTxn(_ *sarama.ConsumerMessage, _ string, _ *string) error {
	return nil
}
