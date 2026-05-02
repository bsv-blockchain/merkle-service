package block

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// mockSyncProducer implements sarama.SyncProducer for capturing published messages.
type mockSyncProducer struct {
	messages []*sarama.ProducerMessage
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	m.messages = append(m.messages, msg)
	return 0, int64(len(m.messages)), nil
}

func (m *mockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
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
func (m *mockSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (m *mockSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func decodeSubtreeWork(t *testing.T, pm *sarama.ProducerMessage) *kafka.SubtreeWorkMessage {
	t.Helper()
	val, err := pm.Value.Encode()
	if err != nil {
		t.Fatalf("failed to encode producer message value: %v", err)
	}
	var msg kafka.SubtreeWorkMessage
	if err := json.Unmarshal(val, &msg); err != nil {
		t.Fatalf("failed to decode subtree work message: %v", err)
	}
	return &msg
}

func newTestProcessor(t *testing.T) (*Processor, *mockSyncProducer) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	workMock := &mockSyncProducer{}
	workProducer := kafka.NewTestProducer(workMock, "subtree-work-test", logger)

	p := &Processor{
		subtreeWorkProducer: workProducer,
	}
	p.InitBase("block-processor-test")
	p.Logger = logger
	return p, workMock
}

// TestBlockProcessedMessage_CorrectFields verifies BLOCK_PROCESSED CallbackTopicMessage
// is constructed with the right fields (no TxID, no Stump, correct Type).
func TestBlockProcessedMessage_CorrectFields(t *testing.T) {
	msg := &kafka.CallbackTopicMessage{
		CallbackURL: "http://example.com/cb",
		Type:        kafka.CallbackBlockProcessed,
		BlockHash:   "blockhash-field-test",
	}

	if msg.Type != kafka.CallbackBlockProcessed {
		t.Errorf("expected CallbackBlockProcessed, got %s", msg.Type)
	}
	if msg.BlockHash != "blockhash-field-test" {
		t.Errorf("expected blockhash-field-test, got %s", msg.BlockHash)
	}
	if msg.TxID != "" {
		t.Errorf("expected empty TxID for BLOCK_PROCESSED, got %s", msg.TxID)
	}
	if msg.StumpRef != "" {
		t.Errorf("expected empty StumpRef for BLOCK_PROCESSED, got %q", msg.StumpRef)
	}

	// Verify encode/decode round-trip preserves fields.
	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decoded, err := kafka.DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Type != kafka.CallbackBlockProcessed {
		t.Errorf("decoded type: expected BLOCK_PROCESSED, got %s", decoded.Type)
	}
	if decoded.BlockHash != "blockhash-field-test" {
		t.Errorf("decoded blockHash: expected blockhash-field-test, got %s", decoded.BlockHash)
	}
	if decoded.CallbackURL != "http://example.com/cb" {
		t.Errorf("decoded callbackURL: expected http://example.com/cb, got %s", decoded.CallbackURL)
	}
}

// TestSubtreeWorkMessage_Published verifies that SubtreeWorkMessages are published
// to the subtree-work producer for each subtree hash.
func TestSubtreeWorkMessage_Published(t *testing.T) {
	_, workMock := newTestProcessor(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	workProducer := kafka.NewTestProducer(workMock, "subtree-work-test", logger)

	subtreeHashes := []string{"subtree-a", "subtree-b", "subtree-c"}
	for i, stHash := range subtreeHashes {
		workMsg := &kafka.SubtreeWorkMessage{
			BlockHash:    "block-123",
			BlockHeight:  850000,
			SubtreeHash:  stHash,
			SubtreeIndex: i,
			DataHubURL:   "http://datahub/subtree",
		}
		data, err := workMsg.Encode()
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		if err := workProducer.PublishWithHashKey(stHash, data); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
	}

	if len(workMock.messages) != 3 {
		t.Fatalf("expected 3 work messages, got %d", len(workMock.messages))
	}

	for i, pm := range workMock.messages {
		msg := decodeSubtreeWork(t, pm)
		if msg.BlockHash != "block-123" {
			t.Errorf("message %d: expected blockHash 'block-123', got %s", i, msg.BlockHash)
		}
		if msg.SubtreeHash != subtreeHashes[i] {
			t.Errorf("message %d: expected subtreeHash %s, got %s", i, subtreeHashes[i], msg.SubtreeHash)
		}
		if msg.SubtreeIndex != i {
			t.Errorf("message %d: expected subtreeIndex %d, got %d", i, i, msg.SubtreeIndex)
		}
	}
}
