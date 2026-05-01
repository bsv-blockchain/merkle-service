package block

import (
	"io"
	"log/slog"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

func TestSubtreeWorkerService_NewAndHealth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	svc := NewSubtreeWorkerService(
		config.KafkaConfig{SubtreeWorkTopic: "subtree-work"},
		config.BlockConfig{},
		config.DataHubConfig{},
		nil, // regStore
		nil, // subtreeStore
		nil, // stumpStore
		nil, // urlRegistry
		nil, // subtreeCounter
		logger,
	)

	if svc == nil {
		t.Fatal("expected non-nil service")
	}

	health := svc.Health()
	if health.Name != "subtree-worker" {
		t.Errorf("expected name 'subtree-worker', got %s", health.Name)
	}
	if health.Status != "unhealthy" {
		t.Errorf("expected 'unhealthy' before start, got %s", health.Status)
	}
}

func TestSubtreeWorkerService_EmitBlockProcessed_NilRegistry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mock := &mockSyncProducer{}

	svc := &SubtreeWorkerService{
		urlRegistry: nil,
	}
	svc.InitBase("subtree-worker-test")
	svc.Logger = logger
	svc.callbackProducer = newTestKafkaProducer(mock, "callback-test", logger)

	// Should not panic, and should return nil since there's no registry.
	if err := svc.emitBlockProcessed("blockhash-123"); err != nil {
		t.Errorf("expected nil error with nil registry, got: %v", err)
	}

	if len(mock.messages) != 0 {
		t.Errorf("expected no messages with nil registry, got %d", len(mock.messages))
	}
}

func newTestKafkaProducer(mock *mockSyncProducer, topic string, logger *slog.Logger) *kafka.Producer {
	return kafka.NewTestProducer(mock, topic, logger)
}
