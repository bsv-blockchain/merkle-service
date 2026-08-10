package subtree

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// The subtree-fetcher is the ONLY producer of SEEN_ON_NETWORK /
// SEEN_MULTIPLE_NODES. Init binds its callbackProducer to
// Kafka.SeenCallbackTopic() so that, when a dedicated SEEN topic is
// configured, those small latency-sensitive callbacks stop sharing the
// 1-partition 'callback' topic with ~545 KB STUMP / BLOCK_PROCESSED payloads.
// STUMP and BLOCK_PROCESSED are produced elsewhere (internal/block) and
// deliberately stay on 'callback'.
//
// These tests build the producer with the same expression Init uses, so a
// change to the topic-resolution rule on the producer side has to be made
// here too — and the consumer side reads the identical helper.

// newSeenTopicTestProcessor builds a Processor whose callbackProducer is bound
// to cfg.Kafka.SeenCallbackTopic(), mirroring Processor.Init.
func newSeenTopicTestProcessor(t *testing.T, cfg *config.Config) (*Processor, *mockSyncProducer) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockProducer := &mockSyncProducer{}
	p := &Processor{
		cfg:               cfg,
		registrationStore: &mockRegStore{registrations: map[string][]string{}},
		seenCounterStore:  &mockSeenCounter{},
		callbackProducer:  kafka.NewTestProducer(mockProducer, cfg.Kafka.SeenCallbackTopic(), logger),
	}
	p.InitBase("subtree-seen-topic-test")
	p.Logger = logger
	return p, mockProducer
}

// TestSeenCallbacksPublishToSeenTopic asserts that with a dedicated SEEN topic
// configured, the SEEN emit path publishes to THAT topic and not to the shared
// 'callback' topic that block processing floods.
func TestSeenCallbacksPublishToSeenTopic(t *testing.T) {
	cfg := &config.Config{Kafka: config.KafkaConfig{
		CallbackTopic:     "callback",
		CallbackSeenTopic: "callback-seen",
	}}
	p, mockProd := newSeenTopicTestProcessor(t, cfg)

	if got := p.callbackProducer.Topic(); got != "callback-seen" {
		t.Fatalf("SEEN callback producer bound to %q, want %q", got, "callback-seen")
	}
	if p.callbackProducer.Topic() == cfg.Kafka.CallbackTopic {
		t.Fatal("SEEN callbacks must not share the topic that carries STUMP/BLOCK_PROCESSED")
	}

	registered := map[string][]string{
		testTx1: {"http://arcade.example.com/cb"},
		testTx2: {"http://arcade.example.com/cb"},
	}
	if err := p.emitBatchedSeenCallbacks(context.Background(), toEntries(registered, nil), "subtree-A"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}

	msgs := mockProd.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 batched SEEN message, got %d", len(msgs))
	}
	if cb := decodeCallbackMsg(t, msgs[0]); cb.Type != kafka.CallbackSeenOnNetwork {
		t.Errorf("published type = %q, want SEEN_ON_NETWORK", cb.Type)
	}
}

// TestSeenCallbacksFallBackToCallbackTopic is the inert-by-default half: with
// no SEEN topic configured, the producer targets 'callback' exactly as before
// the split, so deploying this change without config touches nothing.
func TestSeenCallbacksFallBackToCallbackTopic(t *testing.T) {
	cfg := &config.Config{Kafka: config.KafkaConfig{CallbackTopic: "callback"}}
	p, mockProd := newSeenTopicTestProcessor(t, cfg)

	if got := p.callbackProducer.Topic(); got != "callback" {
		t.Fatalf("SEEN callback producer bound to %q, want the shared %q when no SEEN topic is configured", got, "callback")
	}

	registered := map[string][]string{testTx1: {"http://arcade.example.com/cb"}}
	if err := p.emitBatchedSeenCallbacks(context.Background(), toEntries(registered, nil), "subtree-B"); err != nil {
		t.Fatalf("emitBatchedSeenCallbacks: %v", err)
	}
	if got := len(mockProd.getMessages()); got != 1 {
		t.Fatalf("expected 1 batched SEEN message, got %d", got)
	}
}
