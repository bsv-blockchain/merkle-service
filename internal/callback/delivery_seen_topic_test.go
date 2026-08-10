package callback

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// These tests cover the SEEN callback topic split: SEEN_ON_NETWORK /
// SEEN_MULTIPLE_NODES ride their own Kafka topic with their own consumer so a
// burst of ~545 KB STUMP/BLOCK_PROCESSED deliveries on the 1-partition
// 'callback' topic can no longer head-of-line-block them.
//
// The single most dangerous way to get this wrong is the RETRY path: before
// the split there was one retry producer hard-wired to 'callback'. Reusing it
// for SEEN messages would push every retried SEEN callback back onto the
// blocked topic, quietly restoring the stall for exactly the traffic the split
// exists to protect. Hence the emphasis below on "retry goes back to the
// SOURCE topic".

const (
	testCallbackTopic     = "callback-test"
	testCallbackSeenTopic = "callback-seen-test"
)

// splitTestConfig returns a config with the SEEN split enabled.
func splitTestConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			CallbackTopic:     testCallbackTopic,
			CallbackSeenTopic: testCallbackSeenTopic,
			CallbackDLQTopic:  "callback-dlq-test",
		},
		Callback: config.CallbackConfig{
			MaxRetries:     3,
			BackoffBaseSec: 10,
			TimeoutSec:     5,
		},
	}
}

// newSplitTestDeliveryService builds a DeliveryService wired the way Init
// wires it when the SEEN split is on: one retry producer per source topic.
// Returns (service, callback-topic retry mock, seen-topic retry mock, dlq mock).
func newSplitTestDeliveryService(t *testing.T, cfg *config.Config, httpClient *http.Client) (
	*DeliveryService, *mockSyncProducer, *mockSyncProducer, *mockSyncProducer,
) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	callbackRetry := &mockSyncProducer{}
	seenRetry := &mockSyncProducer{}
	dlq := &mockSyncProducer{}

	ds := &DeliveryService{
		cfg:         cfg,
		httpClient:  httpClient,
		dlqProducer: kafka.NewTestProducer(dlq, cfg.Kafka.CallbackDLQTopic, logger),
		retryProducers: map[string]*kafka.Producer{
			cfg.Kafka.CallbackTopic:     kafka.NewTestProducer(callbackRetry, cfg.Kafka.CallbackTopic, logger),
			cfg.Kafka.CallbackSeenTopic: kafka.NewTestProducer(seenRetry, cfg.Kafka.CallbackSeenTopic, logger),
		},
		stumpStore: store.NewStumpStore(store.NewMemoryBlobStore(), 0, logger),
	}
	ds.InitBase("callback-delivery-split-test")
	ds.Logger = logger

	return ds, callbackRetry, seenRetry, dlq
}

// failingServer returns an httptest server that always 500s, so every delivery
// lands on the retry path.
func failingServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(s.Close)
	return s
}

// TestRetryRepublishesToSourceTopic is the regression guard for the split: a
// failed SEEN delivery consumed from the SEEN topic must be republished to the
// SEEN topic, and a failed STUMP delivery consumed from 'callback' must be
// republished to 'callback'. Cross-routing either way reintroduces the
// head-of-line stall.
func TestRetryRepublishesToSourceTopic(t *testing.T) {
	tests := []struct {
		name        string
		sourceTopic string
		msg         *kafka.CallbackTopicMessage
		wantSeen    int
		wantOther   int
	}{
		{
			name:        "SEEN consumed from the seen topic retries on the seen topic",
			sourceTopic: testCallbackSeenTopic,
			msg: &kafka.CallbackTopicMessage{
				Type:        kafka.CallbackSeenOnNetwork,
				SubtreeHash: "subtree-A",
				TxIDs:       []string{"tx-1", "tx-2"},
			},
			wantSeen:  1,
			wantOther: 0,
		},
		{
			name:        "SEEN_MULTIPLE_NODES from the seen topic retries on the seen topic",
			sourceTopic: testCallbackSeenTopic,
			msg: &kafka.CallbackTopicMessage{
				Type:        kafka.CallbackSeenMultipleNodes,
				SubtreeHash: "subtree-B",
				TxIDs:       []string{"tx-3"},
			},
			wantSeen:  1,
			wantOther: 0,
		},
		{
			name:        "BLOCK_PROCESSED consumed from callback retries on callback",
			sourceTopic: testCallbackTopic,
			msg: &kafka.CallbackTopicMessage{
				Type:      kafka.CallbackBlockProcessed,
				BlockHash: testBlockHash,
			},
			wantSeen:  0,
			wantOther: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := failingServer(t)
			cfg := splitTestConfig()
			ds, callbackRetry, seenRetry, dlq := newSplitTestDeliveryService(t, cfg, server.Client())

			msg := *tc.msg
			msg.CallbackURL = server.URL + "/cb"

			consumed := encodeConsumerMessage(t, &msg)
			consumed.Topic = tc.sourceTopic

			if err := ds.handleMessage(context.Background(), consumed); err != nil {
				t.Fatalf("handleMessage: expected nil after successful republish, got %v", err)
			}

			if got := len(seenRetry.getMessages()); got != tc.wantSeen {
				t.Errorf("seen-topic retry publishes = %d, want %d", got, tc.wantSeen)
			}
			if got := len(callbackRetry.getMessages()); got != tc.wantOther {
				t.Errorf("callback-topic retry publishes = %d, want %d", got, tc.wantOther)
			}
			if got := len(dlq.getMessages()); got != 0 {
				t.Errorf("expected no DLQ publishes with retries available, got %d", got)
			}
		})
	}
}

// TestFutureDatedRetryRepublishesToSourceTopic covers the OTHER republish call
// site — the not-yet-due retry that handleMessage bounces back onto the topic
// rather than pinning a partition for the whole backoff. It must respect the
// source topic too.
func TestFutureDatedRetryRepublishesToSourceTopic(t *testing.T) {
	server := failingServer(t)
	cfg := splitTestConfig()
	ds, callbackRetry, seenRetry, _ := newSplitTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		SubtreeHash: "subtree-C",
		TxIDs:       []string{"tx-future"},
		RetryCount:  1,
		// Far enough out that handleMessage takes the republish branch rather
		// than sleeping the remainder in-process.
		NextRetryAt: time.Now().Add(10 * futureRetryWaitCap),
	}

	consumed := encodeConsumerMessage(t, msg)
	consumed.Topic = testCallbackSeenTopic

	if err := ds.handleMessage(context.Background(), consumed); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	if got := len(seenRetry.getMessages()); got != 1 {
		t.Errorf("seen-topic retry publishes = %d, want 1", got)
	}
	if got := len(callbackRetry.getMessages()); got != 0 {
		t.Errorf("callback-topic retry publishes = %d, want 0 — a future-dated SEEN retry must not land on the blocked topic", got)
	}
}

// TestRetryProducerFor_FallsBackToCallbackTopic pins the lookup contract:
// anything not explicitly registered (empty topic, unknown topic) resolves to
// the 'callback' producer, which is the pre-split behavior.
func TestRetryProducerFor_FallsBackToCallbackTopic(t *testing.T) {
	cfg := splitTestConfig()
	ds, callbackRetry, seenRetry, _ := newSplitTestDeliveryService(t, cfg, http.DefaultClient)

	cases := map[string]string{
		"":                     testCallbackTopic,
		"some-unrelated-topic": testCallbackTopic,
		testCallbackTopic:      testCallbackTopic,
		testCallbackSeenTopic:  testCallbackSeenTopic,
	}
	for input, want := range cases {
		p := ds.retryProducerFor(input)
		if p == nil {
			t.Fatalf("retryProducerFor(%q) = nil", input)
		}
		if got := p.Topic(); got != want {
			t.Errorf("retryProducerFor(%q).Topic() = %q, want %q", input, got, want)
		}
	}

	// Pure lookup: resolving a producer must not publish anything.
	if got := len(callbackRetry.getMessages()) + len(seenRetry.getMessages()); got != 0 {
		t.Errorf("retryProducerFor published %d messages, want 0", got)
	}
}

// TestFallbackMode_SingleTopicBehavesAsBeforeSplit is the "inert until
// configured" guarantee: with CallbackSeenTopic empty, SEEN callbacks resolve
// to the shared 'callback' topic and every delivery — whatever topic string
// the broker stamped on it — retries through the one 'callback' producer,
// exactly as it did before this change.
func TestFallbackMode_SingleTopicBehavesAsBeforeSplit(t *testing.T) {
	cfg := defaultTestConfig()
	if cfg.Kafka.CallbackSeenTopic != "" {
		t.Fatalf("precondition: defaultTestConfig must leave the SEEN split off")
	}
	if got := cfg.Kafka.SeenCallbackTopic(); got != cfg.Kafka.CallbackTopic {
		t.Fatalf("SeenCallbackTopic() = %q, want the shared topic %q", got, cfg.Kafka.CallbackTopic)
	}

	for _, sourceTopic := range []string{"", cfg.Kafka.CallbackTopic} {
		t.Run("sourceTopic="+sourceTopic, func(t *testing.T) {
			server := failingServer(t)
			ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())

			msg := &kafka.CallbackTopicMessage{
				CallbackURL: server.URL + "/cb",
				Type:        kafka.CallbackSeenOnNetwork,
				SubtreeHash: "subtree-D",
				TxIDs:       []string{"tx-fallback"},
			}
			consumed := encodeConsumerMessage(t, msg)
			consumed.Topic = sourceTopic

			if err := ds.handleMessage(context.Background(), consumed); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if got := len(retryMock.getMessages()); got != 1 {
				t.Errorf("retry publishes = %d, want 1", got)
			}
			if got := len(dlqMock.getMessages()); got != 0 {
				t.Errorf("DLQ publishes = %d, want 0", got)
			}
		})
	}
}

// TestRepublishForRetry_NoProducerNeverAcks guards the durability contract at
// the new failure mode the map introduces: if no retry producer can be
// resolved at all, republishForRetry must return an error so the Kafka offset
// stays uncommitted rather than silently dropping the callback.
func TestRepublishForRetry_NoProducerNeverAcks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ds := &DeliveryService{cfg: splitTestConfig(), retryProducers: map[string]*kafka.Producer{}}
	ds.InitBase("callback-delivery-noproducer-test")
	ds.Logger = logger

	err := ds.republishForRetry(context.Background(), testCallbackSeenTopic,
		&kafka.CallbackTopicMessage{CallbackURL: "http://example.invalid/cb", Type: kafka.CallbackSeenOnNetwork}, "test")
	if err == nil {
		t.Fatal("expected an error when no retry producer exists, got nil — the offset would be committed and the callback lost")
	}
}
