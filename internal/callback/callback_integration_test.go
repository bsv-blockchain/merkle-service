//go:build integration

package callback_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/callback"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// newTestStumpStore builds a StumpStore backed by an in-memory blob for
// integration tests. Callers Put the STUMP bytes and embed the returned ref
// in their CallbackTopicMessage.
func newTestStumpStore() store.StumpStore {
	return store.NewStumpStore(store.NewMemoryBlobStore(), 0, slog.Default())
}

var brokers = []string{"localhost:9092"}

func uniqueTopic(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func TestCallbackDelivery_SuccessfulCallback(t *testing.T) {
	stumpsTopic := uniqueTopic(t, "cb_stumps")
	dlqTopic := uniqueTopic(t, "cb_dlq")

	// Set up a mock HTTP server that receives the callback.
	var mu sync.Mutex
	var receivedBody map[string]interface{}
	callbackReceived := make(chan struct{})

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
			// Already closed.
		default:
			close(callbackReceived)
		}
	}))
	defer mockServer.Close()

	// Start the DeliveryService FIRST so the consumer is ready before publishing.
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:          brokers,
			CallbackTopic:    stumpsTopic,
			CallbackDLQTopic: dlqTopic,
			ConsumerGroup:    fmt.Sprintf("cb-test-%d", time.Now().UnixNano()),
		},
		Callback: config.CallbackConfig{
			MaxRetries:     3,
			BackoffBaseSec: 1,
			TimeoutSec:     5,
		},
	}

	svc := callback.NewDeliveryService(cfg, nil, newTestStumpStore())
	if err := svc.Init(nil); err != nil {
		t.Skipf("Kafka not available; skipping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Skipf("Kafka not available; skipping: %v", err)
	}
	defer svc.Stop()

	// Allow consumer group to fully stabilize before publishing.
	time.Sleep(3 * time.Second)

	// Now publish a CallbackTopicMessage to Kafka.
	producer, err := kafka.NewProducer(brokers, stumpsTopic, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available; skipping: %v", err)
	}
	defer producer.Close()

	stumpsMsg := &kafka.CallbackTopicMessage{
		CallbackURL: mockServer.URL + "/notify",
		TxID:        "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd",
		Type:        kafka.CallbackSeenOnNetwork,
		RetryCount:  0,
	}

	encoded, err := stumpsMsg.Encode()
	if err != nil {
		t.Fatalf("failed to encode CallbackTopicMessage: %v", err)
	}

	if err := producer.PublishWithHashKey(stumpsMsg.CallbackURL, encoded); err != nil {
		t.Fatalf("failed to publish stumps message: %v", err)
	}

	// Wait for callback to be received.
	select {
	case <-callbackReceived:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for callback delivery")
	}

	mu.Lock()
	defer mu.Unlock()

	if receivedBody == nil {
		t.Fatal("received nil body")
	}
	if receivedBody["txid"] != stumpsMsg.TxID {
		t.Errorf("expected txid=%q, got %q", stumpsMsg.TxID, receivedBody["txid"])
	}
	if receivedBody["type"] != string(kafka.CallbackSeenOnNetwork) {
		t.Errorf("expected type=%q, got %q", kafka.CallbackSeenOnNetwork, receivedBody["type"])
	}
}

func TestCallbackDelivery_RetryOnFailure(t *testing.T) {
	stumpsTopic := uniqueTopic(t, "cb_retry_stumps")
	dlqTopic := uniqueTopic(t, "cb_retry_dlq")

	// Mock server that fails the first N requests, then succeeds.
	var mu sync.Mutex
	requestCount := 0
	failCount := 2 // fail first 2 attempts
	callbackSuccess := make(chan struct{})

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		current := requestCount
		mu.Unlock()

		if current <= failCount {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackSuccess:
		default:
			close(callbackSuccess)
		}
	}))
	defer mockServer.Close()

	// Start the DeliveryService FIRST.
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:          brokers,
			CallbackTopic:    stumpsTopic,
			CallbackDLQTopic: dlqTopic,
			ConsumerGroup:    fmt.Sprintf("cb-retry-test-%d", time.Now().UnixNano()),
		},
		Callback: config.CallbackConfig{
			MaxRetries:     5,
			BackoffBaseSec: 0, // no backoff delay so retries happen fast
			TimeoutSec:     5,
		},
	}

	svc := callback.NewDeliveryService(cfg, nil, newTestStumpStore())
	if err := svc.Init(nil); err != nil {
		t.Skipf("Kafka not available; skipping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Skipf("Kafka not available; skipping: %v", err)
	}
	defer svc.Stop()

	// Allow consumer group to fully stabilize before publishing.
	time.Sleep(3 * time.Second)

	// Now publish the CallbackTopicMessage.
	producer, err := kafka.NewProducer(brokers, stumpsTopic, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available; skipping: %v", err)
	}
	defer producer.Close()

	stumpsMsg := &kafka.CallbackTopicMessage{
		CallbackURL: mockServer.URL + "/retry-test",
		TxID:        "1122334411223344112233441122334411223344112233441122334411223344",
		Type:        kafka.CallbackStump,
		BlockHash:   "00000000000000000000000000000000000000000000000000000000deadbeef",
		RetryCount:  0,
	}

	encoded, err := stumpsMsg.Encode()
	if err != nil {
		t.Fatalf("failed to encode CallbackTopicMessage: %v", err)
	}

	if err := producer.PublishWithHashKey(stumpsMsg.CallbackURL, encoded); err != nil {
		t.Fatalf("failed to publish stumps message: %v", err)
	}

	// Wait for the successful callback (after retries).
	select {
	case <-callbackSuccess:
	case <-time.After(45 * time.Second):
		mu.Lock()
		t.Fatalf("timed out waiting for successful callback after retries, request count: %d", requestCount)
		mu.Unlock()
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify we got more requests than 1 (retries happened).
	if requestCount < failCount+1 {
		t.Errorf("expected at least %d requests (retries + success), got %d", failCount+1, requestCount)
	}
	t.Logf("total requests to mock server: %d (expected >=%d)", requestCount, failCount+1)
}
