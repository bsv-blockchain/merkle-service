//go:build integration

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/callback"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// ---------- helpers ----------

func randomTxID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func uniqueSetName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// callbackPayload mirrors the JSON body delivered by the callback service (Arcade's CallbackMessage).
type callbackPayload struct {
	Type         string `json:"type"`
	TxID         string `json:"txid,omitempty"`
	BlockHash    string `json:"blockHash,omitempty"`
	SubtreeIndex int    `json:"subtreeIndex,omitempty"`
	Stump        string `json:"stump,omitempty"`
}

// callbackCollector is a thread-safe collector for received callbacks.
type callbackCollector struct {
	mu        sync.Mutex
	payloads  []callbackPayload
	rawBodies [][]byte
	notify    chan struct{}
}

func newCallbackCollector() *callbackCollector {
	return &callbackCollector{
		notify: make(chan struct{}, 100),
	}
}

func (c *callbackCollector) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var p callbackPayload
		_ = json.Unmarshal(body, &p)
		c.mu.Lock()
		c.payloads = append(c.payloads, p)
		c.rawBodies = append(c.rawBodies, body)
		c.mu.Unlock()
		c.notify <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}
}

func (c *callbackCollector) waitForN(t *testing.T, n int, timeout time.Duration) []callbackPayload {
	t.Helper()
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		count := len(c.payloads)
		c.mu.Unlock()
		if count >= n {
			c.mu.Lock()
			result := make([]callbackPayload, len(c.payloads))
			copy(result, c.payloads)
			c.mu.Unlock()
			return result
		}
		select {
		case <-c.notify:
		case <-deadline:
			c.mu.Lock()
			count = len(c.payloads)
			c.mu.Unlock()
			t.Fatalf("timed out waiting for %d callbacks, got %d", n, count)
			return nil
		}
	}
}

// ---------- infrastructure ----------

const (
	aerospikeHost = "localhost"
	aerospikePort = 3000
	kafkaBroker   = "localhost:9092"
)

// determineNamespace tries to connect to Aerospike with several candidate namespaces
// and returns the first one that works.
func determineNamespace(t *testing.T) string {
	t.Helper()
	for _, ns := range []string{"test", "merkle"} {
		client, err := store.NewAerospikeClient(aerospikeHost, aerospikePort, ns, 1, 50, testLogger())
		if err != nil {
			continue
		}
		// Try a write to verify the namespace is usable.
		regStore := store.NewRegistrationStore(client, "ns_probe", 1, 50, 0, testLogger())
		if err := regStore.Add("probe_txid", "http://probe"); err != nil {
			client.Close()
			continue
		}
		client.Close()
		return ns
	}
	t.Skipf("Aerospike not available (tried 'test', 'merkle'); skipping")
	return ""
}

// newAerospikeClient creates an Aerospike client connected to the local instance.
func newAerospikeClient(t *testing.T, namespace string) *store.AerospikeClient {
	t.Helper()
	client, err := store.NewAerospikeClient(aerospikeHost, aerospikePort, namespace, 3, 100, testLogger())
	if err != nil {
		t.Skipf("Aerospike not available: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// newCallbackProducer creates a Kafka producer for the given callback topic.
func newCallbackProducer(t *testing.T, topic string) *kafka.Producer {
	t.Helper()
	producer, err := kafka.NewProducer([]string{kafkaBroker}, topic, testLogger())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	t.Cleanup(func() { producer.Close() })
	return producer
}

// startDeliveryService creates and starts a callback delivery service that
// consumes from the given callback topic and delivers to callback URLs. The
// returned StumpStore is backed by an in-memory blob store — tests that send
// STUMP messages must Put the bytes here and reference them by StumpRef.
func startDeliveryService(t *testing.T, callbackTopic string) (*callback.DeliveryService, store.StumpStore) {
	t.Helper()
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:          []string{kafkaBroker},
			CallbackTopic:    callbackTopic,
			CallbackDLQTopic: callbackTopic + "-dlq",
			ConsumerGroup:    fmt.Sprintf("e2e-test-%d", time.Now().UnixNano()),
		},
		Callback: config.CallbackConfig{
			MaxRetries:     3,
			BackoffBaseSec: 1,
			TimeoutSec:     5,
			SeenThreshold:  3,
		},
	}

	stumpStore := store.NewStumpStore(store.NewMemoryBlobStore(), 0, testLogger())
	ds := callback.NewDeliveryService(cfg, nil, stumpStore)
	if err := ds.Init(nil); err != nil {
		t.Fatalf("failed to init delivery service: %v", err)
	}

	ctx := context.Background()
	if err := ds.Start(ctx); err != nil {
		t.Fatalf("failed to start delivery service: %v", err)
	}
	t.Cleanup(func() { ds.Stop() })

	// Allow the consumer to settle.
	time.Sleep(500 * time.Millisecond)
	return ds, stumpStore
}

// ---------- tests ----------

// TestSeenOnNetworkCallback (10.1)
// Verifies that when a registered txid appears in a subtree, a SEEN_ON_NETWORK
// callback is delivered to the registered callback URL.
func TestSeenOnNetworkCallback(t *testing.T) {
	logger := testLogger()
	namespace := determineNamespace(t)
	asClient := newAerospikeClient(t, namespace)

	// 1. Register a txid with a callback URL in Aerospike.
	txid := randomTxID()
	setName := uniqueSetName("reg")
	regStore := store.NewRegistrationStore(asClient, setName, 3, 100, 0, logger)

	collector := newCallbackCollector()
	mockServer := httptest.NewServer(collector.handler())
	defer mockServer.Close()

	callbackURL := mockServer.URL + "/callback"
	if err := regStore.Add(txid, callbackURL); err != nil {
		t.Fatalf("failed to register txid: %v", err)
	}

	// 2. Verify the registration is readable.
	regs, err := regStore.BatchGet([]string{txid})
	if err != nil {
		t.Fatalf("failed to batch get: %v", err)
	}
	if len(regs[txid]) == 0 {
		t.Fatal("expected txid to be registered")
	}

	// 3. Set up Kafka callback topic and delivery service.
	stumpsTopic := uniqueTopic("stumps")
	ds, _ := startDeliveryService(t, stumpsTopic)
	_ = ds

	// 4. Simulate what the subtree processor does: publish a SEEN_ON_NETWORK
	//    message to the callback topic for each registered callback URL.
	callbackProducer := newCallbackProducer(t, stumpsTopic)
	stumpsMsg := &kafka.CallbackTopicMessage{
		CallbackURL: callbackURL,
		TxID:        txid,
		Type:        kafka.CallbackSeenOnNetwork,
		//"subtree-001",
		RetryCount: 0,
	}
	data, err := stumpsMsg.Encode()
	if err != nil {
		t.Fatalf("failed to encode stumps message: %v", err)
	}
	if err := callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
		t.Fatalf("failed to publish stumps message: %v", err)
	}

	// 5. Wait for the mock server to receive the callback.
	payloads := collector.waitForN(t, 1, 15*time.Second)
	if payloads[0].Type != "SEEN_ON_NETWORK" {
		t.Errorf("expected status SEEN_ON_NETWORK, got %q", payloads[0].Type)
	}
	if payloads[0].TxID != txid {
		t.Errorf("expected txid %s, got %s", txid, payloads[0].TxID)
	}
	t.Logf("SEEN_ON_NETWORK callback received for txid %s", txid)
}

// TestMinedCallbackWithSTUMP (10.2)
// Verifies that a MINED callback with stumpData is delivered after block processing.
func TestMinedCallbackWithSTUMP(t *testing.T) {
	logger := testLogger()
	namespace := determineNamespace(t)
	asClient := newAerospikeClient(t, namespace)

	// 1. Register a txid.
	txid := randomTxID()
	setName := uniqueSetName("reg")
	regStore := store.NewRegistrationStore(asClient, setName, 3, 100, 0, logger)

	collector := newCallbackCollector()
	mockServer := httptest.NewServer(collector.handler())
	defer mockServer.Close()

	callbackURL := mockServer.URL + "/mined-callback"
	if err := regStore.Add(txid, callbackURL); err != nil {
		t.Fatalf("failed to register txid: %v", err)
	}

	// 2. Set up Kafka callback topic and delivery service.
	stumpsTopic := uniqueTopic("stumps")
	_, stumpStore := startDeliveryService(t, stumpsTopic)

	// 3. Simulate what the block processor does: publish a MINED stumps message
	//    whose StumpRef points at the bytes the delivery service will fetch.
	callbackProducer := newCallbackProducer(t, stumpsTopic)
	fakeStumpData := []byte{0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB, 0xCC, 0xDD}
	stumpRef, err := stumpStore.Put(fakeStumpData, 0)
	if err != nil {
		t.Fatalf("failed to put stump: %v", err)
	}
	blockHash := "00000000000000000abc123def4567890000000000000000000000000000ffff"

	cbMsg := &kafka.CallbackTopicMessage{
		CallbackURL:  callbackURL,
		Type:         kafka.CallbackStump,
		TxID:         txid,
		BlockHash:    blockHash,
		SubtreeIndex: 1,
		StumpRef:     stumpRef,
	}
	data, err := cbMsg.Encode()
	if err != nil {
		t.Fatalf("failed to encode callback message: %v", err)
	}
	if err := callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
		t.Fatalf("failed to publish callback message: %v", err)
	}

	// 4. Wait for the mock server to receive the callback.
	payloads := collector.waitForN(t, 1, 15*time.Second)
	if payloads[0].Type != "STUMP" {
		t.Errorf("expected type STUMP, got %q", payloads[0].Type)
	}
	if payloads[0].Stump == "" {
		t.Error("expected stump to be present in STUMP callback")
	}
	if payloads[0].BlockHash != blockHash {
		t.Errorf("expected blockHash %s, got %s", blockHash, payloads[0].BlockHash)
	}
	if payloads[0].TxID != txid {
		t.Errorf("expected txid %s, got %s", txid, payloads[0].TxID)
	}
	t.Logf("STUMP callback received for txid %s", txid)
}

// TestMultipleCallbacks (10.3)
// Verifies that when a txid is registered with 2 different callback URLs,
// both mock servers receive callbacks.
func TestMultipleCallbacks(t *testing.T) {
	logger := testLogger()
	namespace := determineNamespace(t)
	asClient := newAerospikeClient(t, namespace)

	// 1. Register a txid with 2 different callback URLs.
	txid := randomTxID()
	setName := uniqueSetName("reg")
	regStore := store.NewRegistrationStore(asClient, setName, 3, 100, 0, logger)

	collector1 := newCallbackCollector()
	mockServer1 := httptest.NewServer(collector1.handler())
	defer mockServer1.Close()

	collector2 := newCallbackCollector()
	mockServer2 := httptest.NewServer(collector2.handler())
	defer mockServer2.Close()

	callbackURL1 := mockServer1.URL + "/cb1"
	callbackURL2 := mockServer2.URL + "/cb2"

	if err := regStore.Add(txid, callbackURL1); err != nil {
		t.Fatalf("failed to register txid with callback1: %v", err)
	}
	if err := regStore.Add(txid, callbackURL2); err != nil {
		t.Fatalf("failed to register txid with callback2: %v", err)
	}

	// 2. Verify both URLs are registered.
	regs, err := regStore.BatchGet([]string{txid})
	if err != nil {
		t.Fatalf("failed to batch get: %v", err)
	}
	if len(regs[txid]) != 2 {
		t.Fatalf("expected 2 callback URLs, got %d", len(regs[txid]))
	}

	// 3. Set up Kafka callback topic and delivery service.
	stumpsTopic := uniqueTopic("stumps")
	_, _ = startDeliveryService(t, stumpsTopic)
	callbackProducer := newCallbackProducer(t, stumpsTopic)

	// 4. Simulate the subtree processor: publish SEEN_ON_NETWORK for each
	//    callback URL (as the real processor would).
	for _, cbURL := range []string{callbackURL1, callbackURL2} {
		stumpsMsg := &kafka.CallbackTopicMessage{
			CallbackURL: cbURL,
			TxID:        txid,
			Type:        kafka.CallbackSeenOnNetwork,
			//"subtree-multi-001",
			RetryCount: 0,
		}
		data, err := stumpsMsg.Encode()
		if err != nil {
			t.Fatalf("failed to encode stumps message: %v", err)
		}
		if err := callbackProducer.PublishWithHashKey(cbURL, data); err != nil {
			t.Fatalf("failed to publish stumps message: %v", err)
		}
	}

	// 5. Wait for both mock servers to receive callbacks.
	payloads1 := collector1.waitForN(t, 1, 15*time.Second)
	if payloads1[0].Type != "SEEN_ON_NETWORK" {
		t.Errorf("server1: expected status SEEN_ON_NETWORK, got %q", payloads1[0].Type)
	}
	if payloads1[0].TxID != txid {
		t.Errorf("server1: expected txid %s, got %s", txid, payloads1[0].TxID)
	}

	payloads2 := collector2.waitForN(t, 1, 15*time.Second)
	if payloads2[0].Type != "SEEN_ON_NETWORK" {
		t.Errorf("server2: expected status SEEN_ON_NETWORK, got %q", payloads2[0].Type)
	}
	if payloads2[0].TxID != txid {
		t.Errorf("server2: expected txid %s, got %s", txid, payloads2[0].TxID)
	}

	t.Logf("both callback servers received SEEN_ON_NETWORK for txid %s", txid)
}

// TestSeenMultipleNodes (10.4)
// Verifies that after the seen counter hits the threshold, a SEEN_MULTIPLE_NODES
// callback is delivered.
func TestSeenMultipleNodes(t *testing.T) {
	logger := testLogger()
	namespace := determineNamespace(t)
	asClient := newAerospikeClient(t, namespace)

	// 1. Register a txid.
	txid := randomTxID()
	regSetName := uniqueSetName("reg")
	regStore := store.NewRegistrationStore(asClient, regSetName, 3, 100, 0, logger)

	collector := newCallbackCollector()
	mockServer := httptest.NewServer(collector.handler())
	defer mockServer.Close()

	callbackURL := mockServer.URL + "/seen-multi"
	if err := regStore.Add(txid, callbackURL); err != nil {
		t.Fatalf("failed to register txid: %v", err)
	}

	// 2. Create a seen counter store with threshold = 3.
	seenSetName := uniqueSetName("seen")
	threshold := 3
	seenStore := store.NewSeenCounterStore(asClient, seenSetName, threshold, 3, 100, logger)

	// 3. Simulate multiple subtree appearances: increment the seen counter
	//    multiple times and track when threshold is reached.
	var thresholdReached bool
	for i := 0; i < threshold+1; i++ {
		result, err := seenStore.Increment(txid, fmt.Sprintf("subtree-%d", i))
		if err != nil {
			t.Fatalf("failed to increment seen counter (iteration %d): %v", i, err)
		}
		if result.ThresholdReached {
			thresholdReached = true
			t.Logf("threshold reached at count %d", result.NewCount)
		}
		// After threshold, ThresholdReached should be false (only true at exactly threshold).
		if i == threshold && result.ThresholdReached {
			t.Errorf("expected ThresholdReached=false above threshold, count=%d", result.NewCount)
		}
	}

	if !thresholdReached {
		t.Fatal("expected threshold to be reached during increments")
	}

	// 4. Set up Kafka callback topic and delivery service.
	stumpsTopic := uniqueTopic("stumps")
	_, _ = startDeliveryService(t, stumpsTopic)
	callbackProducer := newCallbackProducer(t, stumpsTopic)

	// 5. Publish a SEEN_MULTIPLE_NODES message (as the subtree processor would
	//    when the threshold is reached).
	stumpsMsg := &kafka.CallbackTopicMessage{
		CallbackURL: callbackURL,
		TxID:        txid,
		Type:        kafka.CallbackSeenMultipleNodes,
		//"subtree-multi-nodes-001",
		RetryCount: 0,
	}
	data, err := stumpsMsg.Encode()
	if err != nil {
		t.Fatalf("failed to encode stumps message: %v", err)
	}
	if err := callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
		t.Fatalf("failed to publish stumps message: %v", err)
	}

	// 6. Wait for the mock server to receive the callback.
	payloads := collector.waitForN(t, 1, 15*time.Second)
	if payloads[0].Type != "SEEN_MULTIPLE_NODES" {
		t.Errorf("expected status SEEN_MULTIPLE_NODES, got %q", payloads[0].Type)
	}
	if payloads[0].TxID != txid {
		t.Errorf("expected txid %s, got %s", txid, payloads[0].TxID)
	}

	// 7. Verify that incrementing beyond threshold does NOT set ThresholdReached again.
	result, err := seenStore.Increment(txid, "subtree-extra")
	if err != nil {
		t.Fatalf("failed to increment seen counter past threshold: %v", err)
	}
	if result.ThresholdReached {
		t.Errorf("expected ThresholdReached=false after passing threshold, count=%d", result.NewCount)
	}

	t.Logf("SEEN_MULTIPLE_NODES callback received for txid %s", txid)
}
