package callback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// These tests cover the durability contract that handleMessage must obey
// after F-021: returning nil ONLY when the message has reached a durable
// terminal state (delivered + dedup recorded, or republished to retry topic,
// or successfully published to the DLQ). When the durable side-effect
// itself fails, handleMessage MUST return a non-nil error so the
// consumer-group loop skips MarkMessage and the offset stays uncommitted —
// the message will be re-delivered on the next session.

// encodeConsumerMessage builds a sarama.ConsumerMessage carrying the
// JSON-encoded CallbackTopicMessage, mimicking what the broker delivers.
func encodeConsumerMessage(t *testing.T, msg *kafka.CallbackTopicMessage) *sarama.ConsumerMessage {
	t.Helper()
	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("failed to encode CallbackTopicMessage: %v", err)
	}
	return &sarama.ConsumerMessage{Value: data}
}

// TestHandleMessage_HappyPathReturnsNilAfterDelivery covers the standard
// "deliver, record dedup, ack" path: handleMessage returns nil only after
// HTTP POST has succeeded, so the offset can safely advance.
func TestHandleMessage_HappyPathReturnsNilAfterDelivery(t *testing.T) {
	var deliveries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveries.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-happy",
	}

	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil, got error: %v", err)
	}
	if deliveries.Load() != 1 {
		t.Errorf("expected 1 HTTP delivery, got %d", deliveries.Load())
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry republishes for happy path, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected 0 DLQ publishes for happy path, got %d", got)
	}
	if ds.messagesProcessed.Load() != 1 {
		t.Errorf("expected messagesProcessed=1, got %d", ds.messagesProcessed.Load())
	}
}

// TestHandleMessage_HTTPFailureWithRetriesAvailable_RepublishesBeforeAck
// asserts the central F-021 invariant: when delivery fails but retries are
// available, handleMessage republishes the message to the callback topic
// (durable retry channel) and only returns nil if the publish succeeds.
func TestHandleMessage_HTTPFailureWithRetriesAvailable_RepublishesBeforeAck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-retry",
		RetryCount:  0,
	}

	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil after successful republish, got error: %v", err)
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
		t.Error("expected republished NextRetryAt to be set so future consumers know to wait")
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected 0 DLQ publishes when retries are still available, got %d", got)
	}
}

// TestHandleMessage_HTTPFailureRetriesExhausted_RoutesToDLQBeforeAck asserts
// that when retries are exhausted, handleMessage publishes to the DLQ and
// only returns nil if the DLQ publish succeeds.
func TestHandleMessage_HTTPFailureRetriesExhausted_RoutesToDLQBeforeAck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	cfg.Callback.MaxRetries = 3
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-exhausted",
		RetryCount:  3,
	}

	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil after successful DLQ publish, got error: %v", err)
	}

	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry republishes after retries exhausted, got %d", got)
	}
	dlqMsgs := dlqMock.getMessages()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ publish, got %d", len(dlqMsgs))
	}
	if ds.messagesFailed.Load() != 1 {
		t.Errorf("expected messagesFailed=1, got %d", ds.messagesFailed.Load())
	}
}

// TestHandleMessage_DLQPublishFailureReturnsError simulates Kafka being
// unavailable for the DLQ publish on a retries-exhausted message: handler
// must return non-nil so the consumer-group loop leaves the offset
// uncommitted. This is what guarantees we don't silently drop messages on
// downstream-Kafka outages — instead they're reconsidered next session.
func TestHandleMessage_DLQPublishFailureReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	cfg.Callback.MaxRetries = 3
	ds, _, dlqMock := newTestDeliveryService(t, cfg, server.Client())
	dlqMock.failNext = 100 // exhaust the in-call DLQ publish retry budget

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-dlq-fail",
		RetryCount:  3,
	}

	err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg))
	if err == nil {
		t.Fatal("expected handleMessage to return non-nil error when DLQ publish fails")
	}
	if ds.messagesFailed.Load() != 0 {
		t.Errorf("expected messagesFailed=0 (DLQ never durably accepted), got %d", ds.messagesFailed.Load())
	}
}

// TestHandleMessage_RetryRepublishFailureReturnsError simulates Kafka being
// unavailable for the retry republish: the handler must return non-nil so
// the offset is not committed and the message can be retried on the next
// session. This is the primary guarantee that fixes F-021.
func TestHandleMessage_RetryRepublishFailureReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, server.Client())
	retryMock.failNext = 1 // single republish attempt that fails

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-republish-fail",
	}

	err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg))
	if err == nil {
		t.Fatal("expected handleMessage to return non-nil error when retry republish fails")
	}
}

// TestHandleMessage_FutureRetryShortDelaySleepsInline covers the small-delay
// branch: when NextRetryAt is within futureRetryWaitCap, handleMessage
// sleeps inline and then delivers, returning nil only after the delivery
// has succeeded. No republish should happen because the inline wait
// covered the whole delay.
func TestHandleMessage_FutureRetryShortDelaySleepsInline(t *testing.T) {
	var deliveries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveries.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, server.Client())

	delay := 50 * time.Millisecond
	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-future-short",
		RetryCount:  1,
		NextRetryAt: time.Now().Add(delay),
	}

	start := time.Now()
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil, got error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < delay {
		t.Errorf("handler returned before delay elapsed: %v < %v", elapsed, delay)
	}
	if deliveries.Load() != 1 {
		t.Errorf("expected 1 HTTP delivery after inline wait, got %d", deliveries.Load())
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 republishes for short-delay future retry, got %d", got)
	}
}

// TestHandleMessage_FutureRetryLongDelayRepublishes covers the large-delay
// branch: when NextRetryAt is further out than futureRetryWaitCap,
// handleMessage waits the cap (to slow the consume/republish cadence) and
// then republishes the message — preserving NextRetryAt — and returns nil
// only if the republish succeeds.
func TestHandleMessage_FutureRetryLongDelayRepublishes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP delivery should not happen for a long-delay future retry — message should be republished")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, server.Client())

	farFuture := futureRetryWaitCap * 10
	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-future-long",
		RetryCount:  1,
		NextRetryAt: time.Now().Add(farFuture),
	}

	start := time.Now()
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil after successful republish, got error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < futureRetryWaitCap {
		t.Errorf("handler returned before futureRetryWaitCap elapsed: %v < %v", elapsed, futureRetryWaitCap)
	}
	if elapsed > 2*futureRetryWaitCap {
		t.Errorf("handler blocked for far longer than futureRetryWaitCap: %v", elapsed)
	}

	retryMsgs := retryMock.getMessages()
	if len(retryMsgs) != 1 {
		t.Fatalf("expected 1 retry republish, got %d", len(retryMsgs))
	}
	republished := decodePublishedCallbackMessage(t, retryMsgs[0])
	if republished.NextRetryAt.IsZero() {
		t.Error("expected republished NextRetryAt to remain set")
	}
	if !republished.NextRetryAt.Equal(msg.NextRetryAt) {
		t.Errorf("expected NextRetryAt preserved (%v), got %v", msg.NextRetryAt, republished.NextRetryAt)
	}
	// RetryCount must NOT be bumped on a future-dated republish — the retry
	// budget has already been spent by the consumer that produced this
	// scheduled retry.
	if republished.RetryCount != 1 {
		t.Errorf("expected republished RetryCount unchanged (1), got %d", republished.RetryCount)
	}
}

// TestHandleMessage_PoisonPillReturnsNil asserts that a malformed message
// (cannot be decoded) is logged and ack'd so it doesn't pin the partition
// forever. This is unrelated to the durability contract for valid
// messages, but worth pinning down so future refactors don't accidentally
// turn poison pills into a stuck-consumer outage.
func TestHandleMessage_PoisonPillReturnsNil(t *testing.T) {
	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, &http.Client{Timeout: time.Second})

	if err := ds.handleMessage(context.Background(), &sarama.ConsumerMessage{Value: []byte("not-json")}); err != nil {
		t.Errorf("expected nil for poison-pill message, got: %v", err)
	}
}

// TestHandleMessage_PermanentErrorRoutesToDLQBeforeAck asserts that a
// permanent delivery failure (e.g. STUMP blob missing) produces a DLQ
// publish synchronously before the handler returns nil.
func TestHandleMessage_PermanentErrorRoutesToDLQBeforeAck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("callback URL should never be hit for a missing-blob STUMP message")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, dlqMock, _ := newTestDeliveryServiceWithStumps(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/cb",
		Type:         kafka.CallbackStump,
		BlockHash:    "00000000000000000000000000000000000000000000000000000000b1bcfee0",
		SubtreeIndex: 2,
		StumpRef:     "ref-never-stored",
	}

	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil after successful DLQ publish, got error: %v", err)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry republishes for permanent error, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 1 {
		t.Errorf("expected 1 DLQ publish for permanent error, got %d", got)
	}
}

// TestHandleMessage_StumpOrderingViaSamePartitionSerialization asserts the
// new (stumpGate-free) ordering guarantee: because the consumer-group loop
// processes messages from a single partition serially, and STUMPs land on
// the same partition as BLOCK_PROCESSED for the same callbackURL, calling
// handleMessage in STUMP-then-BLOCK_PROCESSED order naturally delivers in
// that order too. No in-process gate is required.
func TestHandleMessage_StumpOrderingViaSamePartitionSerialization(t *testing.T) {
	var mu sync.Mutex
	var deliveries []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload callbackPayload
		_ = json.Unmarshal(body, &payload)
		mu.Lock()
		deliveries = append(deliveries, payload.Type)
		mu.Unlock()
		// Simulate slow STUMP delivery to give a hypothetical concurrent
		// BLOCK_PROCESSED a chance to race ahead. With sequential handler
		// calls, there's no concurrency to exploit.
		if payload.Type == "STUMP" {
			time.Sleep(20 * time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _, stumpStore := newTestDeliveryServiceWithStumps(t, cfg, server.Client())

	url := server.URL + "/cb"
	blk := "0000000000000000000000000000000000000000000000000000000050a1b15e"
	mustPut := func(b []byte) string {
		ref, err := stumpStore.Put(b, 0)
		if err != nil {
			t.Fatalf("put stump: %v", err)
		}
		return ref
	}
	stumps := []*kafka.CallbackTopicMessage{
		{CallbackURL: url, Type: kafka.CallbackStump, BlockHash: blk, SubtreeIndex: 0, StumpRef: mustPut([]byte{0x01})},
		{CallbackURL: url, Type: kafka.CallbackStump, BlockHash: blk, SubtreeIndex: 1, StumpRef: mustPut([]byte{0x02})},
		{CallbackURL: url, Type: kafka.CallbackStump, BlockHash: blk, SubtreeIndex: 2, StumpRef: mustPut([]byte{0x03})},
	}
	bp := &kafka.CallbackTopicMessage{CallbackURL: url, Type: kafka.CallbackBlockProcessed, BlockHash: blk}

	// Drive the handler the way the consumer-group loop would: one message
	// at a time, in partition order. Same callback URL → same partition,
	// so BLOCK_PROCESSED follows the STUMPs.
	for _, s := range stumps {
		if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, s)); err != nil {
			t.Fatalf("STUMP handleMessage failed: %v", err)
		}
	}
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, bp)); err != nil {
		t.Fatalf("BLOCK_PROCESSED handleMessage failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(deliveries) != 4 {
		t.Fatalf("expected 4 deliveries, got %v", deliveries)
	}
	for i := 0; i < 3; i++ {
		if deliveries[i] != "STUMP" {
			t.Errorf("expected STUMP at position %d, got %q (full order: %v)", i, deliveries[i], deliveries)
		}
	}
	if deliveries[3] != "BLOCK_PROCESSED" {
		t.Errorf("expected BLOCK_PROCESSED last, got %q (full order: %v)", deliveries[3], deliveries)
	}
}

// TestHandleMessage_CrashSimulationOffsetUncommitted is the unit-level
// stand-in for the integration property that, on a real Kafka cluster, a
// crash between consume and durable success would cause the offset to stay
// uncommitted. We simulate the durable-side-effect failure (retry-publish
// or DLQ-publish failing) and assert the handler returns non-nil — which
// is what tells the consumer-group loop NOT to MarkMessage. On the next
// session, the broker re-delivers the same record.
func TestHandleMessage_CrashSimulationOffsetUncommitted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, retryMock, _ := newTestDeliveryService(t, cfg, server.Client())
	retryMock.failNext = 100 // simulate Kafka retry-publish unavailable

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL + "/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "tx-crash-sim",
	}
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err == nil {
		t.Fatal("expected handleMessage to return error so the consumer skips MarkMessage")
	}

	// Now simulate "next session": Kafka recovers and the same message is
	// re-delivered. With the retry producer healthy, the handler should
	// successfully republish and return nil — the durability contract is
	// satisfied without losing the message.
	retryMock.failNext = 0
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("expected nil on re-delivery once retry producer recovers, got error: %v", err)
	}
	if got := len(retryMock.getMessages()); got != 1 {
		t.Errorf("expected 1 successful retry republish on recovery, got %d", got)
	}
}
