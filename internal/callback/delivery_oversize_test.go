package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/logfields"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// These tests pin the semantics introduced after the 2026-08-11 dev-ovh-1
// incident (1000 TPS): a subtree's STUMP, hex-encoded, exceeded arcade's
// then-default 16 MiB `callback.max_body_bytes`; arcade answered
// 413 Request Entity Too Large; merkle classified the 4xx as permanent and
// published the callback to `callback-dlq` after ZERO retries. arcade's
// bump-builder then logged, forever,
//
//	"BLOCK_PROCESSED is missing expected STUMPs — deferring finalization"
//	expected_stumps:1 received_stumps:0 missing_subtree_indices:[0]
//
// so the block never got a BUMP and every transaction in it never reached
// MINED.
//
// The pinned semantics are:
//  1. 413 is NOT permanent — it keeps its retry budget, so an operator
//     raising the receiver's cap heals the block automatically.
//  2. 413 never trips the per-URL circuit breaker, because disabling
//     arcade's callback URL would strand every transaction, not just the
//     oversize block's.
//  3. The condition is loudly observable: a dedicated metric outcome plus an
//     ERROR naming the block hash, subtree index and exact body size.
//  4. A configured `callback.maxBodyBytes` refuses the POST up front instead
//     of uploading a body the receiver is certain to reject.

// recordingURLRegistry captures RecordFailure calls so a test can assert the
// per-URL circuit breaker was (or was not) advanced.
type recordingURLRegistry struct {
	failures atomic.Int32
}

func (r *recordingURLRegistry) Add(_, _ string) error { return nil }

func (r *recordingURLRegistry) GetAll() ([]store.CallbackEntry, error) { return nil, nil }

func (r *recordingURLRegistry) RecordFailure(_ string, _ int) (bool, error) {
	r.failures.Add(1)
	return false, nil
}

var _ store.CallbackURLRegistry = (*recordingURLRegistry)(nil)

// newOversizeTestServer returns an httptest server that always answers 413,
// mimicking arcade's http.MaxBytesReader → StatusRequestEntityTooLarge
// mapping, plus a counter of how many requests actually arrived.
func newOversizeTestServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = w.Write([]byte(`{"error":"request body too large"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// TestDeliverCallback_413IsNotPermanent is the narrowest statement of the
// bug: deliverCallback must classify a 413 as an oversize failure, not a
// permanent one. isPermanentDeliveryError deciding "true" here is precisely
// what routed the incident's STUMP to the DLQ with no retries.
func TestDeliverCallback_413IsNotPermanent(t *testing.T) {
	server, hits := newOversizeTestServer(t)

	cfg := defaultTestConfig()
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    testBlockHash,
		SubtreeIndex: 4,
	}

	err := ds.deliverCallback(context.Background(), msg)
	if err == nil {
		t.Fatal("expected an error for a 413 response, got nil")
	}
	if isPermanentDeliveryError(err) {
		t.Fatal("413 must NOT be classified as permanent — that is what strands the block")
	}
	o := asOversizeDeliveryError(err)
	if o == nil {
		t.Fatalf("expected an oversizeDeliveryError, got %T: %v", err, err)
	}
	if o.statusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("statusCode: expected 413, got %d", o.statusCode)
	}
	if o.bodyBytes <= 0 {
		t.Errorf("bodyBytes: expected the sent body size to be recorded, got %d", o.bodyBytes)
	}
	if hits.Load() != 1 {
		t.Errorf("expected exactly 1 request to reach the receiver, got %d", hits.Load())
	}
}

// TestIsNonRetryable4xx_413IsExcluded guards the classifier itself. Even if a
// future caller stops peeling 413 off ahead of this function, it must not
// re-acquire "permanent" semantics through this path.
func TestIsNonRetryable4xx_413IsExcluded(t *testing.T) {
	if isNonRetryable4xx(http.StatusRequestEntityTooLarge) {
		t.Error("413 must not be reported as a non-retryable 4xx")
	}
	// The genuinely-permanent codes must be unchanged by this fix.
	for _, code := range []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone,
		http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity,
	} {
		if !isNonRetryable4xx(code) {
			t.Errorf("status %d should still be a non-retryable 4xx", code)
		}
	}
	// And the pre-existing retryable ones too.
	for _, code := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		if isNonRetryable4xx(code) {
			t.Errorf("status %d must stay retryable", code)
		}
	}
}

// TestProcessDelivery_413DoesNotGoStraightToDLQ is the regression test for
// the incident. Before the fix this produced exactly one DLQ message and zero
// retries; it must now produce exactly one retry republish and zero DLQ
// messages.
func TestProcessDelivery_413DoesNotGoStraightToDLQ(t *testing.T) {
	server, _ := newOversizeTestServer(t)

	cfg := defaultTestConfig()
	cfg.Callback.BreakerThreshold = 1 // trip on the very first failure, if it were reachable
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())
	registry := &recordingURLRegistry{}
	ds.urlRegistry = registry

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-oversize",
		SubtreeIndex: 0,
		RetryCount:   0,
	}

	beforeOversize := callbackCount(metrics.OutcomeOversize)
	beforeDLQ := callbackCount(metrics.OutcomeDLQ)

	if err := ds.processDelivery(context.Background(), "", msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

	if got := len(dlqMock.getMessages()); got != 0 {
		t.Fatalf("a 413 must not be DLQ'd on the first attempt; got %d DLQ message(s)", got)
	}
	retries := retryMock.getMessages()
	if len(retries) != 1 {
		t.Fatalf("expected exactly 1 retry republish, got %d", len(retries))
	}
	retried := decodePublishedCallbackMessage(t, retries[0])
	if retried.RetryCount != 1 {
		t.Errorf("retryCount: expected 1, got %d", retried.RetryCount)
	}
	if retried.NextRetryAt.IsZero() {
		t.Error("expected the retry to carry a nextRetryAt backoff stamp")
	}
	if retried.BlockHash != "blockhash-oversize" {
		t.Errorf("blockHash lost on retry: %q", retried.BlockHash)
	}

	if delta := callbackCount(metrics.OutcomeOversize) - beforeOversize; delta != 1 {
		t.Errorf("expected oversize counter delta=1, got %d", delta)
	}
	if delta := callbackCount(metrics.OutcomeDLQ) - beforeDLQ; delta != 0 {
		t.Errorf("expected DLQ counter delta=0, got %d", delta)
	}
	if registry.failures.Load() != 0 {
		t.Errorf("the per-URL circuit breaker must not advance on an oversize retry, got %d failures", registry.failures.Load())
	}
}

// TestProcessDelivery_413ExhaustedRetriesStrandsLoudlyWithoutTrippingBreaker
// pins the terminal case. Once the retry budget is spent the message does go
// to the DLQ — but under its own `oversize_stranded` outcome (the metric an
// operator pages on) and WITHOUT advancing the per-URL circuit breaker.
//
// The breaker exclusion is the important half: arcade registers one callback
// URL for the whole deployment, so a block whose subtrees all produce
// oversize STUMPs would cross the default threshold of 20 in a single block
// and auto-disable that URL — killing SEEN and BLOCK_PROCESSED delivery for
// every transaction in flight, not just the stranded block's.
func TestProcessDelivery_413ExhaustedRetriesStrandsLoudlyWithoutTrippingBreaker(t *testing.T) {
	server, _ := newOversizeTestServer(t)

	cfg := defaultTestConfig()
	cfg.Callback.MaxRetries = 3
	cfg.Callback.BreakerThreshold = 1
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())
	registry := &recordingURLRegistry{}
	ds.urlRegistry = registry

	var buf bytes.Buffer
	ds.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-stranded",
		SubtreeIndex: 11,
		RetryCount:   3, // budget already spent
	}

	beforeStranded := callbackCount(metrics.OutcomeOversizeStranded)

	if err := ds.processDelivery(context.Background(), "", msg); err != nil {
		t.Fatalf("processDelivery returned error: %v", err)
	}

	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected no further retries once the budget is spent, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 1 {
		t.Fatalf("expected exactly 1 DLQ message, got %d", got)
	}
	if delta := callbackCount(metrics.OutcomeOversizeStranded) - beforeStranded; delta != 1 {
		t.Errorf("expected oversize_stranded counter delta=1, got %d", delta)
	}
	if registry.failures.Load() != 0 {
		t.Errorf("the per-URL circuit breaker must never advance on oversize, got %d failures", registry.failures.Load())
	}

	entry := findLogEntry(t, buf.Bytes(), func(e map[string]any) bool {
		return e["reason"] == "oversize"
	})
	if entry["level"] != "ERROR" {
		t.Errorf("expected the strand notice at ERROR, got %v", entry["level"])
	}
	if entry[logfields.KeyBlockHash] != "blockhash-stranded" {
		t.Errorf("strand notice must name the block hash; got %v", entry[logfields.KeyBlockHash])
	}
	if entry[logfields.KeySubtreeIndex] != float64(11) {
		t.Errorf("strand notice must name the subtree index; got %v", entry[logfields.KeySubtreeIndex])
	}
	if v, ok := entry["bodyBytes"].(float64); !ok || v <= 0 {
		t.Errorf("strand notice must name the body size; got %v", entry["bodyBytes"])
	}
	if v, ok := entry["status"].(float64); !ok || int(v) != http.StatusRequestEntityTooLarge {
		t.Errorf("strand notice must name the status; got %v", entry["status"])
	}
}

// TestDeliverCallback_413LogsBlockIdentityAndSize pins requirement 4 of the
// fix at the point of detection: the ERROR must carry block hash, subtree
// index and the exact body size, so an operator can tell instantly which
// block is stuck and by how much the payload overshot.
func TestDeliverCallback_413LogsBlockIdentityAndSize(t *testing.T) {
	server, _ := newOversizeTestServer(t)

	cfg := defaultTestConfig()
	cfg.Callback.MaxBodyBytes = 0 // learn the limit from the 413, not locally
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	var buf bytes.Buffer
	ds.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-413-log",
		SubtreeIndex: 7,
		TxID:         "tx-413",
	}

	if err := ds.deliverCallback(context.Background(), msg); err == nil {
		t.Fatal("expected an error for a 413 response")
	}

	entry := findLogEntry(t, buf.Bytes(), func(e map[string]any) bool {
		v, ok := e["status"].(float64)
		return ok && int(v) == http.StatusRequestEntityTooLarge
	})
	if entry[logfields.KeyBlockHash] != "blockhash-413-log" {
		t.Errorf("missing/wrong %s: %v", logfields.KeyBlockHash, entry[logfields.KeyBlockHash])
	}
	if entry[logfields.KeySubtreeIndex] != float64(7) {
		t.Errorf("missing/wrong %s: %v", logfields.KeySubtreeIndex, entry[logfields.KeySubtreeIndex])
	}
	if entry[logfields.KeyCallbackURL] != msg.CallbackURL {
		t.Errorf("missing/wrong %s: %v", logfields.KeyCallbackURL, entry[logfields.KeyCallbackURL])
	}
	if v, ok := entry["bodyBytes"].(float64); !ok || v <= 0 {
		t.Errorf("missing/wrong bodyBytes: %v", entry["bodyBytes"])
	}
	if entry["type"] != "STUMP" {
		t.Errorf("missing/wrong type: %v", entry["type"])
	}
}

// TestDeliverCallback_PreflightMaxBodyBytesRefusesThePost pins the pre-flight
// check: with callback.maxBodyBytes configured, an over-limit body is never
// uploaded. The receiver must see zero requests — the point is to turn a
// doomed multi-MiB POST plus a request timeout into an immediate, fully
// contextualized local diagnosis.
func TestDeliverCallback_PreflightMaxBodyBytesRefusesThePost(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	cfg.Callback.MaxBodyBytes = 32 // smaller than any real payload
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	var buf bytes.Buffer
	ds.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-preflight",
		SubtreeIndex: 2,
	}

	beforeOversize := callbackCount(metrics.OutcomeOversize)

	err := ds.deliverCallback(context.Background(), msg)
	if err == nil {
		t.Fatal("expected an oversize error from the pre-flight check")
	}
	if hits.Load() != 0 {
		t.Errorf("pre-flight must not send the request; receiver saw %d hit(s)", hits.Load())
	}
	o := asOversizeDeliveryError(err)
	if o == nil {
		t.Fatalf("expected an oversizeDeliveryError, got %T: %v", err, err)
	}
	if isPermanentDeliveryError(err) {
		t.Error("a pre-flight oversize refusal must not be permanent")
	}
	if o.statusCode != 0 {
		t.Errorf("statusCode: expected 0 (never sent), got %d", o.statusCode)
	}
	if o.limitBytes != 32 {
		t.Errorf("limitBytes: expected 32, got %d", o.limitBytes)
	}
	if delta := callbackCount(metrics.OutcomeOversize) - beforeOversize; delta != 1 {
		t.Errorf("expected oversize counter delta=1, got %d", delta)
	}

	entry := findLogEntry(t, buf.Bytes(), func(e map[string]any) bool {
		return e[logfields.KeyBlockHash] == "blockhash-preflight"
	})
	if v, ok := entry["limitBytes"].(float64); !ok || int64(v) != 32 {
		t.Errorf("pre-flight log must name the configured limit; got %v", entry["limitBytes"])
	}
	if v, ok := entry["bodyBytes"].(float64); !ok || v <= 32 {
		t.Errorf("pre-flight log must name the over-limit body size; got %v", entry["bodyBytes"])
	}
}

// TestDeliverCallback_PreflightDisabledByDefault guards the non-breaking
// default: maxBodyBytes=0 means "trust the receiver". merkle cannot know a
// third-party receiver's cap, so a non-zero default here would start refusing
// bodies the receiver would happily have accepted.
func TestDeliverCallback_PreflightDisabledByDefault(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	if cfg.Callback.MaxBodyBytes != 0 {
		t.Fatalf("test precondition: expected MaxBodyBytes 0, got %d", cfg.Callback.MaxBodyBytes)
	}
	ds, _, _ := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    testBlockHash,
		SubtreeIndex: 1,
	}
	if err := ds.deliverCallback(context.Background(), msg); err != nil {
		t.Fatalf("delivery should succeed with the pre-flight check disabled: %v", err)
	}
	if hits.Load() != 1 {
		t.Errorf("expected the request to be sent, receiver saw %d hit(s)", hits.Load())
	}
}

// TestHandleMessage_413RepublishesBeforeAck ties the new class back to the
// durability contract: handleMessage may only return nil once the retry
// republish has been acknowledged by Kafka. If the republish fails, the
// offset must stay uncommitted so the oversize callback is reconsidered —
// never silently dropped.
func TestHandleMessage_413RepublishesBeforeAck(t *testing.T) {
	server, _ := newOversizeTestServer(t)

	cfg := defaultTestConfig()
	ds, retryMock, dlqMock := newTestDeliveryService(t, cfg, server.Client())

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-durable",
		SubtreeIndex: 5,
	}

	// Happy path: republished, then ack'd.
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, msg)); err != nil {
		t.Fatalf("handleMessage returned error: %v", err)
	}
	if len(retryMock.getMessages()) != 1 {
		t.Fatalf("expected 1 retry republish, got %d", len(retryMock.getMessages()))
	}
	if len(dlqMock.getMessages()) != 0 {
		t.Fatalf("expected 0 DLQ messages, got %d", len(dlqMock.getMessages()))
	}

	// Republish fails: must NOT ack.
	retryMock.failNext = 1
	fresh := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-durable-2",
		SubtreeIndex: 6,
	}
	if err := ds.handleMessage(context.Background(), encodeConsumerMessage(t, fresh)); err == nil {
		t.Fatal("expected a non-nil error so the Kafka offset stays uncommitted")
	}
}

// TestDeliverCallback_BodyWarnThresholdLogs pins the early-warning signal:
// a body over bodyWarnBytes logs a WARN naming the block, subtree and size
// even when the POST succeeds, so growth is visible BEFORE it becomes an
// outage. The body is inflated via a real STUMP blob so the warning is
// exercised through the same hex-encode path that doubled the payload in the
// incident.
func TestDeliverCallback_BodyWarnThresholdLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultTestConfig()
	ds, _, _, stumpStore := newTestDeliveryServiceWithStumps(t, cfg, server.Client())

	var buf bytes.Buffer
	ds.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Just over half the warn threshold in raw bytes; hex doubles it, which
	// is exactly the inflation that made a "small enough" STUMP too large.
	raw := bytes.Repeat([]byte{0xab}, (bodyWarnBytes/2)+1024)
	ref, err := stumpStore.Put(raw, 0)
	if err != nil {
		t.Fatalf("storing stump blob: %v", err)
	}

	msg := &kafka.CallbackTopicMessage{
		CallbackURL:  server.URL + "/callback",
		Type:         kafka.CallbackStump,
		BlockHash:    "blockhash-warn",
		SubtreeIndex: 3,
		StumpRef:     ref,
	}
	if err := ds.deliverCallback(context.Background(), msg); err != nil {
		t.Fatalf("delivery should still succeed: %v", err)
	}

	entry := findLogEntry(t, buf.Bytes(), func(e map[string]any) bool {
		return e["level"] == "WARN" && e[logfields.KeyBlockHash] == "blockhash-warn"
	})
	if v, ok := entry["bodyBytes"].(float64); !ok || int(v) <= bodyWarnBytes {
		t.Errorf("warn must name a body size above the threshold; got %v", entry["bodyBytes"])
	}
	if entry[logfields.KeySubtreeIndex] != float64(3) {
		t.Errorf("warn must name the subtree index; got %v", entry[logfields.KeySubtreeIndex])
	}
}

// findLogEntry decodes the JSON-lines log buffer and returns the first entry
// matching pred, failing the test when none does. Delivery emits several
// lines per attempt, so tests must select rather than assume a single entry.
func findLogEntry(t *testing.T, raw []byte, pred func(map[string]any) bool) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("decode log entry: %v\nraw: %s", err, line)
		}
		if pred(entry) {
			return entry
		}
	}
	t.Fatalf("no matching log entry found in:\n%s", raw)
	return nil
}

// TestStoreBody_ByteBudgetEvicts pins the memory bound that makes the STUMP
// body cache safe now that bodies can legitimately be huge.
//
// bodyCacheMaxEntries alone is only a safe bound while entries are a
// predictable size. The 2026-08-11 incident proved they are not — and arcade's
// cap has since been raised to 128 MiB so that bodies that large CAN be
// delivered. Count-bounded, the cache would then hold 64 x 128 MiB ~= 8 GiB
// and OOM-kill the delivery pod, escalating "one block's STUMPs are oversized"
// into "callback delivery is dead".
func TestStoreBody_ByteBudgetEvicts(t *testing.T) {
	ds := &DeliveryService{}

	// Entries far below the count limit, but together far above the byte
	// budget: the byte bound must be the one that binds.
	const chunk = bodyCacheMaxBytes / 4
	for i := range 8 {
		ds.storeBody(strconv.Itoa(i), make([]byte, chunk))
	}

	if ds.bodyBytes > bodyCacheMaxBytes {
		t.Errorf("resident bytes %d exceed budget %d", ds.bodyBytes, bodyCacheMaxBytes)
	}
	if len(ds.bodyCache) != len(ds.bodyOrder) {
		t.Errorf("cache/order desync: %d entries vs %d order slots", len(ds.bodyCache), len(ds.bodyOrder))
	}
	// bodyBytes must stay an exact running sum, or the budget silently drifts.
	var sum int
	for _, v := range ds.bodyCache {
		sum += len(v)
	}
	if sum != ds.bodyBytes {
		t.Errorf("bodyBytes drifted: tracked %d, actual %d", ds.bodyBytes, sum)
	}
	// FIFO: the newest entry survives, the oldest does not.
	if ds.cachedBody("7") == nil {
		t.Error("most recently stored entry was evicted")
	}
	if ds.cachedBody("0") != nil {
		t.Error("oldest entry should have been evicted by the byte budget")
	}
}

// TestStoreBody_EntryBudgetStillEvicts guards the original count bound: small
// bodies must still be capped at bodyCacheMaxEntries, since the byte budget
// would never bind for them.
func TestStoreBody_EntryBudgetStillEvicts(t *testing.T) {
	ds := &DeliveryService{}
	for i := range bodyCacheMaxEntries + 10 {
		ds.storeBody(strconv.Itoa(i), []byte("small"))
	}
	if len(ds.bodyCache) != bodyCacheMaxEntries {
		t.Errorf("expected %d entries, got %d", bodyCacheMaxEntries, len(ds.bodyCache))
	}
	if ds.cachedBody("0") != nil {
		t.Error("oldest entry should have been evicted by the count budget")
	}
}

// TestStoreBody_OversizedSingleBodyIsStillCached pins the degenerate case: a
// single body larger than the whole budget is cached anyway, alone. Refusing
// it would restore precisely the per-subscriber re-fetch/re-hex/re-marshal
// duplication the cache exists to eliminate — and that duplication is at its
// most expensive exactly when the body is at its largest.
func TestStoreBody_OversizedSingleBodyIsStillCached(t *testing.T) {
	ds := &DeliveryService{}
	ds.storeBody("small", []byte("x"))
	huge := make([]byte, bodyCacheMaxBytes+1)
	ds.storeBody("huge", huge)

	if ds.cachedBody("huge") == nil {
		t.Fatal("an over-budget body must still be cached, so the fan-out does not rebuild it per subscriber")
	}
	if len(ds.bodyCache) != 1 {
		t.Errorf("expected the over-budget body to be the only resident entry, got %d", len(ds.bodyCache))
	}
}

// TestStoreBody_DuplicateKeyDoesNotDoubleCount guards the running sum against
// the classic incremental-accounting bug: re-storing an existing key must not
// add its size again, or bodyBytes drifts upward until the cache evicts
// everything on every insert.
func TestStoreBody_DuplicateKeyDoesNotDoubleCount(t *testing.T) {
	ds := &DeliveryService{}
	body := make([]byte, 1024)
	ds.storeBody("k", body)
	ds.storeBody("k", body)

	if ds.bodyBytes != 1024 {
		t.Errorf("bodyBytes: expected 1024 after a duplicate store, got %d", ds.bodyBytes)
	}
	if len(ds.bodyOrder) != 1 {
		t.Errorf("order: expected 1 entry after a duplicate store, got %d", len(ds.bodyOrder))
	}
}
