package callback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/ssrfguard"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// CallbackDeduper abstracts callback deduplication for testability.
type CallbackDeduper interface {
	Exists(txid, callbackURL, statusType string) (bool, error)
	Record(txid, callbackURL, statusType string, ttl time.Duration) error
}

// futureRetryWaitCap is the maximum amount of time handleMessage will block
// in-process while waiting for a not-yet-due retry to mature. Anything larger
// than this and the consumer republishes the message back to the callback
// topic with the same NextRetryAt and returns — so a long backoff converts
// into N short consume/sleep/republish cycles instead of pinning a partition.
// Kept well below Sarama's default Consumer.Group.Session.Timeout (10s) so we
// never trip a rebalance from a single in-flight wait.
const futureRetryWaitCap = 2 * time.Second

// permanentDeliveryError marks a delivery failure that cannot be cured by
// retrying (e.g. the STUMP blob has expired or is unreachable). Wrapping an
// error with errPermanentDelivery causes processDelivery to skip the retry
// schedule and send the message straight to the DLQ.
type permanentDeliveryError struct {
	err error
}

func (e *permanentDeliveryError) Error() string { return e.err.Error() }
func (e *permanentDeliveryError) Unwrap() error { return e.err }

func errPermanentDelivery(err error) error { return &permanentDeliveryError{err: err} }

func isPermanentDeliveryError(err error) bool {
	var p *permanentDeliveryError
	return errors.As(err, &p)
}

// callbackPayload is the JSON body sent to the callback URL, matching Arcade's CallbackMessage.
type callbackPayload struct {
	Type         string   `json:"type"`
	TxID         string   `json:"txid,omitempty"`
	TxIDs        []string `json:"txids,omitempty"`
	BlockHash    string   `json:"blockHash,omitempty"`
	SubtreeIndex int      `json:"subtreeIndex,omitempty"`
	Stump        string   `json:"stump,omitempty"`
}

// DeliveryService consumes callback messages from the callback Kafka topic
// and delivers them via HTTP POST.
//
// Durability contract (F-021): handleMessage processes each consumed message
// synchronously and returns nil ONLY after the message has reached a durable
// terminal state — i.e. one of:
//
//   - successfully delivered + dedup recorded, OR
//   - republished to the callback topic for a future retry (retry budget left), OR
//   - published to the DLQ (retries exhausted or permanent failure).
//
// Returning a non-nil error from handleMessage skips MarkMessage in the
// consumer-group loop, so the Kafka offset stays put and the message will be
// re-delivered when the consumer session is re-established. This guarantees
// that a process crash, restart, or forced shutdown after the offset is
// (or would have been) marked cannot permanently lose a callback. The
// previous design parked retries in time.AfterFunc timers and dispatched to
// an in-process worker pool, both of which were lost on crash — the dedup
// store could not save us because no upstream component republishes callback
// messages on restart.
//
// STUMP/BLOCK_PROCESSED ordering: callback URL is the partition key, so all
// messages for a given (block, callbackURL) land on the same partition and
// are consumed sequentially. With synchronous handling, prior STUMP
// deliveries complete before BLOCK_PROCESSED is processed without any extra
// gating primitive — the previous in-memory stumpGate is no longer needed.
type DeliveryService struct {
	service.BaseService

	cfg           *config.Config
	consumer      *kafka.Consumer
	dlqProducer   *kafka.Producer
	retryProducer *kafka.Producer
	httpClient    *http.Client
	dedupStore    CallbackDeduper
	stumpStore    store.StumpStore

	messagesProcessed atomic.Int64
	messagesRetried   atomic.Int64
	messagesFailed    atomic.Int64
	messagesDedupe    atomic.Int64
}

// NewDeliveryService creates a new callback DeliveryService. stumpStore is
// required whenever STUMP-type messages are delivered — it is the claim-check
// store holding the STUMP bytes referenced by CallbackTopicMessage.StumpRef.
func NewDeliveryService(cfg *config.Config, dedupStore CallbackDeduper, stumpStore store.StumpStore) *DeliveryService {
	return &DeliveryService{
		cfg:        cfg,
		dedupStore: dedupStore,
		stumpStore: stumpStore,
	}
}

// Init initializes the delivery service, setting up the Kafka consumer, producers, and HTTP client.
func (d *DeliveryService) Init(_ interface{}) error {
	d.InitBase("callback-delivery")

	if d.cfg.Callback.AllowPrivateIPs {
		d.Logger.Warn(
			"SSRF guard relaxed: callback delivery will dial private/loopback/link-local addresses",
			"setting", "callback.allowPrivateIPs",
		)
	}
	d.httpClient = newDeliveryHTTPClient(d.cfg.Callback)

	// Create producer for publishing permanently failed messages to the DLQ topic.
	dlqProducer, err := kafka.NewProducer(
		d.cfg.Kafka.Brokers,
		d.cfg.Kafka.CallbackDLQTopic,
		d.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback DLQ producer: %w", err)
	}
	d.dlqProducer = dlqProducer

	// Create producer for republishing retry-eligible messages back onto the
	// callback topic. Retries flow through Kafka rather than in-process timers
	// so a crash between consume and re-deliver doesn't lose the message.
	retryProducer, err := kafka.NewProducer(
		d.cfg.Kafka.Brokers,
		d.cfg.Kafka.CallbackTopic,
		d.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback retry producer: %w", err)
	}
	d.retryProducer = retryProducer

	// Create consumer for the callback topic.
	consumer, err := kafka.NewConsumer(
		d.cfg.Kafka.Brokers,
		d.cfg.Kafka.ConsumerGroup+"-callback",
		[]string{d.cfg.Kafka.CallbackTopic},
		d.handleMessage,
		d.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback consumer: %w", err)
	}
	d.consumer = consumer

	d.Logger.Info("callback delivery service initialized",
		"callbackTopic", d.cfg.Kafka.CallbackTopic,
		"callbackDlqTopic", d.cfg.Kafka.CallbackDLQTopic,
		"maxRetries", d.cfg.Callback.MaxRetries,
		"backoffBaseSec", d.cfg.Callback.BackoffBaseSec,
		"timeoutSec", d.cfg.Callback.TimeoutSec,
		"futureRetryWaitCap", futureRetryWaitCap,
		"maxConnsPerHost", maxConnsPerHostOrDefault(d.cfg.Callback.MaxConnsPerHost),
		"maxIdleConnsPerHost", maxIdleConnsPerHostOrDefault(d.cfg.Callback.MaxIdleConnsPerHost),
		"allowPrivateIPs", d.cfg.Callback.AllowPrivateIPs,
	)

	return nil
}

// Start begins consuming callback messages from Kafka.
func (d *DeliveryService) Start(ctx context.Context) error {
	d.Logger.Info("starting callback delivery service")

	// Observability: periodic INFO heartbeat so prod operators can see
	// throughput without enabling DEBUG (successful deliveries log at DEBUG).
	go d.heartbeat(d.Context())

	if err := d.consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start callback consumer: %w", err)
	}

	d.SetStarted(true)
	d.Logger.Info("callback delivery service started")
	return nil
}

// Stop gracefully shuts down the delivery service.
func (d *DeliveryService) Stop() error {
	d.Logger.Info("stopping callback delivery service")

	var firstErr error

	if d.consumer != nil {
		if err := d.consumer.Stop(); err != nil {
			d.Logger.Error("failed to stop consumer", "error", err)
			firstErr = err
		}
	}

	if d.retryProducer != nil {
		if err := d.retryProducer.Close(); err != nil {
			d.Logger.Error("failed to close retry producer", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if d.dlqProducer != nil {
		if err := d.dlqProducer.Close(); err != nil {
			d.Logger.Error("failed to close DLQ producer", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	d.SetStarted(false)
	d.Cancel()
	d.Logger.Info("callback delivery service stopped",
		"messagesProcessed", d.messagesProcessed.Load(),
		"messagesRetried", d.messagesRetried.Load(),
		"messagesFailed", d.messagesFailed.Load(),
	)
	return firstErr
}

// Health returns the current health status of the delivery service.
func (d *DeliveryService) Health() service.HealthStatus {
	status := "healthy"
	if !d.IsStarted() {
		status = "unhealthy"
	}

	return service.HealthStatus{
		Name:   d.Name,
		Status: status,
		Details: map[string]string{
			"messagesProcessed": fmt.Sprintf("%d", d.messagesProcessed.Load()),
			"messagesRetried":   fmt.Sprintf("%d", d.messagesRetried.Load()),
			"messagesFailed":    fmt.Sprintf("%d", d.messagesFailed.Load()),
		},
	}
}

// handleMessage is the durable entry-point for a single Kafka message. It
// returns nil ONLY after the message has reached a terminal durable state
// (delivered + dedup recorded, OR republished to retry topic, OR routed to
// DLQ). Returning non-nil leaves the Kafka offset unmarked so the message is
// re-consumed on the next session — which is what we want when the durable
// side-effect itself fails.
//
// Same-partition serialization (callback URL is the partition key) means
// STUMP messages for a given (block, callbackURL) are processed before
// BLOCK_PROCESSED for the same key, without any in-process gating.
func (d *DeliveryService) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	cbMsg, err := kafka.DecodeCallbackTopicMessage(msg.Value)
	if err != nil {
		// A poison-pill message that cannot be decoded should not block the
		// partition forever. Log and ack so the consumer can advance — the
		// raw bytes are still inspectable via Kafka's retention.
		d.Logger.Error("failed to decode callback message, skipping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		return nil
	}

	// Not-yet-due retry: wait briefly in-process if the remaining delay is
	// small, otherwise republish back to the topic with the same NextRetryAt
	// so we don't pin a partition for the full backoff window.
	if !cbMsg.NextRetryAt.IsZero() {
		remaining := time.Until(cbMsg.NextRetryAt)
		if remaining > 0 {
			if remaining <= futureRetryWaitCap {
				select {
				case <-time.After(remaining):
				case <-ctx.Done():
					// Session is going away; leave the offset uncommitted so
					// the message is re-delivered on the next session.
					return ctx.Err()
				}
			} else {
				// Sleep the cap to slow the consume/republish cycle to no
				// faster than once per futureRetryWaitCap, then republish.
				select {
				case <-time.After(futureRetryWaitCap):
				case <-ctx.Done():
					return ctx.Err()
				}
				return d.republishForRetry(cbMsg, "future-dated retry not yet due")
			}
		}
	}

	return d.processDelivery(ctx, cbMsg)
}

// processDelivery runs the dedup check, HTTP delivery, dedup record, and
// retry/DLQ logic for a single message inline. It returns nil only after the
// message reaches a durable terminal state; a non-nil return means the
// Kafka offset must NOT be marked so the message is re-consumed.
func (d *DeliveryService) processDelivery(ctx context.Context, cbMsg *kafka.CallbackTopicMessage) error {
	d.Logger.Debug("processing callback message",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		"subtreeIndex", cbMsg.SubtreeIndex,
	)

	// Check callback dedup — skip if already delivered.
	if d.dedupStore != nil {
		dedupKey := dedupKeyForMessage(cbMsg)
		if dedupKey != "" {
			exists, err := d.dedupStore.Exists(dedupKey, cbMsg.CallbackURL, string(cbMsg.Type))
			if err != nil {
				// Previous behavior was "proceed with delivery" — but if a
				// prior attempt had succeeded and Aerospike is briefly
				// unreadable, we'd deliver a duplicate BLOCK_PROCESSED. Safer
				// to fall through to the retry path so the next attempt
				// re-checks dedup once the store recovers.
				d.Logger.Error("dedup check failed, scheduling retry", "error", err, "dedupKey", dedupKey, "callbackUrl", cbMsg.CallbackURL)
				return d.scheduleRetryOrDLQ(cbMsg, fmt.Errorf("dedup check: %w", err))
			}
			if exists {
				d.Logger.Debug("skipping duplicate callback delivery",
					"dedupKey", dedupKey,
					"callbackUrl", cbMsg.CallbackURL,
					"type", cbMsg.Type,
				)
				d.messagesDedupe.Add(1)
				return nil
			}
		}
	}

	// Attempt HTTP POST delivery.
	deliverErr := d.deliverCallback(ctx, cbMsg)
	if deliverErr == nil {
		// Record successful delivery for dedup. Delivery already succeeded —
		// the offset will advance regardless. Log dedup-store outages at
		// ERROR so they're visible in prod without DEBUG.
		if d.dedupStore != nil {
			dedupKey := dedupKeyForMessage(cbMsg)
			if dedupKey != "" {
				ttl := time.Duration(d.cfg.Callback.DedupTTLSec) * time.Second
				if recErr := d.dedupStore.Record(dedupKey, cbMsg.CallbackURL, string(cbMsg.Type), ttl); recErr != nil {
					d.Logger.Error("failed to record callback dedup", "error", recErr, "dedupKey", dedupKey, "callbackUrl", cbMsg.CallbackURL)
				}
			}
		}
		d.messagesProcessed.Add(1)
		d.Logger.Debug("callback delivered successfully",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"type", cbMsg.Type,
			"subtreeIndex", cbMsg.SubtreeIndex,
		)
		return nil
	}

	d.Logger.Warn("callback delivery failed",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		"subtreeIndex", cbMsg.SubtreeIndex,
		"error", deliverErr,
	)
	return d.scheduleRetryOrDLQ(cbMsg, deliverErr)
}

// scheduleRetryOrDLQ decides whether a failed message should be republished
// for retry or routed to the DLQ, then performs the durable side-effect. It
// returns nil iff the durable side-effect succeeded; otherwise the caller
// must propagate the error up so the Kafka offset is not committed.
func (d *DeliveryService) scheduleRetryOrDLQ(cbMsg *kafka.CallbackTopicMessage, cause error) error {
	// Permanent failures (e.g. STUMP blob expired) skip the retry budget and
	// go straight to the DLQ — retrying cannot recover them.
	if isPermanentDeliveryError(cause) {
		d.Logger.Error("callback permanently failed, publishing to DLQ",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"type", cbMsg.Type,
			"retryCount", cbMsg.RetryCount,
			"subtreeIndex", cbMsg.SubtreeIndex,
			"reason", "permanent",
			"cause", cause,
		)
		return d.publishToDLQDurably(cbMsg)
	}

	// Retry budget exhausted: route to DLQ.
	if cbMsg.RetryCount >= d.cfg.Callback.MaxRetries {
		d.Logger.Error("callback retries exhausted, publishing to DLQ",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"type", cbMsg.Type,
			"retryCount", cbMsg.RetryCount,
			"subtreeIndex", cbMsg.SubtreeIndex,
			"cause", cause,
		)
		return d.publishToDLQDurably(cbMsg)
	}

	// Bump retry count + compute next NextRetryAt using linear backoff, then
	// republish back to the callback topic. The republish is the durable
	// side-effect: until it succeeds, we must not ack the source message.
	cbMsg.RetryCount++
	backoffSec := d.cfg.Callback.BackoffBaseSec * cbMsg.RetryCount
	cbMsg.NextRetryAt = time.Now().Add(time.Duration(backoffSec) * time.Second)

	d.Logger.Info("scheduling callback retry via Kafka republish",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"retryCount", cbMsg.RetryCount,
		"nextRetryAt", cbMsg.NextRetryAt,
		"backoffSec", backoffSec,
		"subtreeIndex", cbMsg.SubtreeIndex,
		"cause", cause,
	)

	return d.republishForRetry(cbMsg, "retry after delivery failure")
}

// republishForRetry encodes cbMsg and publishes it back to the callback
// topic via the retry producer. The partition key is the callback URL so the
// retry lands on the same partition as the original — preserving STUMP →
// BLOCK_PROCESSED ordering for the same (block, callbackURL).
//
// Returning nil means the publish was acknowledged by Kafka and the source
// message can now be safely ack'd. A non-nil return means the publish failed
// and the caller must NOT mark the offset.
func (d *DeliveryService) republishForRetry(cbMsg *kafka.CallbackTopicMessage, reason string) error {
	data, err := cbMsg.Encode()
	if err != nil {
		return fmt.Errorf("encode callback message for retry republish (%s): %w", reason, err)
	}
	if err := d.retryProducer.PublishWithHashKey(cbMsg.CallbackURL, data); err != nil {
		d.Logger.Error("retry republish failed, leaving Kafka offset uncommitted",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"retryCount", cbMsg.RetryCount,
			"reason", reason,
			"error", err,
		)
		return fmt.Errorf("retry republish (%s): %w", reason, err)
	}
	d.messagesRetried.Add(1)
	return nil
}

// publishToDLQDurably publishes a permanently failed message to the DLQ topic
// and returns nil only on success. It retries the publish a few times before
// surfacing the failure so the Kafka offset is not committed when the DLQ
// itself is unreachable — that way the message is reconsidered on the next
// session instead of being silently dropped.
func (d *DeliveryService) publishToDLQDurably(cbMsg *kafka.CallbackTopicMessage) error {
	delays := []time.Duration{0, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	var lastErr error
	for i, delay := range delays {
		if delay > 0 {
			time.Sleep(delay)
		}
		if err := d.publishToDLQ(cbMsg); err != nil {
			lastErr = err
			d.Logger.Warn("DLQ publish attempt failed",
				"attempt", i+1,
				"callbackUrl", cbMsg.CallbackURL,
				"txid", cbMsg.TxID,
				"error", err,
			)
			continue
		}
		d.messagesFailed.Add(1)
		return nil
	}
	d.Logger.Error("DLQ publish exhausted all retries, leaving Kafka offset uncommitted",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		"error", lastErr,
	)
	return fmt.Errorf("DLQ publish: %w", lastErr)
}

// heartbeat emits an INFO-level throughput line every 30 seconds until ctx
// is canceled. Lets operators see "service is alive and doing work" without
// turning on DEBUG for per-message success logs.
func (d *DeliveryService) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.Logger.Info("callback delivery heartbeat",
				"messagesProcessed", d.messagesProcessed.Load(),
				"messagesRetried", d.messagesRetried.Load(),
				"messagesFailed", d.messagesFailed.Load(),
				"messagesDedupe", d.messagesDedupe.Load(),
			)
		}
	}
}

// deliverCallback makes an HTTP POST to the callback URL with the CallbackMessage payload.
func (d *DeliveryService) deliverCallback(ctx context.Context, msg *kafka.CallbackTopicMessage) error {
	payload := callbackPayload{
		Type:         string(msg.Type),
		TxID:         msg.TxID,
		TxIDs:        msg.TxIDs,
		BlockHash:    msg.BlockHash,
		SubtreeIndex: msg.SubtreeIndex,
	}

	// STUMP bytes are claim-checked: CallbackTopicMessage carries only a ref,
	// fetch the blob and hex-encode for the HTTP payload.
	if msg.Type == kafka.CallbackStump && msg.StumpRef != "" {
		if d.stumpStore == nil {
			return errPermanentDelivery(fmt.Errorf("stump store not configured for STUMP delivery"))
		}
		stumpBytes, err := d.stumpStore.Get(msg.StumpRef)
		if err != nil {
			if errors.Is(err, store.ErrStumpNotFound) {
				// Blob expired (DAH) or never written — no amount of retry will
				// produce it. Fail permanently so processDelivery routes to DLQ.
				return errPermanentDelivery(fmt.Errorf("stump blob missing for ref %s: %w", msg.StumpRef, err))
			}
			return fmt.Errorf("fetching stump ref %s: %w", msg.StumpRef, err)
		}
		payload.Stump = hex.EncodeToString(stumpBytes)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal callback payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, msg.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Add idempotency key header for receiver-side dedup.
	idempotencyKey := buildIdempotencyKey(msg)
	if idempotencyKey != "" {
		req.Header.Set("X-Idempotency-Key", idempotencyKey)
	}

	start := time.Now()
	resp, err := d.httpClient.Do(req)
	if err != nil {
		d.Logger.Debug("callback http transport error",
			"callbackUrl", msg.CallbackURL,
			"durationMs", time.Since(start).Milliseconds(),
			"idempotencyKey", idempotencyKey,
			"type", msg.Type,
			"txid", msg.TxID,
			"subtreeIndex", msg.SubtreeIndex,
			"error", err,
		)
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodyBytes, truncated := readBodyCapped(resp.Body, 4*1024)
	d.Logger.Debug("callback http error response",
		"callbackUrl", msg.CallbackURL,
		"status", resp.StatusCode,
		"statusText", resp.Status,
		"durationMs", time.Since(start).Milliseconds(),
		"responseBody", string(bodyBytes),
		"bodyTruncated", truncated,
		"contentType", resp.Header.Get("Content-Type"),
		"contentLength", resp.Header.Get("Content-Length"),
		"retryAfter", resp.Header.Get("Retry-After"),
		"xRequestId", resp.Header.Get("X-Request-Id"),
		"idempotencyKey", idempotencyKey,
		"type", msg.Type,
		"txid", msg.TxID,
		"subtreeIndex", msg.SubtreeIndex,
	)

	snippet := bodyBytes
	if len(snippet) > 256 {
		snippet = snippet[:256]
	}
	if len(snippet) == 0 {
		return fmt.Errorf("callback returned non-2xx status: %d", resp.StatusCode)
	}
	return fmt.Errorf("callback returned non-2xx status: %d: %s", resp.StatusCode, string(snippet))
}

// readBodyCapped reads up to max bytes from r and reports whether the
// underlying body had more data than was read. Used so a misbehaving receiver
// returning a huge HTML error page cannot balloon log size or memory.
func readBodyCapped(r io.Reader, max int64) ([]byte, bool) {
	buf, err := io.ReadAll(io.LimitReader(r, max))
	if err != nil {
		return buf, false
	}
	// Probe one more byte to see whether the body was larger than the cap.
	var probe [1]byte
	n, _ := r.Read(probe[:])
	return buf, n > 0
}

// buildIdempotencyKey creates a unique key for a callback delivery.
func buildIdempotencyKey(msg *kafka.CallbackTopicMessage) string {
	if msg.Type == kafka.CallbackBlockProcessed {
		return msg.BlockHash + ":" + string(msg.Type)
	}
	if msg.Type == kafka.CallbackStump {
		return msg.BlockHash + ":" + strconv.Itoa(msg.SubtreeIndex) + ":STUMP"
	}
	if len(msg.TxIDs) > 0 {
		return hashTxIDs(msg.TxIDs) + ":" + string(msg.Type)
	}
	if msg.TxID != "" {
		return msg.TxID + ":" + string(msg.Type)
	}
	return ""
}

// dedupKeyForMessage returns the dedup key for a message.
func dedupKeyForMessage(msg *kafka.CallbackTopicMessage) string {
	if msg.Type == kafka.CallbackBlockProcessed {
		return msg.BlockHash
	}
	if msg.Type == kafka.CallbackStump {
		return msg.BlockHash + ":" + strconv.Itoa(msg.SubtreeIndex)
	}
	if len(msg.TxIDs) > 0 {
		return hashTxIDs(msg.TxIDs)
	}
	return msg.TxID
}

// hashTxIDs produces a stable hash of a txid set for use as dedup/idempotency keys.
func hashTxIDs(txids []string) string {
	sorted := make([]string, len(txids))
	copy(sorted, txids)
	sort.Strings(sorted)
	h := sha256.Sum256([]byte(strings.Join(sorted, ",")))
	return hex.EncodeToString(h[:8])
}

// maxConnsPerHostOrDefault returns v or 32 when v <= 0.
func maxConnsPerHostOrDefault(v int) int {
	if v <= 0 {
		return 32
	}
	return v
}

// maxIdleConnsPerHostOrDefault returns v or 16 when v <= 0.
func maxIdleConnsPerHostOrDefault(v int) int {
	if v <= 0 {
		return 16
	}
	return v
}

// newDeliveryHTTPClient builds the HTTP client used to deliver callbacks.
// It applies an SSRF-aware Dialer.Control hook that rejects connections
// to private/loopback/link-local IPs unless cfg.AllowPrivateIPs is set.
// The hook fires after DNS resolution so DNS rebinding cannot bypass it
// (Go's resolver passes the resolved IP to Control as part of address).
func newDeliveryHTTPClient(cfg config.CallbackConfig) *http.Client {
	allowPrivate := cfg.AllowPrivateIPs
	transport := &http.Transport{
		MaxIdleConns:        maxConnsPerHostOrDefault(cfg.MaxConnsPerHost) * 10,
		MaxIdleConnsPerHost: maxIdleConnsPerHostOrDefault(cfg.MaxIdleConnsPerHost),
		MaxConnsPerHost:     maxConnsPerHostOrDefault(cfg.MaxConnsPerHost),
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // small JSON payloads — compression adds latency
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Control: func(network, address string, _ syscall.RawConn) error {
				// Only filter inet sockets — Control is also called for
				// unix sockets in pathological cases; ignore those.
				if network != "tcp" && network != "tcp4" && network != "tcp6" {
					return nil
				}
				return ssrfguard.CheckDialAddress(address, allowPrivate)
			},
		}).DialContext,
	}
	return &http.Client{
		Timeout:   time.Duration(cfg.TimeoutSec) * time.Second,
		Transport: transport,
	}
}

// publishToDLQ publishes a permanently failed message to the dead-letter queue topic.
func (d *DeliveryService) publishToDLQ(msg *kafka.CallbackTopicMessage) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode callback message for DLQ: %w", err)
	}

	if err := d.dlqProducer.PublishWithHashKey(msg.CallbackURL, data); err != nil {
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	return nil
}
