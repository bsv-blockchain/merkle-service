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
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// CallbackDeduper abstracts callback deduplication for testability.
type CallbackDeduper interface {
	Exists(txid, callbackURL, statusType string) (bool, error)
	Record(txid, callbackURL, statusType string, ttl time.Duration) error
}

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

// stumpGate coordinates STUMP/BLOCK_PROCESSED delivery ordering.
// It ensures all STUMP HTTP deliveries for a (blockHash, callbackURL) complete
// before BLOCK_PROCESSED is delivered to that callbackURL.
//
// Safety: for a given callbackURL, all messages are hash-partitioned to the same
// Kafka partition, so handleMessage sees STUMPs before BLOCK_PROCESSED. Add()
// is called in handleMessage (sequential per partition) and Done()/Wait() are
// called in processDelivery (concurrent workers).
type stumpGate struct {
	mu    sync.Mutex
	gates map[string]*sync.WaitGroup
}

func newStumpGate() *stumpGate {
	return &stumpGate{gates: make(map[string]*sync.WaitGroup)}
}

func stumpGateKey(blockHash, callbackURL string) string {
	return blockHash + "|" + callbackURL
}

// Add registers a pending STUMP delivery. Called from handleMessage (sequential per partition).
func (g *stumpGate) Add(blockHash, callbackURL string) {
	key := stumpGateKey(blockHash, callbackURL)
	g.mu.Lock()
	defer g.mu.Unlock()
	wg, ok := g.gates[key]
	if !ok {
		wg = &sync.WaitGroup{}
		g.gates[key] = wg
	}
	wg.Add(1)
}

// Done signals that a STUMP delivery has completed. Called from processDelivery.
func (g *stumpGate) Done(blockHash, callbackURL string) {
	key := stumpGateKey(blockHash, callbackURL)
	g.mu.Lock()
	wg, ok := g.gates[key]
	g.mu.Unlock()
	if ok {
		wg.Done()
	}
}

// Wait blocks until all registered STUMPs for the (blockHash, callbackURL) are done.
// Called from processDelivery for BLOCK_PROCESSED messages.
func (g *stumpGate) Wait(blockHash, callbackURL string) {
	key := stumpGateKey(blockHash, callbackURL)
	g.mu.Lock()
	wg, ok := g.gates[key]
	g.mu.Unlock()
	if ok {
		wg.Wait()
		g.mu.Lock()
		delete(g.gates, key)
		g.mu.Unlock()
	}
}

// DeliveryService consumes callback messages from the callback Kafka topic
// and delivers them via HTTP POST, with linear backoff retry logic.
type DeliveryService struct {
	service.BaseService

	cfg         *config.Config
	consumer    *kafka.Consumer
	dlqProducer *kafka.Producer
	httpClient  *http.Client
	dedupStore  CallbackDeduper
	stumpStore  store.StumpStore
	stumpGate   *stumpGate

	// Worker pool for concurrent delivery.
	workCh   chan *kafka.CallbackTopicMessage
	workerWg sync.WaitGroup

	// shuttingDown guards the retry-scheduler path: once Stop begins, in-flight
	// time.AfterFunc callbacks must not attempt to send on workCh. Tracked with
	// retryTimerWg so Stop can wait for pending timer callbacks to finish.
	shuttingDown atomic.Bool
	retryTimerWg sync.WaitGroup

	messagesProcessed atomic.Int64
	messagesRetried   atomic.Int64
	messagesFailed    atomic.Int64
	messagesDedupe    atomic.Int64
	messagesParked    atomic.Int64 // retries currently parked in time.AfterFunc timers
}

// NewDeliveryService creates a new callback DeliveryService. stumpStore is
// required whenever STUMP-type messages are delivered — it is the claim-check
// store holding the STUMP bytes referenced by CallbackTopicMessage.StumpRef.
func NewDeliveryService(cfg *config.Config, dedupStore CallbackDeduper, stumpStore store.StumpStore) *DeliveryService {
	return &DeliveryService{
		cfg:        cfg,
		dedupStore: dedupStore,
		stumpStore: stumpStore,
		stumpGate:  newStumpGate(),
	}
}

// Init initializes the delivery service, setting up the Kafka consumer, producers, and HTTP client.
func (d *DeliveryService) Init(_ interface{}) error {
	d.InitBase("callback-delivery")

	// Set up the HTTP client with tuned transport for high-throughput delivery.
	maxConnsPerHost := d.cfg.Callback.MaxConnsPerHost
	if maxConnsPerHost <= 0 {
		maxConnsPerHost = 32
	}
	maxIdleConnsPerHost := d.cfg.Callback.MaxIdleConnsPerHost
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = 16
	}

	transport := &http.Transport{
		MaxIdleConns:        maxConnsPerHost * 10, // global pool
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // small JSON payloads — compression adds latency
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	d.httpClient = &http.Client{
		Timeout:   time.Duration(d.cfg.Callback.TimeoutSec) * time.Second,
		Transport: transport,
	}

	// Create producer for publishing permanently failed messages to the DLQ topic.
	// Retries are parked in-process (see scheduleRetry) rather than republished
	// to the callback topic, so no retry-producer is needed.
	dlqProducer, err := kafka.NewProducer(
		d.cfg.Kafka.Brokers,
		d.cfg.Kafka.CallbackDLQTopic,
		d.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback DLQ producer: %w", err)
	}
	d.dlqProducer = dlqProducer

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

	// Initialize the work channel for the worker pool.
	workers := d.cfg.Callback.DeliveryWorkers
	if workers <= 0 {
		workers = 64
	}
	d.workCh = make(chan *kafka.CallbackTopicMessage, workers*2)

	d.Logger.Info("callback delivery service initialized",
		"callbackTopic", d.cfg.Kafka.CallbackTopic,
		"callbackDlqTopic", d.cfg.Kafka.CallbackDLQTopic,
		"maxRetries", d.cfg.Callback.MaxRetries,
		"backoffBaseSec", d.cfg.Callback.BackoffBaseSec,
		"timeoutSec", d.cfg.Callback.TimeoutSec,
		"deliveryWorkers", workers,
		"maxConnsPerHost", maxConnsPerHost,
		"maxIdleConnsPerHost", maxIdleConnsPerHost,
	)

	return nil
}

// Start begins consuming callback messages from Kafka and launches delivery workers.
func (d *DeliveryService) Start(ctx context.Context) error {
	d.Logger.Info("starting callback delivery service")

	// Launch delivery workers.
	workers := d.cfg.Callback.DeliveryWorkers
	if workers <= 0 {
		workers = 64
	}
	d.workerWg.Add(workers)
	for i := 0; i < workers; i++ {
		go d.deliveryWorker(d.Context())
	}

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

	// Flip the shutdown flag before closing workCh so in-flight retry timers
	// drop their messages instead of sending to a closed channel.
	d.shuttingDown.Store(true)
	d.retryTimerWg.Wait()

	// Close work channel to signal workers to drain and exit.
	if d.workCh != nil {
		close(d.workCh)
		d.workerWg.Wait()
	}

	if d.consumer != nil {
		if err := d.consumer.Stop(); err != nil {
			d.Logger.Error("failed to stop consumer", "error", err)
			firstErr = err
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

// handleMessage decodes a Kafka message and dispatches it to the worker pool.
func (d *DeliveryService) handleMessage(_ context.Context, msg *sarama.ConsumerMessage) error {
	cbMsg, err := kafka.DecodeCallbackTopicMessage(msg.Value)
	if err != nil {
		d.Logger.Error("failed to decode callback message",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		return fmt.Errorf("failed to decode callback message: %w", err)
	}

	// Park not-yet-due retries in-process instead of looping them through Kafka.
	// Before this change, a message with NextRetryAt in the future was
	// immediately republished to the callback topic, which caused the consumer
	// to spin at partition speed republishing the same retry until the delay
	// elapsed. The Kafka offset still advances (MarkMessage runs after
	// handleMessage returns nil) so there's no durability regression —
	// retries were never persisted across restarts anyway, dedup prevents
	// double-delivery on re-drive from upstream.
	if !cbMsg.NextRetryAt.IsZero() && time.Now().Before(cbMsg.NextRetryAt) {
		delay := time.Until(cbMsg.NextRetryAt)
		d.Logger.Debug("parking retry in-process",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"nextRetryAt", cbMsg.NextRetryAt,
			"delay", delay,
		)
		d.scheduleRetry(cbMsg, delay)
		return nil
	}

	// Register first-attempt STUMP deliveries with the gate so BLOCK_PROCESSED
	// can wait for them. This runs sequentially per partition, guaranteeing all
	// STUMPs for a callbackURL are registered before BLOCK_PROCESSED is dispatched.
	if cbMsg.Type == kafka.CallbackStump && cbMsg.RetryCount == 0 {
		d.stumpGate.Add(cbMsg.BlockHash, cbMsg.CallbackURL)
	}

	// Dispatch to worker pool (blocking send provides backpressure).
	d.workCh <- cbMsg
	return nil
}

// deliveryWorker is a goroutine that processes delivery jobs from the work channel.
func (d *DeliveryService) deliveryWorker(ctx context.Context) {
	defer d.workerWg.Done()
	for msg := range d.workCh {
		d.processDelivery(ctx, msg)
	}
}

// processDelivery handles dedup check, HTTP delivery, dedup record, and retry/DLQ logic for a single message.
func (d *DeliveryService) processDelivery(ctx context.Context, cbMsg *kafka.CallbackTopicMessage) {
	// Signal STUMP completion when this function exits (success, dedup skip, or failure).
	if cbMsg.Type == kafka.CallbackStump && cbMsg.RetryCount == 0 {
		defer d.stumpGate.Done(cbMsg.BlockHash, cbMsg.CallbackURL)
	}

	// Wait for all STUMP deliveries to complete before delivering BLOCK_PROCESSED.
	if cbMsg.Type == kafka.CallbackBlockProcessed {
		d.stumpGate.Wait(cbMsg.BlockHash, cbMsg.CallbackURL)
	}

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
				// Previous behaviour was "proceed with delivery" — but if a
				// prior attempt had succeeded and Aerospike is briefly
				// unreadable, we'd deliver a duplicate BLOCK_PROCESSED. Safer
				// to fall through to the retry path so the next attempt
				// re-checks dedup once the store recovers.
				d.Logger.Error("dedup check failed, scheduling retry", "error", err, "dedupKey", dedupKey, "callbackUrl", cbMsg.CallbackURL)
				d.scheduleRetryAfterFailure(cbMsg, fmt.Errorf("dedup check: %w", err))
				return
			}
			if exists {
				d.Logger.Debug("skipping duplicate callback delivery",
					"dedupKey", dedupKey,
					"callbackUrl", cbMsg.CallbackURL,
					"type", cbMsg.Type,
				)
				d.messagesDedupe.Add(1)
				return
			}
		}
	}

	// Attempt HTTP POST delivery.
	err := d.deliverCallback(ctx, cbMsg)
	if err == nil {
		// Record successful delivery for dedup. Delivery already succeeded —
		// the offset will advance regardless. Raising to ERROR (vs the
		// previous Warn) so dedup-store outages are visible in prod logs
		// without requiring DEBUG.
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
		return
	}

	d.Logger.Warn("callback delivery failed",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		"subtreeIndex", cbMsg.SubtreeIndex,
		"error", err,
	)

	// Permanent failures (e.g. STUMP blob expired) skip the retry schedule
	// and go straight to the DLQ — retrying cannot recover them.
	if isPermanentDeliveryError(err) {
		d.Logger.Error("callback permanently failed, publishing to DLQ",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"type", cbMsg.Type,
			"retryCount", cbMsg.RetryCount,
			"subtreeIndex", cbMsg.SubtreeIndex,
			"reason", "permanent",
		)
		d.messagesFailed.Add(1)
		d.publishToDLQWithRetry(cbMsg)
		return
	}

	// Check if we've exhausted retries.
	if cbMsg.RetryCount >= d.cfg.Callback.MaxRetries {
		d.Logger.Error("callback permanently failed, publishing to DLQ",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"type", cbMsg.Type,
			"retryCount", cbMsg.RetryCount,
			"subtreeIndex", cbMsg.SubtreeIndex,
		)
		d.messagesFailed.Add(1)
		d.publishToDLQWithRetry(cbMsg)
		return
	}

	d.scheduleRetryAfterFailure(cbMsg, err)
}

// scheduleRetryAfterFailure bumps RetryCount, computes the next NextRetryAt
// using linear backoff, and parks the message in-process until the delay
// elapses. Shared by the HTTP-delivery-failed and dedup-check-failed paths.
func (d *DeliveryService) scheduleRetryAfterFailure(cbMsg *kafka.CallbackTopicMessage, cause error) {
	cbMsg.RetryCount++
	backoffSec := d.cfg.Callback.BackoffBaseSec * cbMsg.RetryCount
	cbMsg.NextRetryAt = time.Now().Add(time.Duration(backoffSec) * time.Second)

	d.Logger.Info("scheduling callback retry",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"retryCount", cbMsg.RetryCount,
		"nextRetryAt", cbMsg.NextRetryAt,
		"backoffSec", backoffSec,
		"subtreeIndex", cbMsg.SubtreeIndex,
		"cause", cause,
	)

	d.messagesRetried.Add(1)
	d.scheduleRetry(cbMsg, time.Duration(backoffSec)*time.Second)
}

// scheduleRetry parks the message in a time.AfterFunc for the given delay,
// then re-dispatches it to the worker pool. Respects shutdown: if Stop has
// fired, the timer callback drops the message instead of sending on a
// potentially-closing channel.
func (d *DeliveryService) scheduleRetry(cbMsg *kafka.CallbackTopicMessage, delay time.Duration) {
	if d.shuttingDown.Load() {
		d.Logger.Warn("shutdown in progress, dropping parked retry",
			"callbackUrl", cbMsg.CallbackURL,
			"txid", cbMsg.TxID,
			"retryCount", cbMsg.RetryCount,
		)
		return
	}

	d.retryTimerWg.Add(1)
	d.messagesParked.Add(1)
	time.AfterFunc(delay, func() {
		defer d.retryTimerWg.Done()
		defer d.messagesParked.Add(-1)
		if d.shuttingDown.Load() {
			return
		}
		// Non-blocking send protects us if the workCh has somehow been closed
		// between the shuttingDown check and the send. The recover is a
		// belt-and-braces because AfterFunc has no ctx handle of its own.
		defer func() {
			if r := recover(); r != nil {
				d.Logger.Warn("retry dispatch recovered from panic, message dropped",
					"callbackUrl", cbMsg.CallbackURL,
					"txid", cbMsg.TxID,
					"panic", r,
				)
			}
		}()
		d.workCh <- cbMsg
	})
}

// publishToDLQWithRetry attempts publishToDLQ up to 3 times with 500ms →
// 1s → 2s backoff. On final failure the message is dropped but the ERROR
// log + messagesFailed counter surface it — previously the log said
// "failed to publish to DLQ" with no follow-up and callers assumed success.
func (d *DeliveryService) publishToDLQWithRetry(cbMsg *kafka.CallbackTopicMessage) {
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
		return
	}
	d.Logger.Error("DLQ publish exhausted all retries, message lost",
		"callbackUrl", cbMsg.CallbackURL,
		"txid", cbMsg.TxID,
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		"error", lastErr,
	)
}

// heartbeat emits an INFO-level throughput line every 30 seconds until ctx
// is cancelled. Lets operators see "service is alive and doing work" without
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
				"messagesParked", d.messagesParked.Load(),
				"workChLen", len(d.workCh),
				"workChCap", cap(d.workCh),
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
	defer resp.Body.Close()

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
