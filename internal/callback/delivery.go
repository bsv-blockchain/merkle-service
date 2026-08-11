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
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/logfields"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
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

// oversizeDeliveryError marks a delivery that failed purely because the
// request body was bigger than the receiver is willing to accept — either the
// receiver answered 413 Request Entity Too Large, or our own pre-flight check
// (callback.maxBodyBytes) refused to send it.
//
// This is deliberately a DIFFERENT class from permanentDeliveryError even
// though 413 is a 4xx. Every other non-retryable 4xx (400/401/403/404/405/410
// /415/422) says "this request is wrong and always will be" — a bad URL, a bad
// token, a payload the receiver cannot parse. 413 says something categorically
// different: "this request is too big *for my current configuration*". That is
// an operational condition, curable without changing anything about the
// message, by raising the receiver's cap.
//
// The distinction is not academic. Live on dev-ovh-1 at 1000 TPS on
// 2026-08-11, one subtree's STUMP exceeded arcade's then-default 16 MiB
// `callback.max_body_bytes` after hex inflation (hex doubles the blob).
// arcade answered 413, we classified it as `permanent`, logged
//
//	"callback permanently failed, publishing to DLQ" type:"STUMP" reason:"permanent"
//
// and published it to `callback-dlq` after ZERO retry attempts. arcade's
// bump-builder then logged, forever:
//
//	"BLOCK_PROCESSED is missing expected STUMPs — deferring finalization"
//	expected_stumps:1 received_stumps:0 missing_subtree_indices:[0]
//
// It never built a BUMP for that block, so EVERY transaction in it never
// reached MINED. Because arcade's completeness gate leaves `processed_at`
// NULL, its watchdog re-drives the block via /reprocess indefinitely — so the
// block IS recoverable, but only if merkle is still willing to attempt the
// delivery. Sending it straight to the DLQ with no retries threw away the one
// window in which an operator could raise the cap and have the block heal
// itself.
//
// Routing: an oversize failure takes the normal retry ladder (5 attempts,
// linear 30s backoff ≈ 7.5 minutes of durable, Kafka-parked retries) instead
// of the DLQ, and it never trips the per-URL circuit breaker — see
// scheduleRetryOrDLQ.
type oversizeDeliveryError struct {
	err error
	// bodyBytes is the size of the body we built (and either sent or
	// refused to send). Carried on the error so the retry/DLQ decision point
	// can name it in the operator-facing log without rebuilding the payload.
	bodyBytes int
	// limitBytes is the cap that was violated: the receiver's, when we know
	// it (pre-flight), or 0 when we only learned about it from a 413.
	limitBytes int64
	// statusCode is the HTTP status the receiver returned, or 0 when the
	// request was never sent because the pre-flight check refused it.
	statusCode int
}

func (e *oversizeDeliveryError) Error() string { return e.err.Error() }
func (e *oversizeDeliveryError) Unwrap() error { return e.err }

func errOversizeDelivery(err error, bodyBytes int, limitBytes int64, statusCode int) error {
	return &oversizeDeliveryError{err: err, bodyBytes: bodyBytes, limitBytes: limitBytes, statusCode: statusCode}
}

// asOversizeDeliveryError returns the oversize error in err's chain, or nil.
func asOversizeDeliveryError(err error) *oversizeDeliveryError {
	var o *oversizeDeliveryError
	if errors.As(err, &o) {
		return o
	}
	return nil
}

// callbackPayload is the JSON body sent to the callback URL, matching Arcade's CallbackMessage.
type callbackPayload struct {
	Type         string   `json:"type"`
	TxID         string   `json:"txid,omitempty"`
	TxIDs        []string `json:"txids,omitempty"`
	BlockHash    string   `json:"blockHash,omitempty"`
	SubtreeIndex int      `json:"subtreeIndex,omitempty"`
	Stump        string   `json:"stump,omitempty"`

	// BLOCK_PROCESSED enrichment — see CallbackTopicMessage. Additive and
	// omitempty so consumers that don't read them are unaffected.
	MerkleRoot             string   `json:"merkleRoot,omitempty"`
	SubtreeCount           *int     `json:"subtreeCount,omitempty"`
	SubtreeHashes          []string `json:"subtreeHashes,omitempty"`
	CoinbaseBUMP           string   `json:"coinbaseBump,omitempty"`
	ExpectedSubtreeIndices []int    `json:"expectedSubtreeIndices,omitempty"`
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

	cfg *config.Config
	// consumers holds one consumer per source topic: always the shared
	// 'callback' topic (STUMP / BLOCK_PROCESSED), plus — when
	// Kafka.CallbackSeenTopic is configured — a second, independent consumer
	// group on the SEEN topic. Two consumers means two independent sets of
	// partition workers, so a burst of ~545 KB STUMP deliveries can no longer
	// head-of-line-block the small SEEN callbacks behind it.
	consumers   []*kafka.Consumer
	dlqProducer *kafka.Producer
	// retryProducers maps SOURCE topic -> the producer that republishes a
	// failed delivery back onto THAT SAME topic. Keying by source topic is
	// load-bearing: a single retry producer pinned to 'callback' would push
	// SEEN retries onto the blocked topic and silently reintroduce exactly the
	// head-of-line blocking this split removes. Lookups go through
	// retryProducerFor, which falls back to the 'callback' producer for an
	// unknown/empty topic.
	retryProducers map[string]*kafka.Producer
	httpClient     *http.Client
	dedupStore     CallbackDeduper
	stumpStore     store.StumpStore
	// urlRegistry trips a per-URL circuit breaker after a DLQ'd callback so dead
	// endpoints stop receiving fan-out. nil disables the breaker.
	urlRegistry store.CallbackURLRegistry

	// bodyCache memoizes the fully-marshaled HTTP body of STUMP callbacks,
	// keyed by (StumpRef, BlockHash, SubtreeIndex). The auth token travels in
	// the Authorization HEADER, so the body of one subtree's STUMP callback
	// is byte-identical for every subscriber registered for its txids —
	// without the cache the same ~quarter-MB blob was re-fetched,
	// re-hex-encoded (~half MB), and re-JSON-marshaled (~half MB) once per
	// subscriber: at 100 subscribers, 100x redundant work and two ~half-MB
	// allocations per delivery on the hot path. Entries are evicted FIFO at
	// bodyCacheMaxEntries.
	bodyMu    sync.Mutex
	bodyCache map[string][]byte
	bodyOrder []string
}

// bodyCacheMaxEntries bounds the STUMP body cache. Bodies are ~545 KB
// (~271 KB stump, hex-doubled, plus small JSON fields), so 64 entries caps
// the cache at ~35 MB while comfortably covering the distinct subtrees in
// flight during one block's delivery fan-out.
const bodyCacheMaxEntries = 64

// bodyWarnBytes is the built-body size above which a delivery logs a WARN
// naming the block, subtree and exact byte count — even when the POST then
// succeeds.
//
// 8 MiB is half of arcade's original `callback.max_body_bytes` default
// (16 MiB, `config.DefaultCallbackMaxBodyBytes`). A body at this size means
// the deployment is one subtree-growth step away from the 2026-08-11
// dev-ovh-1 incident, in which a STUMP crossed that cap and the block it
// belonged to was stranded (see oversizeDeliveryError). This is the
// early-warning signal that the receiver's cap is about to be reached.
//
// It is a constant rather than config on purpose — its job is "tell me the
// payloads are growing", which needs no tuning, while the *enforcing* knob
// (callback.maxBodyBytes) is configurable because only the operator knows the
// receiver's real cap.
//
// The warning fires per DELIVERY, not per built body: it sits after the
// bodyCache lookup, so one subtree fanned out to N subscribers logs N lines.
// Deliberate — the body is memoized but the risk is per-request, and any one
// of those N POSTs can be the one that 413s. A normal STUMP body is ~545 KB,
// so crossing 8 MiB at all already means the deployment is close to an
// outage, which is exactly when the volume is worth paying.
const bodyWarnBytes = 8 << 20

// NewDeliveryService creates a new callback DeliveryService. stumpStore is
// required whenever STUMP-type messages are delivered — it is the claim-check
// store holding the STUMP bytes referenced by CallbackTopicMessage.StumpRef.
// logger, when non-nil, overrides the default logger InitBase would
// otherwise install — this is what lets the configured LOG_LEVEL reach the
// callback-delivery service instead of silently falling back to InitBase's
// hardcoded Info logger.
func NewDeliveryService(cfg *config.Config, dedupStore CallbackDeduper, stumpStore store.StumpStore, urlRegistry store.CallbackURLRegistry, logger *slog.Logger) *DeliveryService {
	d := &DeliveryService{
		cfg:         cfg,
		dedupStore:  dedupStore,
		stumpStore:  stumpStore,
		urlRegistry: urlRegistry,
		bodyCache:   make(map[string][]byte, bodyCacheMaxEntries),
	}
	d.InitBase("callback-delivery")
	if logger != nil {
		d.Logger = logger
	}
	return d
}

// cachedBody returns the memoized body for key, or nil.
func (d *DeliveryService) cachedBody(key string) []byte {
	d.bodyMu.Lock()
	defer d.bodyMu.Unlock()
	return d.bodyCache[key]
}

// storeBody memoizes body under key with FIFO eviction. The build happens
// outside the lock (half-MB hex+marshal work must not serialize the
// per-partition delivery workers); a concurrent duplicate build is harmless —
// first writer wins.
func (d *DeliveryService) storeBody(key string, body []byte) {
	d.bodyMu.Lock()
	defer d.bodyMu.Unlock()
	if d.bodyCache == nil {
		// Lazy init: tests construct DeliveryService via struct literal.
		d.bodyCache = make(map[string][]byte, bodyCacheMaxEntries)
	}
	if _, ok := d.bodyCache[key]; !ok {
		if len(d.bodyOrder) >= bodyCacheMaxEntries {
			oldest := d.bodyOrder[0]
			d.bodyOrder = d.bodyOrder[1:]
			delete(d.bodyCache, oldest)
		}
		d.bodyCache[key] = body
		d.bodyOrder = append(d.bodyOrder, key)
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
		d.cfg.Kafka.TopicPartitions(),
		d.cfg.Kafka.TopicRetention(),
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
		d.cfg.Kafka.TopicPartitions(),
		d.cfg.Kafka.TopicRetention(),
		d.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback retry producer: %w", err)
	}
	d.retryProducers = map[string]*kafka.Producer{d.cfg.Kafka.CallbackTopic: retryProducer}

	// Create consumer for the callback topic.
	consumer, err := kafka.NewConsumer(
		d.cfg.Kafka.Brokers,
		d.cfg.Kafka.ConsumerGroup+"-callback",
		[]string{d.cfg.Kafka.CallbackTopic},
		d.handleMessage,
		nil, // 'callback' must stay at 1 partition until a cross-partition BLOCK_PROCESSED barrier exists
		d.cfg.Kafka.TopicRetention(),
		d.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback consumer: %w", err)
	}
	d.consumers = []*kafka.Consumer{consumer}

	// SEEN split: when a dedicated SEEN topic is configured, stand up a second,
	// fully independent consumer group for it. Its partition workers run in
	// parallel with the 'callback' ones, so a block's STUMP/BLOCK_PROCESSED
	// burst can no longer delay a SEEN_ON_NETWORK. Unlike 'callback', this
	// topic IS created wide (TopicPartitions carries CallbackSeenTopic) —
	// SEEN callbacks have no STUMP → BLOCK_PROCESSED barrier to preserve, and
	// arcade's status lattice refuses to let a lower status supersede a higher
	// one, so cross-partition SEEN reordering cannot regress a transaction.
	//
	// When CallbackSeenTopic is empty, SeenCallbackTopic() == CallbackTopic and
	// we skip this entirely: one consumer, one retry producer, exactly the
	// pre-split behavior.
	seenTopic := d.cfg.Kafka.SeenCallbackTopic()
	if seenTopic != d.cfg.Kafka.CallbackTopic {
		seenRetryProducer, sErr := kafka.NewProducer(
			d.cfg.Kafka.Brokers,
			seenTopic,
			d.cfg.Kafka.TopicPartitions(),
			d.cfg.Kafka.TopicRetention(),
			d.Logger,
		)
		if sErr != nil {
			return fmt.Errorf("failed to create SEEN callback retry producer: %w", sErr)
		}
		d.retryProducers[seenTopic] = seenRetryProducer

		seenConsumer, sErr := kafka.NewConsumer(
			d.cfg.Kafka.Brokers,
			d.cfg.Kafka.ConsumerGroup+"-callback-seen",
			[]string{seenTopic},
			d.handleMessage,
			d.cfg.Kafka.TopicPartitions(),
			d.cfg.Kafka.TopicRetention(),
			d.Logger,
		)
		if sErr != nil {
			// Unwind the retry producer created moments ago. Init's caller
			// treats an error as "service never started" and does not call
			// Stop(), so anything already opened here has to be closed on this
			// path or its broker connections leak for the process lifetime.
			if cErr := seenRetryProducer.Close(); cErr != nil {
				d.Logger.Warn("failed to close SEEN retry producer after consumer init failure", "error", cErr)
			}
			delete(d.retryProducers, seenTopic)
			return fmt.Errorf("failed to create SEEN callback consumer: %w", sErr)
		}
		d.consumers = append(d.consumers, seenConsumer)
	}

	d.Logger.Info(
		"callback delivery service initialized",
		"callbackTopic", d.cfg.Kafka.CallbackTopic,
		"callbackSeenTopic", seenTopic,
		"callbackSeenPartitions", d.cfg.Kafka.TopicPartitions()[d.cfg.Kafka.CallbackSeenTopic],
		"callbackSeenSplitEnabled", seenTopic != d.cfg.Kafka.CallbackTopic,
		"callbackDlqTopic", d.cfg.Kafka.CallbackDLQTopic,
		"maxRetries", d.cfg.Callback.MaxRetries,
		"backoffBaseSec", d.cfg.Callback.BackoffBaseSec,
		"timeoutSec", d.cfg.Callback.TimeoutSec,
		"futureRetryWaitCap", futureRetryWaitCap,
		"maxConnsPerHost", maxConnsPerHostOrDefault(d.cfg.Callback.MaxConnsPerHost),
		"maxIdleConnsPerHost", maxIdleConnsPerHostOrDefault(d.cfg.Callback.MaxIdleConnsPerHost),
		// 0 means "no local pre-flight cap; discover the receiver's limit from
		// its 413". Surfaced at startup because whether this is set is the
		// difference between a diagnosable oversize failure and an opaque one.
		"maxBodyBytes", d.cfg.Callback.MaxBodyBytes,
		"bodyWarnBytes", bodyWarnBytes,
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

	// Start every configured consumer ('callback', plus the SEEN topic when the
	// split is enabled). A failure on any one aborts startup — a half-started
	// service would silently strand one class of callbacks.
	for _, c := range d.consumers {
		if err := c.Start(ctx); err != nil {
			return fmt.Errorf("failed to start callback consumer: %w", err)
		}
	}

	d.SetStarted(true)
	d.Logger.Info("callback delivery service started")
	return nil
}

// Stop gracefully shuts down the delivery service.
func (d *DeliveryService) Stop() error {
	d.Logger.Info("stopping callback delivery service")

	var firstErr error

	for _, c := range d.consumers {
		if c == nil {
			continue
		}
		if err := c.Stop(); err != nil {
			d.Logger.Error("failed to stop consumer", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Close every per-source-topic retry producer. Iterated over a sorted key
	// list so shutdown logging is deterministic rather than map-order random.
	for _, topic := range sortedKeys(d.retryProducers) {
		p := d.retryProducers[topic]
		if p == nil {
			continue
		}
		if err := p.Close(); err != nil {
			d.Logger.Error("failed to close retry producer", "topic", topic, "error", err)
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
	d.Logger.Info(
		"callback delivery service stopped",
		"messagesProcessed", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDelivered))),
		"messagesRetried", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeRetryScheduled))),
		"messagesFailed", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDLQ))),
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
			"messagesProcessed": fmt.Sprintf("%d", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDelivered)))),
			"messagesRetried":   fmt.Sprintf("%d", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeRetryScheduled)))),
			"messagesFailed":    fmt.Sprintf("%d", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDLQ)))),
			// Surfaces whether the SEEN split is actually live in this pod —
			// the first thing to check when SEEN callbacks stop arriving.
			"consumedTopics":  strings.Join(sortedKeys(d.retryProducers), ","),
			"consumerCount":   fmt.Sprintf("%d", len(d.consumers)),
			"seenSplitActive": fmt.Sprintf("%t", len(d.consumers) > 1),
		},
	}
}

// sortedKeys returns the keys of m in ascending order, so map iteration never
// leaks nondeterministic ordering into logs or health output.
func sortedKeys(m map[string]*kafka.Producer) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// retryProducerFor returns the producer that republishes retries back onto
// topic — the SOURCE topic the message was consumed from.
//
// Routing a retry to its source topic is a correctness requirement, not a
// nicety: before the SEEN split there was a single retry producer pinned to
// 'callback', and reusing it for SEEN messages would drop every SEEN retry
// onto the topic that STUMP floods block, reintroducing the head-of-line
// stall for exactly the callbacks that most need low latency.
//
// An unknown or empty topic (tests construct kafka.Message without one, and a
// consumer could in principle be pointed at another topic) falls back to the
// 'callback' producer, which is the pre-split behavior.
func (d *DeliveryService) retryProducerFor(topic string) *kafka.Producer {
	if p, ok := d.retryProducers[topic]; ok {
		return p
	}
	return d.retryProducers[d.cfg.Kafka.CallbackTopic]
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
//
// msg.Topic is threaded down the whole retry path so a republished retry goes
// back to the topic it came from — see retryProducerFor.
func (d *DeliveryService) handleMessage(ctx context.Context, msg *kafka.Message) error {
	cbMsg, err := kafka.DecodeCallbackTopicMessage(msg.Value)
	if err != nil {
		// A poison-pill message that cannot be decoded should not block the
		// partition forever. Log and ack so the consumer can advance — the
		// raw bytes are still inspectable via Kafka's retention.
		d.Logger.Error(
			"failed to decode callback message, skipping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDecodeError).Inc()
		metrics.IncKafkaConsumerError(msg.Topic, metrics.KafkaErrorDecode)
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
				return d.republishForRetry(ctx, msg.Topic, cbMsg, "future-dated retry not yet due")
			}
		}
	}

	return d.processDelivery(ctx, msg.Topic, cbMsg)
}

// processDelivery runs the dedup check, HTTP delivery, dedup record, and
// retry/DLQ logic for a single message inline. It returns nil only after the
// message reaches a durable terminal state; a non-nil return means the
// Kafka offset must NOT be marked so the message is re-consumed.
//
// sourceTopic is the topic the message was consumed from; it decides which
// retry producer a rescheduled delivery is republished through.
func (d *DeliveryService) processDelivery(ctx context.Context, sourceTopic string, cbMsg *kafka.CallbackTopicMessage) error {
	d.Logger.Debug(
		"processing callback message",
		logfields.CallbackURL(cbMsg.CallbackURL),
		logfields.TxID(cbMsg.TxID),
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		logfields.SubtreeIndex(cbMsg.SubtreeIndex),
	)

	// Check callback dedup — skip if already delivered.
	//
	// A reprocess-originated callback (BypassDedup) is an explicit "re-emit
	// this block's results" request, so it MUST force re-delivery past the
	// dedup set: deduping it against the original delivery silently defeats
	// arcade's reorg recovery (#208). We bypass only the existence *check*
	// here — the key is still recorded on success below — so normal
	// (non-reprocess) duplicates for the same block continue to be
	// suppressed afterward. Reprocess is operator/arcade-driven and
	// rate-limited at the /reprocess handler, and a forced delivery does not
	// itself trigger another reprocess, so this cannot self-perpetuate into
	// a callback storm.
	if d.dedupStore != nil && cbMsg.BypassDedup {
		d.Logger.Info(
			"forcing callback re-delivery (reprocess): bypassing dedup",
			"dedupKey", dedupKeyForMessage(cbMsg),
			logfields.CallbackURL(cbMsg.CallbackURL),
			"type", cbMsg.Type,
			logfields.BlockHash(cbMsg.BlockHash),
			logfields.SubtreeIndex(cbMsg.SubtreeIndex),
		)
	}
	if d.dedupStore != nil && !cbMsg.BypassDedup {
		dedupKey := dedupKeyForMessage(cbMsg)
		if dedupKey != "" {
			checkStart := time.Now()
			exists, err := d.dedupStore.Exists(dedupKey, cbMsg.CallbackURL, string(cbMsg.Type))
			checkOutcome := metrics.OutcomeMiss
			if err != nil {
				checkOutcome = metrics.OutcomeError
			} else if exists {
				checkOutcome = metrics.OutcomeHit
			}
			metrics.ObserveCallbackDedupCheck(checkOutcome, time.Since(checkStart))
			if err != nil {
				// Previous behavior was "proceed with delivery" — but if a
				// prior attempt had succeeded and Aerospike is briefly
				// unreadable, we'd deliver a duplicate BLOCK_PROCESSED. Safer
				// to fall through to the retry path so the next attempt
				// re-checks dedup once the store recovers.
				d.Logger.Error("dedup check failed, scheduling retry", "error", err, "dedupKey", dedupKey, logfields.CallbackURL(cbMsg.CallbackURL))
				return d.scheduleRetryOrDLQ(ctx, sourceTopic, cbMsg, fmt.Errorf("dedup check: %w", err))
			}
			if exists {
				// Log at info so silent suppressions are observable in
				// production without DEBUG. A persistent flood of these
				// for the same (callbackUrl, blockHash) signals that a
				// prior delivery hit DLQ and /reprocess is needed to
				// clear the stale dedup state — see
				// bsv-blockchain/merkle-service#122 for the full
				// post-DLQ failure mode.
				d.Logger.Info(
					"skipping duplicate callback delivery",
					"dedupKey", dedupKey,
					logfields.CallbackURL(cbMsg.CallbackURL),
					"type", cbMsg.Type,
					logfields.BlockHash(cbMsg.BlockHash),
					logfields.SubtreeIndex(cbMsg.SubtreeIndex),
				)
				metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDedupHit).Inc()
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
				recordStart := time.Now()
				ttl := time.Duration(d.cfg.Callback.DedupTTLSec) * time.Second
				recErr := d.dedupStore.Record(dedupKey, cbMsg.CallbackURL, string(cbMsg.Type), ttl)
				recordOutcome := metrics.OutcomeSuccess
				if recErr != nil {
					recordOutcome = metrics.OutcomeError
					d.Logger.Error("failed to record callback dedup", "error", recErr, "dedupKey", dedupKey, logfields.CallbackURL(cbMsg.CallbackURL))
				}
				metrics.ObserveCallbackDedupRecord(recordOutcome, time.Since(recordStart))
			}
		}
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDelivered).Inc()
		metrics.ObserveCallbackRetryAttempt(cbMsg.RetryCount)
		d.Logger.Debug(
			"callback delivered successfully",
			logfields.CallbackURL(cbMsg.CallbackURL),
			logfields.TxID(cbMsg.TxID),
			"type", cbMsg.Type,
			logfields.SubtreeIndex(cbMsg.SubtreeIndex),
		)
		return nil
	}

	d.Logger.Warn(
		"callback delivery failed",
		logfields.CallbackURL(cbMsg.CallbackURL),
		logfields.TxID(cbMsg.TxID),
		"type", cbMsg.Type,
		"retryCount", cbMsg.RetryCount,
		logfields.SubtreeIndex(cbMsg.SubtreeIndex),
		"error", deliverErr,
	)
	return d.scheduleRetryOrDLQ(ctx, sourceTopic, cbMsg, deliverErr)
}

// scheduleRetryOrDLQ decides whether a failed message should be republished
// for retry or routed to the DLQ, then performs the durable side-effect. It
// returns nil iff the durable side-effect succeeded; otherwise the caller
// must propagate the error up so the Kafka offset is not committed.
func (d *DeliveryService) scheduleRetryOrDLQ(ctx context.Context, sourceTopic string, cbMsg *kafka.CallbackTopicMessage, cause error) error {
	// Oversize is checked BEFORE the permanent check and before the retry
	// budget, because it needs the opposite treatment from both.
	//
	// It must not be permanent: 413 is curable by raising the receiver's cap,
	// and DLQ-on-first-413 is exactly what stranded a block on 2026-08-11
	// (see oversizeDeliveryError). So it falls through to the normal retry
	// ladder below — 5 attempts on a 30s linear backoff is ~7.5 minutes of
	// durable, Kafka-parked retrying, during which an operator raising the cap
	// heals the block with no further action. Beyond that window arcade's own
	// watchdog keeps the block recoverable: its completeness gate leaves
	// `processed_at` NULL and re-drives /reprocess, which re-emits this STUMP.
	//
	// It must also not trip the per-URL circuit breaker on the way out. The
	// breaker's premise is "this endpoint is dead, stop fanning out to it" —
	// but a receiver rejecting one oversize STUMP is emphatically alive, and
	// a block full of oversize STUMPs would cross the default threshold of 20
	// in a single block and auto-disable arcade's callback URL, killing SEEN
	// and BLOCK_PROCESSED delivery for every transaction, not just the
	// stranded block's. That is a strictly worse outage than the one we are
	// fixing.
	if o := asOversizeDeliveryError(cause); o != nil {
		if cbMsg.RetryCount < d.cfg.Callback.MaxRetries {
			// Retry budget left: fall through to the normal republish path,
			// which logs the scheduled retry. The loud ERROR naming the block
			// was already emitted at detection time by logOversize.
			return d.scheduleRetry(ctx, sourceTopic, cbMsg, cause)
		}
		// Budget exhausted. This is the "a block is stranded" moment — say so
		// in those words, with the block identity and the exact size, so an
		// operator reading logs or alerting on the metric knows immediately
		// which block is stuck and why. Distinct outcome label from the
		// generic DLQ counter precisely so it can be alerted on separately.
		d.Logger.Error(
			"BLOCK STRANDED: oversize callback exhausted its retries and is going to the DLQ — "+
				"the receiver never got this subtree's STUMP, so it cannot finalize the block and none of the "+
				"block's transactions can reach MINED. Raise the receiver's inbound body cap "+
				"(arcade: callback.max_body_bytes / ARCADE_CALLBACK_MAX_BODY_BYTES), then re-drive the block "+
				"with POST /reprocess",
			logfields.CallbackURL(cbMsg.CallbackURL),
			logfields.TxID(cbMsg.TxID),
			"type", cbMsg.Type,
			logfields.BlockHash(cbMsg.BlockHash),
			logfields.SubtreeIndex(cbMsg.SubtreeIndex),
			"bodyBytes", o.bodyBytes,
			"limitBytes", o.limitBytes,
			"status", o.statusCode,
			"retryCount", cbMsg.RetryCount,
			"reason", "oversize",
			"cause", cause,
		)
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeOversizeStranded).Inc()
		if err := d.publishToDLQDurably(ctx, cbMsg); err != nil {
			return err
		}
		// Deliberately NO recordCallbackURLFailure here — see above.
		return nil
	}

	// Permanent failures (e.g. STUMP blob expired) skip the retry budget and
	// go straight to the DLQ — retrying cannot recover them.
	if isPermanentDeliveryError(cause) {
		d.Logger.Error(
			"callback permanently failed, publishing to DLQ",
			logfields.CallbackURL(cbMsg.CallbackURL),
			logfields.TxID(cbMsg.TxID),
			"type", cbMsg.Type,
			"retryCount", cbMsg.RetryCount,
			logfields.SubtreeIndex(cbMsg.SubtreeIndex),
			"reason", "permanent",
			"cause", cause,
		)
		if err := d.publishToDLQDurably(ctx, cbMsg); err != nil {
			return err
		}
		d.recordCallbackURLFailure(cbMsg.CallbackURL)
		return nil
	}

	// Retry budget exhausted: route to DLQ.
	if cbMsg.RetryCount >= d.cfg.Callback.MaxRetries {
		d.Logger.Error(
			"callback retries exhausted, publishing to DLQ",
			logfields.CallbackURL(cbMsg.CallbackURL),
			logfields.TxID(cbMsg.TxID),
			"type", cbMsg.Type,
			"retryCount", cbMsg.RetryCount,
			logfields.SubtreeIndex(cbMsg.SubtreeIndex),
			"cause", cause,
		)
		if err := d.publishToDLQDurably(ctx, cbMsg); err != nil {
			return err
		}
		d.recordCallbackURLFailure(cbMsg.CallbackURL)
		return nil
	}

	return d.scheduleRetry(ctx, sourceTopic, cbMsg, cause)
}

// scheduleRetry bumps the retry count, computes the next NextRetryAt using
// linear backoff, and republishes back to the source topic. The republish is
// the durable side-effect: until it succeeds, we must not ack the source
// message.
//
// Extracted from scheduleRetryOrDLQ so the oversize branch — which is
// evaluated ahead of the permanent-failure check, and so cannot fall through
// to the tail of that function — schedules retries through exactly the same
// code path, with the same backoff and the same durability contract, as every
// other retryable failure. Duplicating it there would be a standing invitation
// for the two ladders to drift.
func (d *DeliveryService) scheduleRetry(ctx context.Context, sourceTopic string, cbMsg *kafka.CallbackTopicMessage, cause error) error {
	cbMsg.RetryCount++
	backoffSec := d.cfg.Callback.BackoffBaseSec * cbMsg.RetryCount
	cbMsg.NextRetryAt = time.Now().Add(time.Duration(backoffSec) * time.Second)

	d.Logger.Info(
		"scheduling callback retry via Kafka republish",
		logfields.CallbackURL(cbMsg.CallbackURL),
		logfields.TxID(cbMsg.TxID),
		"retryCount", cbMsg.RetryCount,
		"nextRetryAt", cbMsg.NextRetryAt,
		"backoffSec", backoffSec,
		logfields.SubtreeIndex(cbMsg.SubtreeIndex),
		"cause", cause,
	)

	return d.republishForRetry(ctx, sourceTopic, cbMsg, "retry after delivery failure")
}

// recordCallbackURLFailure trips the per-URL circuit breaker after a callback
// to this URL was DLQ'd. When the URL crosses cfg.Callback.BreakerThreshold it
// is disabled in the registry so BLOCK_PROCESSED / STUMP fan-out stops
// targeting it; a re-registration via /watch clears the breaker. A nil registry
// or a non-positive threshold disables the breaker entirely. Best-effort —
// a registry error is logged, not propagated (the DLQ publish already
// succeeded, so the source offset must still be committed).
func (d *DeliveryService) recordCallbackURLFailure(callbackURL string) {
	threshold := d.cfg.Callback.BreakerThreshold
	if d.urlRegistry == nil || threshold <= 0 || callbackURL == "" {
		return
	}
	disabled, err := d.urlRegistry.RecordFailure(callbackURL, threshold)
	if err != nil {
		d.Logger.Warn("failed to record callback URL breaker failure",
			logfields.CallbackURL(callbackURL), "error", err)
		return
	}
	if disabled {
		d.Logger.Warn("callback URL auto-disabled after repeated DLQ'd deliveries — "+
			"re-register via /watch to re-enable",
			logfields.CallbackURL(callbackURL), "threshold", threshold)
	}
}

// republishForRetry encodes cbMsg and publishes it back to sourceTopic — the
// topic it was consumed from — via that topic's retry producer. The partition
// key is derived from the message's scope (subtree or block, plus URL) so
// retries land on the same partition as the original — preserving
// STUMP → BLOCK_PROCESSED ordering for the same (subtree, callbackURL) — while
// still scattering across partitions when there's a single registered URL
// (F-059).
//
// Routing by sourceTopic is what keeps the SEEN split intact across retries: a
// retry producer pinned to 'callback' would drag every retried SEEN callback
// back behind the STUMP flood it was split away from.
//
// Returning nil means the publish was acknowledged by Kafka and the source
// message can now be safely ack'd. A non-nil return means the publish failed
// and the caller must NOT mark the offset.
func (d *DeliveryService) republishForRetry(ctx context.Context, sourceTopic string, cbMsg *kafka.CallbackTopicMessage, reason string) error {
	data, err := cbMsg.Encode()
	if err != nil {
		return fmt.Errorf("encode callback message for retry republish (%s): %w", reason, err)
	}
	producer := d.retryProducerFor(sourceTopic)
	if producer == nil {
		// No retry channel at all: never ack, so the message is redelivered
		// rather than silently dropped.
		return fmt.Errorf("no retry producer for source topic %q (%s)", sourceTopic, reason)
	}
	// WithoutCancel: this republish is a durable retry hand-off that must not
	// be dropped by the originating consumer ctx being canceled mid-flight;
	// only the trace context is preserved.
	if err := producer.PublishWithHashKey(context.WithoutCancel(ctx), cbMsg.PartitionKey(), data); err != nil {
		d.Logger.Error(
			"retry republish failed, leaving Kafka offset uncommitted",
			logfields.CallbackURL(cbMsg.CallbackURL),
			logfields.TxID(cbMsg.TxID),
			"retryCount", cbMsg.RetryCount,
			"reason", reason,
			"sourceTopic", sourceTopic,
			"error", err,
		)
		return fmt.Errorf("retry republish (%s): %w", reason, err)
	}
	metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeRetryScheduled).Inc()
	return nil
}

// publishToDLQDurably publishes a permanently failed message to the DLQ topic
// and returns nil only on success. It retries the publish a few times before
// surfacing the failure so the Kafka offset is not committed when the DLQ
// itself is unreachable — that way the message is reconsidered on the next
// session instead of being silently dropped.
func (d *DeliveryService) publishToDLQDurably(ctx context.Context, cbMsg *kafka.CallbackTopicMessage) error {
	delays := []time.Duration{0, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	var lastErr error
	for i, delay := range delays {
		if delay > 0 {
			time.Sleep(delay)
		}
		if err := d.publishToDLQ(ctx, cbMsg); err != nil {
			lastErr = err
			d.Logger.Warn(
				"DLQ publish attempt failed",
				"attempt", i+1,
				logfields.CallbackURL(cbMsg.CallbackURL),
				logfields.TxID(cbMsg.TxID),
				"error", err,
			)
			continue
		}
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDLQ).Inc()
		return nil
	}
	d.Logger.Error(
		"DLQ publish exhausted all retries, leaving Kafka offset uncommitted",
		logfields.CallbackURL(cbMsg.CallbackURL),
		logfields.TxID(cbMsg.TxID),
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
			d.Logger.Info(
				"callback delivery heartbeat",
				"messagesProcessed", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDelivered))),
				"messagesRetried", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeRetryScheduled))),
				"messagesFailed", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDLQ))),
				"messagesDedupe", int64(testutil.ToFloat64(metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeDedupHit))),
			)
		}
	}
}

// deliverCallback makes an HTTP POST to the callback URL with the
// CallbackMessage payload. When msg.CallbackToken is non-empty the request
// carries `Authorization: Bearer <token>` — arcade's callback endpoint
// requires this header. An empty token sends the request unauthenticated,
// preserving today's behavior for deployments where arcade has not yet
// shipped its matching token-passing change.
func (d *DeliveryService) deliverCallback(ctx context.Context, msg *kafka.CallbackTopicMessage) error {
	payload := callbackPayload{
		Type:         string(msg.Type),
		TxID:         msg.TxID,
		TxIDs:        msg.TxIDs,
		BlockHash:    msg.BlockHash,
		SubtreeIndex: msg.SubtreeIndex,
		// BLOCK_PROCESSED enrichment — pass through verbatim. Empty for other
		// callback types (and for BLOCK_PROCESSED produced before the producer
		// stamped them), where omitempty drops them from the JSON.
		MerkleRoot:             msg.MerkleRoot,
		SubtreeCount:           msg.SubtreeCount,
		SubtreeHashes:          msg.SubtreeHashes,
		CoinbaseBUMP:           msg.CoinbaseBUMP,
		ExpectedSubtreeIndices: msg.ExpectedSubtreeIndices,
	}

	// STUMP bytes are claim-checked: CallbackTopicMessage carries only a ref,
	// fetch the blob and hex-encode for the HTTP payload. The auth token is a
	// header, so the finished body is identical for every subscriber of this
	// (block, subtree) — memoize the marshaled bytes and pay the blob fetch,
	// hex encode, and JSON marshal once per subtree instead of once per
	// subscriber.
	var body []byte
	if msg.Type == kafka.CallbackStump && msg.StumpRef != "" {
		if d.stumpStore == nil {
			return errPermanentDelivery(fmt.Errorf("stump store not configured for STUMP delivery"))
		}
		bodyKey := msg.StumpRef + "|" + msg.BlockHash + "|" + strconv.Itoa(msg.SubtreeIndex)
		if body = d.cachedBody(bodyKey); body == nil {
			stumpStart := time.Now()
			stumpBytes, err := d.stumpStore.Get(msg.StumpRef)
			stumpOutcome := metrics.OutcomeSuccess
			if err != nil {
				if errors.Is(err, store.ErrStumpNotFound) {
					stumpOutcome = metrics.OutcomeNotFound
				} else {
					stumpOutcome = metrics.OutcomeError
				}
			}
			metrics.ObserveCallbackStumpFetch(stumpOutcome, time.Since(stumpStart))
			if err != nil {
				if errors.Is(err, store.ErrStumpNotFound) {
					// Blob expired (DAH) or never written — no amount of retry will
					// produce it. Fail permanently so processDelivery routes to DLQ.
					metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeStumpNotFound).Inc()
					return errPermanentDelivery(fmt.Errorf("stump blob missing for ref %s: %w", msg.StumpRef, err))
				}
				return fmt.Errorf("fetching stump ref %s: %w", msg.StumpRef, err)
			}
			payload.Stump = hex.EncodeToString(stumpBytes)
			body, err = json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed to marshal callback payload: %w", err)
			}
			d.storeBody(bodyKey, body)
		}
	} else {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal callback payload: %w", err)
		}
	}

	// Size gate. The body is finished at this point (including a cache hit),
	// so this covers every delivery, not just freshly-built ones.
	//
	// The WARN is unconditional early warning; the pre-flight refusal only
	// fires when an operator has told us the receiver's cap. Refusing here
	// rather than POSTing saves a doomed multi-MiB upload plus a request
	// timeout, and — far more importantly — produces a diagnosis that names
	// the block hash, subtree index and exact byte count, instead of an
	// opaque 413 from the far end that we historically threw away as
	// "permanent". See oversizeDeliveryError for the incident.
	if len(body) > bodyWarnBytes {
		d.Logger.Warn(
			"callback body is approaching the receiver's inbound size limit — "+
				"a STUMP that crosses it is rejected with 413 and strands its block until the cap is raised",
			logfields.CallbackURL(msg.CallbackURL),
			"type", msg.Type,
			logfields.BlockHash(msg.BlockHash),
			logfields.SubtreeIndex(msg.SubtreeIndex),
			"bodyBytes", len(body),
			"warnThresholdBytes", bodyWarnBytes,
			"configuredMaxBodyBytes", d.cfg.Callback.MaxBodyBytes,
		)
	}
	if limit := d.cfg.Callback.MaxBodyBytes; limit > 0 && int64(len(body)) > limit {
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeOversize).Inc()
		// No request is issued on this path, so ObserveCallbackDelivery never
		// runs — record the size explicitly so the largest bodies (the ones
		// that matter most for spotting the growth trend) are not silently
		// missing from the payload-size histogram.
		metrics.ObserveCallbackPayloadSize(msg.CallbackURL, len(body))
		d.logOversize(msg, len(body), limit, 0,
			"callback body exceeds callback.maxBodyBytes — refusing to POST a body the receiver will reject with 413")
		return errOversizeDelivery(
			fmt.Errorf("callback body %d bytes exceeds configured callback.maxBodyBytes %d", len(body), limit),
			len(body), limit, 0,
		)
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

	// Bearer token authentication when arcade configured one for this URL.
	// Empty token → no Authorization header so older deployments without the
	// matching arcade-side token check continue to work.
	if msg.CallbackToken != "" {
		req.Header.Set("Authorization", "Bearer "+msg.CallbackToken)
	}

	start := time.Now()
	resp, err := d.httpClient.Do(req)
	if err != nil {
		d.Logger.Debug(
			"callback http transport error",
			logfields.CallbackURL(msg.CallbackURL),
			"durationMs", time.Since(start).Milliseconds(),
			"idempotencyKey", idempotencyKey,
			"type", msg.Type,
			logfields.TxID(msg.TxID),
			logfields.SubtreeIndex(msg.SubtreeIndex),
			"error", err,
		)
		metrics.ObserveCallbackDelivery(msg.CallbackURL, 0, len(body), time.Since(start), err)
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	metrics.ObserveCallbackDelivery(msg.CallbackURL, resp.StatusCode, len(body), time.Since(start), nil)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodyBytes, truncated := readBodyCapped(resp.Body, 4*1024)
	d.Logger.Debug(
		"callback http error response",
		logfields.CallbackURL(msg.CallbackURL),
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
		logfields.TxID(msg.TxID),
		logfields.SubtreeIndex(msg.SubtreeIndex),
	)

	snippet := bodyBytes
	if len(snippet) > 256 {
		snippet = snippet[:256]
	}
	var statusErr error
	if len(snippet) == 0 {
		statusErr = fmt.Errorf("callback returned non-2xx status: %d", resp.StatusCode)
	} else {
		statusErr = fmt.Errorf("callback returned non-2xx status: %d: %s", resp.StatusCode, string(snippet))
	}
	// 413 first: it is a 4xx, but it is a payload-SIZE problem, not a
	// "this request is invalid forever" problem, and conflating the two is
	// what stranded a whole block's transactions on 2026-08-11 (see
	// oversizeDeliveryError). Give it its own class so it keeps its retry
	// budget and never trips the per-URL circuit breaker.
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomeOversize).Inc()
		d.logOversize(msg, len(body), d.cfg.Callback.MaxBodyBytes, resp.StatusCode,
			"callback REJECTED as too large by the receiver (413) — raise the receiver's inbound body cap "+
				"(arcade: callback.max_body_bytes / ARCADE_CALLBACK_MAX_BODY_BYTES); until it is raised, "+
				"this block cannot be finalized and its transactions cannot reach MINED")
		return errOversizeDelivery(statusErr, len(body), d.cfg.Callback.MaxBodyBytes, resp.StatusCode)
	}
	// 4xx other than 408 (Request Timeout), 413 (handled above) and 429 (Too
	// Many Requests) signal a client-side problem the receiver cannot resolve
	// by us retrying — wrong URL, missing/invalid auth token, malformed
	// payload, gone. Route straight to DLQ so a misconfigured arcade does not
	// pin a callback worker on every block for the full retry budget. 408/429
	// stay retryable because they are explicit "try again later" signals.
	if isNonRetryable4xx(resp.StatusCode) {
		metrics.CallbackMessagesTotal.WithLabelValues(metrics.OutcomePermanent4xx).Inc()
		return errPermanentDelivery(statusErr)
	}
	return statusErr
}

// logOversize emits the single operator-facing ERROR for an oversize
// callback. It deliberately names the block hash, subtree index and exact
// body size: those three facts are what turn "some callback failed" into
// "block <hash> subtree <n> is stranded because its body is <n> bytes and the
// receiver's cap is lower than that", which is diagnosable at a glance.
//
// limit is 0 when we only learned about the cap from a 413 and the operator
// has not mirrored the receiver's value into callback.maxBodyBytes; status is
// 0 when our own pre-flight check refused the POST.
func (d *DeliveryService) logOversize(msg *kafka.CallbackTopicMessage, bodyBytes int, limit int64, status int, headline string) {
	d.Logger.Error(
		headline,
		logfields.CallbackURL(msg.CallbackURL),
		logfields.TxID(msg.TxID),
		"type", msg.Type,
		logfields.BlockHash(msg.BlockHash),
		logfields.SubtreeIndex(msg.SubtreeIndex),
		"bodyBytes", bodyBytes,
		"limitBytes", limit,
		"status", status,
		"stumpRef", msg.StumpRef,
		"retryCount", msg.RetryCount,
	)
}

// isNonRetryable4xx reports whether code is a 4xx response that we should
// treat as permanent. 408 (Request Timeout) and 429 (Too Many Requests) are
// the standard "try again later" 4xx codes and stay on the retry path.
//
// 413 (Request Entity Too Large) is likewise NOT permanent, but for a
// different reason: it is curable by an operator raising the receiver's cap,
// so it gets its own oversize class with its own routing rather than sharing
// the generic retry path — see oversizeDeliveryError and the caller above,
// which peels 413 off before consulting this function. It is listed here too
// so that any future caller of isNonRetryable4xx cannot silently reintroduce
// the "413 == permanent == DLQ == stranded block" bug.
func isNonRetryable4xx(code int) bool {
	if code < 400 || code >= 500 {
		return false
	}
	switch code {
	case http.StatusRequestTimeout, http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
		return false
	}
	return true
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
//
// The SSRF-guarded transport is wrapped with otelhttp.NewTransport so every
// callback POST carries a client span and a traceparent header derived from
// deliverCallback's request context — this is what lets the trace begun on
// Kafka consume (internal/kafka's consumer span in dispatchRecord) ride all
// the way back into arcade's callback handler. With telemetry disabled this
// uses the global no-op TracerProvider, so the wrap is inert.
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
		Transport: otelhttp.NewTransport(transport),
	}
}

// publishToDLQ publishes a permanently failed message to the dead-letter queue topic.
func (d *DeliveryService) publishToDLQ(ctx context.Context, msg *kafka.CallbackTopicMessage) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode callback message for DLQ: %w", err)
	}

	// WithoutCancel: DLQ hand-off must survive a canceled consumer ctx.
	if err := d.dlqProducer.PublishWithHashKey(context.WithoutCancel(ctx), msg.PartitionKey(), data); err != nil {
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	return nil
}
