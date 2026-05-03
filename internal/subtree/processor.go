package subtree

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// RegistrationGetter abstracts registration lookups for testability.
//
// The shape mirrors store.RegistrationStore: BatchGet/Get return
// []store.CallbackEntry so the per-URL bearer token reaches the SEEN
// callback emit sites and is propagated through CallbackTopicMessage to
// the delivery service.
type RegistrationGetter interface {
	BatchGet(txids []string) (map[string][]store.CallbackEntry, error)
	Get(txid string) ([]store.CallbackEntry, error)
}

// SeenCounter abstracts seen-count tracking for testability.
type SeenCounter interface {
	Increment(txid, subtreeID string) (*store.IncrementResult, error)
}

// RegCache abstracts the registration deduplication cache for testability.
//
// The cache only stores positive results. Negative ("not registered")
// lookups are intentionally not cached so that a /watch arriving after
// an early negative lookup is not hidden until cache eviction (F-020).
type RegCache interface {
	FilterUncached(txids []string) (uncached, cachedRegistered []string)
	SetMultiRegistered(txids []string) error
}

// Processor consumes subtree announcement messages from Kafka, fetches full
// subtree data from DataHub, stores it, checks registrations, and emits callbacks.
type Processor struct {
	service.BaseService

	cfg               *config.Config
	consumer          *kafka.Consumer
	callbackProducer  *kafka.Producer
	retryProducer     *kafka.Producer // re-publishes to the subtree topic on transient failure
	dlqProducer       *kafka.Producer // publishes to subtree-dlq when MaxAttempts is exceeded
	registrationStore RegistrationGetter
	seenCounterStore  SeenCounter
	subtreeStore      store.SubtreeStore
	regCache          RegCache
	dedupCache        *cache.DedupCache
	dataHubClient     *datahub.Client

	messagesProcessed atomic.Int64
	messagesRetried   atomic.Int64
	messagesDLQ       atomic.Int64
}

// NewProcessor creates a new subtree Processor.
func NewProcessor(
	cfg *config.Config,
	registrationStore RegistrationGetter,
	seenCounterStore SeenCounter,
	subtreeStore store.SubtreeStore,
) *Processor {
	return &Processor{
		cfg:               cfg,
		registrationStore: registrationStore,
		seenCounterStore:  seenCounterStore,
		subtreeStore:      subtreeStore,
	}
}

// Init initializes the subtree processor, setting up the Kafka consumer, producer, and registration cache.
func (p *Processor) Init(_ interface{}) error {
	p.InitBase("subtree-fetcher")

	// Initialize DataHub client. SSRF guard rejects peer-supplied URLs
	// that point at private/loopback/link-local destinations unless the
	// operator opts in via DataHub.AllowPrivateIPs (F-028).
	p.dataHubClient = datahub.NewClientWithSSRFGuard(
		p.cfg.DataHub.TimeoutSec,
		p.cfg.DataHub.MaxRetries,
		p.cfg.DataHub.MaxBlockBytes,
		p.cfg.DataHub.MaxSubtreeBytes,
		p.cfg.DataHub.AllowPrivateIPs,
		p.Logger,
	)

	// Initialize message dedup cache.
	if p.cfg.Subtree.DedupCacheSize > 0 {
		p.dedupCache = cache.NewDedupCache(p.cfg.Subtree.DedupCacheSize)
	}

	// Initialize registration deduplication cache (txmetacache).
	regCache, err := cache.NewRegistrationCache(p.cfg.Subtree.CacheMaxMB, p.Logger)
	if err != nil {
		p.Logger.Warn("failed to create registration cache, proceeding without cache", "error", err)
	} else {
		p.regCache = regCache
	}

	callbackProducer, err := kafka.NewProducer(
		p.cfg.Kafka.Brokers,
		p.cfg.Kafka.CallbackTopic,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback producer: %w", err)
	}
	p.callbackProducer = callbackProducer

	// Bounded-retry producer: transient failures republish the subtree message
	// back onto the same topic with AttemptCount+1. Separate producer so we can
	// close it explicitly on shutdown without touching callbackProducer.
	retryProducer, err := kafka.NewProducer(
		p.cfg.Kafka.Brokers,
		p.cfg.Kafka.SubtreeTopic,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree retry producer: %w", err)
	}
	p.retryProducer = retryProducer

	// DLQ producer: when AttemptCount hits SubtreeConfig.MaxAttempts the
	// message is routed here instead of being re-driven again, preventing the
	// partition-stall that the consumer-without-MarkMessage path used to cause.
	dlqProducer, err := kafka.NewProducer(
		p.cfg.Kafka.Brokers,
		p.cfg.Kafka.SubtreeDLQTopic,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree DLQ producer: %w", err)
	}
	p.dlqProducer = dlqProducer

	consumer, err := kafka.NewConsumer(
		p.cfg.Kafka.Brokers,
		p.cfg.Kafka.ConsumerGroup+"-subtree",
		[]string{p.cfg.Kafka.SubtreeTopic},
		p.handleMessage,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree consumer: %w", err)
	}
	p.consumer = consumer

	p.Logger.Info("subtree-fetcher initialized",
		"storageMode", p.cfg.Subtree.StorageMode,
		"subtreeTopic", p.cfg.Kafka.SubtreeTopic,
		"subtreeDLQTopic", p.cfg.Kafka.SubtreeDLQTopic,
		"callbackTopic", p.cfg.Kafka.CallbackTopic,
		"maxAttempts", p.cfg.Subtree.MaxAttempts,
		"cacheEnabled", p.regCache != nil,
	)

	return nil
}

// Start begins consuming subtree messages from Kafka.
func (p *Processor) Start(ctx context.Context) error {
	p.Logger.Info("starting subtree-fetcher")

	if err := p.consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start subtree consumer: %w", err)
	}

	p.SetStarted(true)
	p.Logger.Info("subtree-fetcher started")
	return nil
}

// Stop gracefully shuts down the subtree processor.
func (p *Processor) Stop() error {
	p.Logger.Info("stopping subtree-fetcher")

	var firstErr error

	if p.consumer != nil {
		if err := p.consumer.Stop(); err != nil {
			p.Logger.Error("failed to stop consumer", "error", err)
			firstErr = err
		}
	}

	if p.callbackProducer != nil {
		if err := p.callbackProducer.Close(); err != nil {
			p.Logger.Error("failed to close callback producer", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if p.retryProducer != nil {
		if err := p.retryProducer.Close(); err != nil {
			p.Logger.Error("failed to close retry producer", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if p.dlqProducer != nil {
		if err := p.dlqProducer.Close(); err != nil {
			p.Logger.Error("failed to close DLQ producer", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	p.SetStarted(false)
	p.Cancel()
	p.Logger.Info("subtree-fetcher stopped",
		"messagesProcessed", p.messagesProcessed.Load(),
		"messagesRetried", p.messagesRetried.Load(),
		"messagesDLQ", p.messagesDLQ.Load(),
	)
	return firstErr
}

// Health returns the current health status of the subtree processor.
func (p *Processor) Health() service.HealthStatus {
	status := "healthy"
	if !p.IsStarted() {
		status = "unhealthy"
	}

	return service.HealthStatus{
		Name:   p.Name,
		Status: status,
		Details: map[string]string{
			"messagesProcessed": fmt.Sprintf("%d", p.messagesProcessed.Load()),
		},
	}
}

// handleMessage processes a single subtree announcement message from Kafka.
//
// On transient failure (DataHub/blob store/parse/registration lookup, any
// SEEN callback encode/publish failure, or a seen-counter increment failure)
// the message is re-published to the subtree topic with AttemptCount+1 and
// nil is returned so the consumer MarkMessage's and the partition advances.
// Once AttemptCount reaches SubtreeConfig.MaxAttempts the message is routed
// to the subtree-dlq topic instead of being re-driven again.
//
// The dedup cache is updated only on full success — any transient failure
// path returns before p.dedupCache.Add, so a redelivery from the retry
// pipeline is reprocessed rather than being silently skipped (F-057, F-058).
//
// The only errors returned upward are producer-level failures that prevent us
// from either acking or requeueing — those still stall the partition so we
// don't lose data, but they indicate Kafka-side trouble rather than a poison
// pill.
func (p *Processor) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	subtreeMsg, err := kafka.DecodeSubtreeMessage(msg.Value)
	if err != nil {
		// Malformed bytes at the head of the partition cannot be recovered by
		// re-driving — drop the offset by returning nil after logging. A
		// decode failure is not DLQ-able because we don't have a structured
		// message to wrap.
		p.Logger.Error("failed to decode subtree message, dropping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		return nil
	}

	p.Logger.Debug("processing subtree announcement",
		"hash", subtreeMsg.Hash,
		"dataHubUrl", subtreeMsg.DataHubURL,
		"attemptCount", subtreeMsg.AttemptCount,
	)

	// Check dedup cache — skip if already successfully processed.
	if p.dedupCache != nil && p.dedupCache.Contains(subtreeMsg.Hash) {
		p.Logger.Debug("skipping duplicate subtree message", "hash", subtreeMsg.Hash)
		return nil
	}

	// 3.2: Fetch binary subtree data from DataHub.
	rawData, err := p.dataHubClient.FetchSubtreeRaw(ctx, subtreeMsg.DataHubURL, subtreeMsg.Hash)
	if err != nil {
		return p.handleTransientFailure(subtreeMsg, "fetching subtree from DataHub", err)
	}

	// 3.3: Store raw binary data in the subtree blob store.
	if p.cfg.Subtree.StorageMode == "realtime" {
		if err = p.subtreeStore.StoreSubtree(subtreeMsg.Hash, rawData, 0); err != nil {
			return p.handleTransientFailure(subtreeMsg, "storing subtree", err)
		}
	}

	// 3.4: Parse raw binary data into txid list.
	// DataHub returns concatenated 32-byte hashes, not full go-subtree Serialize() format.
	txids, err := datahub.ParseRawTxids(rawData)
	if err != nil {
		return p.handleTransientFailure(subtreeMsg, "parsing subtree txids", err)
	}
	p.Logger.Debug("processing subtree txids", "length", len(txids), "hash", subtreeMsg.Hash)

	if len(txids) == 0 {
		if p.dedupCache != nil {
			p.dedupCache.Add(subtreeMsg.Hash)
		}
		p.messagesProcessed.Add(1)
		return nil
	}

	// 4.2-4.4: Check registrations via cache and Aerospike.
	registeredTxids, err := p.findRegisteredTxids(txids)
	if err != nil {
		return p.handleTransientFailure(subtreeMsg, "checking registrations", err)
	}

	// 4.5-4.6: Emit batched callbacks grouped by callbackURL.
	//
	// A failure to encode/publish a SEEN_ON_NETWORK or SEEN_MULTIPLE_NODES
	// notification must NOT be silently swallowed: route through the same
	// retry/DLQ pipeline as a processing failure so the subtree message is
	// either retried or terminally DLQ'd. Otherwise downstream callback
	// consumers permanently lose SEEN notifications during a Kafka
	// callback-topic outage (F-057).
	if err := p.emitBatchedSeenCallbacks(registeredTxids, subtreeMsg.Hash); err != nil {
		return p.handleTransientFailure(subtreeMsg, "publishing batched SEEN callbacks", err)
	}

	// Mark subtree as successfully processed for dedup.
	if p.dedupCache != nil {
		p.dedupCache.Add(subtreeMsg.Hash)
	}

	p.messagesProcessed.Add(1)
	return nil
}

// handleTransientFailure bumps AttemptCount and either re-publishes the
// message to the subtree topic or, once MaxAttempts has been reached, parks
// it on subtree-dlq. Returns nil on successful hand-off so the consumer acks
// the original offset; returns an error only when the producer itself is
// broken (partition stall is preferable to silent loss in that case).
func (p *Processor) handleTransientFailure(subtreeMsg *kafka.SubtreeMessage, stage string, cause error) error {
	nextAttempt := subtreeMsg.AttemptCount + 1
	maxAttempts := p.cfg.Subtree.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}

	if nextAttempt >= maxAttempts {
		p.Logger.Error("subtree message exceeded max attempts, routing to DLQ",
			"hash", subtreeMsg.Hash,
			"stage", stage,
			"attemptCount", subtreeMsg.AttemptCount,
			"maxAttempts", maxAttempts,
			"error", cause,
		)
		subtreeMsg.AttemptCount = nextAttempt
		data, encErr := subtreeMsg.Encode()
		if encErr != nil {
			return fmt.Errorf("encoding subtree message for DLQ: %w", encErr)
		}
		if pubErr := p.dlqProducer.Publish(subtreeMsg.Hash, data); pubErr != nil {
			return fmt.Errorf("publishing subtree message to DLQ: %w", pubErr)
		}
		p.messagesDLQ.Add(1)
		return nil
	}

	p.Logger.Warn("subtree message transient failure, re-publishing for retry",
		"hash", subtreeMsg.Hash,
		"stage", stage,
		"attemptCount", subtreeMsg.AttemptCount,
		"nextAttempt", nextAttempt,
		"error", cause,
	)
	subtreeMsg.AttemptCount = nextAttempt
	data, encErr := subtreeMsg.Encode()
	if encErr != nil {
		return fmt.Errorf("encoding subtree message for retry: %w", encErr)
	}
	if pubErr := p.retryProducer.Publish(subtreeMsg.Hash, data); pubErr != nil {
		return fmt.Errorf("re-publishing subtree message for retry: %w", pubErr)
	}
	p.messagesRetried.Add(1)
	return nil
}

// findRegisteredTxids uses the cache and Aerospike to find which txids are registered.
// Returns a map of txid → []CallbackEntry (URL + token) for all registered txids.
func (p *Processor) findRegisteredTxids(txids []string) (map[string][]store.CallbackEntry, error) {
	var uncached, cachedRegistered []string

	if p.regCache != nil {
		uncached, cachedRegistered = p.regCache.FilterUncached(txids)
	} else {
		uncached = txids
	}

	// 4.3: Batch lookup uncached txids in Aerospike.
	var registeredFromStore map[string][]store.CallbackEntry
	if len(uncached) > 0 {
		var err error
		registeredFromStore, err = p.registrationStore.BatchGet(uncached)
		if err != nil {
			return nil, fmt.Errorf("batch get registrations: %w", err)
		}
	}

	// 4.4: Update cache with positive results only. Negatives are NOT
	// cached: a txid observed before its /watch registration must remain
	// looked up against the backing store on subsequent passes (F-020).
	if p.regCache != nil && len(registeredFromStore) > 0 {
		foundTxids := make([]string, 0, len(registeredFromStore))
		for _, txid := range uncached {
			if _, found := registeredFromStore[txid]; found {
				foundTxids = append(foundTxids, txid)
			}
		}
		if len(foundTxids) > 0 {
			_ = p.regCache.SetMultiRegistered(foundTxids)
		}
	}

	// Combine: start with uncached results (already have CallbackEntry from BatchGet).
	allRegistered := make(map[string][]store.CallbackEntry, len(cachedRegistered)+len(registeredFromStore))
	for txid, entries := range registeredFromStore {
		allRegistered[txid] = entries
	}

	// For cached-registered txids, fetch CallbackEntry tuples via BatchGet.
	//
	// A failure here MUST surface as an error (F-056). The cache told us these
	// txids are registered; if the backing store lookup fails we cannot
	// construct an accurate registeredTxids map. Returning a partial map and
	// letting the caller proceed would mark the subtree processed in the dedup
	// cache and permanently drop SEEN_ON_NETWORK and threshold callbacks for
	// the affected txids on redelivery. Propagate the error so handleMessage
	// re-drives via handleTransientFailure (which leaves the dedup cache
	// untouched).
	if len(cachedRegistered) > 0 {
		cachedEntries, err := p.registrationStore.BatchGet(cachedRegistered)
		if err != nil {
			return nil, fmt.Errorf("batch get callbackURLs for cached txids: %w", err)
		}
		for txid, entries := range cachedEntries {
			allRegistered[txid] = entries
		}
	}

	return allRegistered, nil
}

// emitBatchedSeenCallbacks emits batched SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES callbacks.
// Groups txids by callbackURL and publishes one message per callbackURL.
//
// Returns a non-nil error if any per-URL encode or publish fails, or if any
// seenCounterStore.Increment call fails. The loop continues past a single
// per-txid/per-URL failure so independent callback targets still receive
// their best-effort delivery on this attempt (partial success), but the
// first error encountered is returned to the caller so handleMessage can
// re-drive the subtree message through handleTransientFailure rather than
// silently acking and dropping SEEN notifications.
//
// F-057 made publish failures bubble up. F-058 extends the same contract to
// seen-counter increment failures: previously, a transient
// seenCounterStore.Increment error was logged and skipped while
// handleMessage still added the subtree hash to the dedup cache, permanently
// undercounting network observations and suppressing SEEN_MULTIPLE_NODES
// callbacks for the affected txids. Returning the error keeps the dedup
// cache untouched (handleMessage gates that add on success) and routes the
// work through handleTransientFailure for redelivery.
func (p *Processor) emitBatchedSeenCallbacks(registeredTxids map[string][]store.CallbackEntry, subtreeID string) error {
	if len(registeredTxids) == 0 {
		return nil
	}

	// Track the first error so the caller can re-drive the whole subtree
	// message, while still attempting the remaining URLs (each callback target
	// is independent — a hiccup on one shouldn't deny delivery to the others
	// on this attempt).
	var firstErr error

	// Invert txid→[]CallbackEntry to callbackURL→txids for SEEN_ON_NETWORK,
	// while remembering the latest token observed per URL. If multiple txids
	// have the same URL with different tokens (mid-rotation), the non-empty
	// token wins; in practice they all came through the same /watch payload.
	seenGroups := make(map[string][]string)
	urlTokens := make(map[string]string)
	for txid, entries := range registeredTxids {
		for _, e := range entries {
			seenGroups[e.URL] = append(seenGroups[e.URL], txid)
			if existing, ok := urlTokens[e.URL]; !ok || (existing == "" && e.Token != "") {
				urlTokens[e.URL] = e.Token
			}
		}
	}

	// 4.5: Emit one batched SEEN_ON_NETWORK per callbackURL, chunked so the JSON
	// payload stays comfortably under Kafka brokers' default message.max.bytes (1MB).
	for callbackURL, txids := range seenGroups {
		for _, chunk := range chunkTxIDs(txids, callbackBatchChunkSize) {
			msg := &kafka.CallbackTopicMessage{
				CallbackURL:   callbackURL,
				CallbackToken: urlTokens[callbackURL],
				Type:          kafka.CallbackSeenOnNetwork,
				TxIDs:         chunk,
			}
			data, err := msg.Encode()
			if err != nil {
				p.Logger.Error("failed to encode batched SEEN_ON_NETWORK", "callbackURL", callbackURL, "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("encoding SEEN_ON_NETWORK for %s: %w", callbackURL, err)
				}
				continue
			}
			if err := p.callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
				p.Logger.Error("failed to publish batched SEEN_ON_NETWORK", "callbackURL", callbackURL, "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("publishing SEEN_ON_NETWORK for %s: %w", callbackURL, err)
				}
			}
		}
	}

	// 4.6: Increment seen counters and collect threshold-reached txids.
	//
	// A failure here MUST surface as an error (F-058). Pre-fix this path
	// logged a warning and continued, while handleMessage still added the
	// subtree hash to the dedup cache — permanently dropping
	// SEEN_MULTIPLE_NODES callbacks for the affected txids on redelivery.
	// Capture the first error and return it so handleMessage re-drives via
	// handleTransientFailure (which leaves the dedup cache untouched), but
	// keep iterating remaining txids so independent counters still get their
	// best-effort increment + threshold callback on this attempt.
	thresholdGroups := make(map[string][]string) // callbackURL → threshold-reached txids
	for txid, entries := range registeredTxids {
		result, err := p.seenCounterStore.Increment(txid, subtreeID)
		if err != nil {
			p.Logger.Error("failed to increment seen counter", "txid", txid, "subtreeID", subtreeID, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("incrementing seen counter for %s: %w", txid, err)
			}
			continue
		}
		if result.ThresholdReached {
			for _, e := range entries {
				thresholdGroups[e.URL] = append(thresholdGroups[e.URL], txid)
			}
		}
	}

	// Emit one batched SEEN_MULTIPLE_NODES per callbackURL, chunked to fit broker limits.
	for callbackURL, txids := range thresholdGroups {
		for _, chunk := range chunkTxIDs(txids, callbackBatchChunkSize) {
			msg := &kafka.CallbackTopicMessage{
				CallbackURL:   callbackURL,
				CallbackToken: urlTokens[callbackURL],
				Type:          kafka.CallbackSeenMultipleNodes,
				TxIDs:         chunk,
			}
			data, err := msg.Encode()
			if err != nil {
				p.Logger.Error("failed to encode batched SEEN_MULTIPLE_NODES", "callbackURL", callbackURL, "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("encoding SEEN_MULTIPLE_NODES for %s: %w", callbackURL, err)
				}
				continue
			}
			if err := p.callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
				p.Logger.Error("failed to publish batched SEEN_MULTIPLE_NODES", "callbackURL", callbackURL, "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("publishing SEEN_MULTIPLE_NODES for %s: %w", callbackURL, err)
				}
			}
		}
	}

	return firstErr
}

// callbackBatchChunkSize caps txids per batched callback message so the JSON
// payload (~67 bytes per hex txid plus envelope) stays well under Kafka's
// default broker message.max.bytes of 1MB.
const callbackBatchChunkSize = 5000

func chunkTxIDs(txids []string, size int) [][]string {
	if len(txids) <= size {
		return [][]string{txids}
	}
	chunks := make([][]string, 0, (len(txids)+size-1)/size)
	for i := 0; i < len(txids); i += size {
		end := i + size
		if end > len(txids) {
			end = len(txids)
		}
		chunks = append(chunks, txids[i:end])
	}
	return chunks
}
