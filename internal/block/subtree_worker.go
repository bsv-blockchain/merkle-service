package block

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// SubtreeWorkerService consumes SubtreeWorkMessages from Kafka, processes each
// subtree (registration lookup, STUMP build, MINED callback publishing), writes
// STUMPs to the shared cache, and coordinates BLOCK_PROCESSED emission via an
// Aerospike subtree counter.
//
// On transient processing failure (Aerospike timeout, DataHub fetch failure,
// blob store hiccup) the work item is re-published to subtree-work with
// AttemptCount+1 instead of being silently dropped. The per-block subtree
// counter is decremented exactly once per work item — on successful processing
// or on DLQ hand-off — so BLOCK_PROCESSED still fires when retries exhaust,
// but a transient blip during a single block no longer leaves the bump-builder
// staring at an incomplete STUMP set.
type SubtreeWorkerService struct {
	service.BaseService

	kafkaCfg         config.KafkaConfig
	blockCfg         config.BlockConfig
	datahubCfg       config.DataHubConfig
	consumer         *kafka.Consumer
	callbackProducer *kafka.Producer
	retryProducer    *kafka.Producer // re-publishes to subtree-work on transient failure
	dlqProducer      *kafka.Producer // publishes to subtree-work-dlq when MaxAttempts is exceeded
	regStore         store.RegistrationStore
	subtreeStore     store.SubtreeStore
	stumpStore       store.StumpStore
	urlRegistry      store.CallbackURLRegistry
	subtreeCounter   store.SubtreeCounterStore
	expectedStumps   store.ExpectedStumpStore
	dataHubClient    *datahub.Client
	regCache         RegCache
	batchSem         chan struct{}
}

func NewSubtreeWorkerService(
	kafkaCfg config.KafkaConfig,
	blockCfg config.BlockConfig,
	datahubCfg config.DataHubConfig,
	regStore store.RegistrationStore,
	subtreeStore store.SubtreeStore,
	stumpStore store.StumpStore,
	urlRegistry store.CallbackURLRegistry,
	subtreeCounter store.SubtreeCounterStore,
	expectedStumps store.ExpectedStumpStore,
	logger *slog.Logger,
) *SubtreeWorkerService {
	s := &SubtreeWorkerService{
		kafkaCfg:       kafkaCfg,
		blockCfg:       blockCfg,
		datahubCfg:     datahubCfg,
		regStore:       regStore,
		subtreeStore:   subtreeStore,
		stumpStore:     stumpStore,
		urlRegistry:    urlRegistry,
		subtreeCounter: subtreeCounter,
		expectedStumps: expectedStumps,
	}
	s.InitBase("subtree-worker")
	if logger != nil {
		s.Logger = logger
	}
	return s
}

func (s *SubtreeWorkerService) Init(_ interface{}) error {
	s.dataHubClient = datahub.NewClientWithSSRFGuard(
		s.datahubCfg.TimeoutSec,
		s.datahubCfg.MaxRetries,
		s.datahubCfg.MaxBlockBytes,
		s.datahubCfg.MaxSubtreeBytes,
		s.datahubCfg.AllowPrivateIPs,
		s.Logger,
	)
	s.dataHubClient.SetPeerHealth(datahub.NewPeerHealth(
		s.datahubCfg.PeerHealth.FailureThreshold,
		time.Duration(s.datahubCfg.PeerHealth.CooldownSec)*time.Second,
	))

	// Initialize block-time registration cache. A miss falls through to
	// Aerospike, so a cache failure is not fatal — log and proceed.
	if s.blockCfg.RegCacheMaxMB > 0 {
		regCache, err := cache.NewRegistrationCache(s.blockCfg.RegCacheMaxMB, s.Logger)
		if err != nil {
			s.Logger.Warn(
				"failed to create block registration cache, proceeding without",
				"error", err,
				"maxMB", s.blockCfg.RegCacheMaxMB,
			)
		} else {
			s.regCache = regCache
		}
	}

	// Bound concurrent BatchGets so a single block fanning out 14+ subtrees
	// can't exhaust the Aerospike connection pool. <=0 disables the gate.
	if s.blockCfg.BatchGetConcurrency > 0 {
		s.batchSem = make(chan struct{}, s.blockCfg.BatchGetConcurrency)
	}

	callbackProducer, err := kafka.NewProducer(
		s.kafkaCfg.Brokers,
		s.kafkaCfg.CallbackTopic,
		s.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback producer: %w", err)
	}
	s.callbackProducer = callbackProducer

	retryProducer, err := kafka.NewProducer(
		s.kafkaCfg.Brokers,
		s.kafkaCfg.SubtreeWorkTopic,
		s.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree-work retry producer: %w", err)
	}
	s.retryProducer = retryProducer

	dlqTopic := s.kafkaCfg.SubtreeWorkDLQTopic
	if dlqTopic == "" {
		dlqTopic = "subtree-work-dlq"
	}
	dlqProducer, err := kafka.NewProducer(
		s.kafkaCfg.Brokers,
		dlqTopic,
		s.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree-work DLQ producer: %w", err)
	}
	s.dlqProducer = dlqProducer

	consumer, err := kafka.NewConsumer(
		s.kafkaCfg.Brokers,
		s.kafkaCfg.ConsumerGroup+"-subtree-worker",
		[]string{s.kafkaCfg.SubtreeWorkTopic},
		s.handleMessage,
		s.kafkaCfg.TopicPartitions(),
		s.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree-work consumer: %w", err)
	}
	s.consumer = consumer

	s.Logger.Info(
		"subtree worker service initialized",
		"subtreeWorkTopic", s.kafkaCfg.SubtreeWorkTopic,
		"subtreeWorkDLQTopic", dlqTopic,
		"maxAttempts", s.maxAttempts(),
		"regCacheEnabled", s.regCache != nil,
		"regCacheMaxMB", s.blockCfg.RegCacheMaxMB,
		"batchGetConcurrency", s.blockCfg.BatchGetConcurrency,
	)
	return nil
}

func (s *SubtreeWorkerService) Start(ctx context.Context) error {
	s.Logger.Info("starting subtree worker service")
	s.SetStarted(true)
	return s.consumer.Start(ctx)
}

func (s *SubtreeWorkerService) Stop() error {
	s.Logger.Info("stopping subtree worker service")
	s.SetStarted(false)
	var firstErr error
	if s.consumer != nil {
		if err := s.consumer.Stop(); err != nil {
			firstErr = err
		}
	}
	if s.callbackProducer != nil {
		if err := s.callbackProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.retryProducer != nil {
		if err := s.retryProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.dlqProducer != nil {
		if err := s.dlqProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *SubtreeWorkerService) Health() service.HealthStatus {
	status := "healthy"
	if !s.IsStarted() {
		status = "unhealthy"
	}
	return service.HealthStatus{
		Name:   "subtree-worker",
		Status: status,
	}
}

func (s *SubtreeWorkerService) maxAttempts() int {
	if s.blockCfg.MaxAttempts > 0 {
		return s.blockCfg.MaxAttempts
	}
	return 10
}

// handleMessage consumes a single SubtreeWorkMessage.
//
// Correctness contract: the work item is ack'd (return nil) and the per-block
// subtree counter is decremented ONLY after either (a) processing succeeded
// with no callbacks to publish, or (b) every STUMP callback has been durably
// stored and published to Kafka. A failure during processing OR during
// callback publishing routes the work back through handleTransientFailure,
// which re-publishes for retry (preserving the counter) or routes to DLQ at
// max attempts (decrementing the counter so BLOCK_PROCESSED can still fire).
// This prevents the silent drop where a Kafka/blob-store hiccup left the
// downstream consumer waiting for STUMPs that never arrive (F-012).
//
// A failure of the counter Decrement itself (Aerospike/SQL transient hiccup)
// is propagated to the consumer rather than being swallowed (F-013): on the
// success path we route through handleTransientFailure so the work item is
// retried and the next attempt can re-try the decrement; on the DLQ path we
// return the error so the consumer redelivers and we eventually emit
// BLOCK_PROCESSED once the counter store recovers. Without this, a transient
// Decrement failure would silently leave the per-block counter > 0 forever,
// arcade waiting for a BLOCK_PROCESSED that never fires.
//
// A failure of the BLOCK_PROCESSED publish itself (callback-topic Kafka
// outage) is also propagated rather than swallowed (F-014). The counter has
// already been decremented to 0 at this point; the work item is re-driven
// through handleTransientFailure so the consumer retries. On redelivery the
// counter goes negative; we treat remaining<=0 as "still need to emit" and
// re-publish BLOCK_PROCESSED. Receiver-side dedup at the delivery service
// (keyed by blockHash + callbackURL + type) ensures the registered endpoint
// sees BLOCK_PROCESSED at most once per (block, URL) pair.
func (s *SubtreeWorkerService) handleMessage(ctx context.Context, msg *kafka.Message) error {
	workMsg, err := kafka.DecodeSubtreeWorkMessage(msg.Value)
	if err != nil {
		s.Logger.Error(
			"failed to decode subtree work message, dropping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		// Return nil so the consumer marks the offset; a malformed payload at
		// the head of the partition cannot be recovered by re-driving.
		return nil
	}

	s.Logger.Debug(
		"processing subtree work item",
		"subtreeHash", workMsg.SubtreeHash,
		"blockHash", workMsg.BlockHash,
		"blockHeight", workMsg.BlockHeight,
		"attemptCount", workMsg.AttemptCount,
	)

	result, err := ProcessBlockSubtree(
		ctx,
		workMsg.SubtreeHash,
		uint64(workMsg.BlockHeight),
		workMsg.BlockHash,
		workMsg.DataHubURL,
		s.dataHubClient,
		s.subtreeStore,
		s.regStore,
		s.regCache,
		s.batchSem,
		s.blockCfg.PostMineTTLSec,
		workMsg.OverrideCallbackURL,
		workMsg.OverrideCallbackToken,
		s.Logger,
	)
	if err != nil {
		// Transient processing failure: re-drive instead of silently dropping
		// the STUMP. Counter is decremented only on DLQ (terminal) — for
		// retries, the next successful attempt will decrement.
		return s.handleTransientFailure(workMsg, err)
	}

	// Publish one STUMP callback per (callbackURL, subtree) combination.
	// A failure here (blob-store write, encode, or Kafka publish) must NOT be
	// silently swallowed: route through the same retry/DLQ pipeline as a
	// processing failure so the work item is either retried or terminally
	// DLQ'd. Otherwise downstream consumers stall waiting for STUMPs that
	// were dropped on the floor.
	if result != nil && len(result.CallbackGroups) > 0 {
		if pubErr := s.publishSubtreeCallbacks(workMsg, result); pubErr != nil {
			return s.handleTransientFailure(workMsg, pubErr)
		}
		// Record this subtree's index into each matched URL's expected-STUMP set
		// BEFORE decrementing the counter, so the set is complete when the last
		// subtree drains the counter and BLOCK_PROCESSED reads it. Reliable, not
		// best-effort: an under-counted set would let arcade under-expect STUMPs
		// and silently miss one — so a failure re-drives the (idempotent) work
		// item, exactly like the counter decrement below.
		if s.expectedStumps != nil {
			urls := make([]string, 0, len(result.CallbackGroups))
			for callbackURL := range result.CallbackGroups {
				urls = append(urls, callbackURL)
			}
			if recErr := s.expectedStumps.AddSubtreeIndex(workMsg.BlockHash, workMsg.SubtreeIndex, urls); recErr != nil {
				return s.handleTransientFailure(workMsg, fmt.Errorf("recording expected-STUMP indices: %w", recErr))
			}
		}
	}

	// Successful processing — decrement the per-block counter. If the
	// counter store fails transiently here, route through the retry pipeline
	// so the work item is redelivered and the decrement is re-attempted; we
	// must not ack with a non-decremented counter, or BLOCK_PROCESSED will
	// never fire for this block (F-013).
	if err := s.decrementCounterAndMaybeEmit(workMsg.BlockHash, workMsg.OverrideCallbackURL, workMsg.OverrideCallbackToken); err != nil {
		return s.handleTransientFailure(workMsg, fmt.Errorf("decrementing subtree counter: %w", err))
	}
	return nil
}

// handleTransientFailure either re-publishes the work item to subtree-work for
// retry or, once max attempts is reached, parks it on subtree-work-dlq and
// decrements the counter so BLOCK_PROCESSED can still fire (with a missing
// STUMP that arcade will surface as a BUMP build error rather than silent loss).
//
// On the DLQ branch the counter is decremented BEFORE the DLQ publish. If the
// decrement fails (counter-store transient hiccup), we return the error so the
// consumer redelivers the work item — and crucially the DLQ publish has not
// happened yet, so redelivery does NOT accumulate duplicate DLQ entries while
// the counter store is degraded (F-013). If the DLQ publish fails after a
// successful decrement, redelivery will re-decrement (going negative) and
// re-attempt the DLQ publish; the negative-decrement re-emits BLOCK_PROCESSED
// which is deduplicated by the receiver, while the DLQ publish either
// eventually succeeds or surfaces as a sustained loud-error log.
func (s *SubtreeWorkerService) handleTransientFailure(workMsg *kafka.SubtreeWorkMessage, cause error) error {
	nextAttempt := workMsg.AttemptCount + 1
	maxAttempts := s.maxAttempts()

	if nextAttempt >= maxAttempts {
		s.Logger.Error(
			"subtree work item exceeded max attempts, routing to DLQ",
			"subtreeHash", workMsg.SubtreeHash,
			"blockHash", workMsg.BlockHash,
			"subtreeIndex", workMsg.SubtreeIndex,
			"attemptCount", workMsg.AttemptCount,
			"maxAttempts", maxAttempts,
			"error", cause,
		)
		// Decrement FIRST so a counter-store hiccup aborts before we publish to
		// the DLQ — otherwise every redelivery while the counter store is
		// degraded would publish another DLQ duplicate.
		if decErr := s.decrementCounterAndMaybeEmit(workMsg.BlockHash, workMsg.OverrideCallbackURL, workMsg.OverrideCallbackToken); decErr != nil {
			s.Logger.Error(
				"ALERT: subtree counter decrement failed on DLQ path; deferring DLQ publish until counter store recovers",
				"subtreeHash", workMsg.SubtreeHash,
				"blockHash", workMsg.BlockHash,
				"subtreeIndex", workMsg.SubtreeIndex,
				"error", decErr,
			)
			return fmt.Errorf("decrementing subtree counter on DLQ path for block %s: %w",
				workMsg.BlockHash, decErr)
		}
		workMsg.AttemptCount = nextAttempt
		data, encErr := workMsg.Encode()
		if encErr != nil {
			// Encoding our own struct really shouldn't fail — return the
			// error so the consumer doesn't ack and we get a chance on the
			// next session.
			return fmt.Errorf("encoding subtree work message for DLQ: %w", encErr)
		}
		// PublishWithHashKey to keep the worker's subtree-hash keying uniform with
		// the retry/fan-out paths (SHA256-derived). The DLQ is its own topic so
		// this is cosmetic for co-location, but it avoids a confusing mixed scheme.
		if pubErr := s.dlqProducer.PublishWithHashKey(workMsg.SubtreeHash, data); pubErr != nil {
			return fmt.Errorf("publishing subtree work message to DLQ: %w", pubErr)
		}
		return nil
	}

	s.Logger.Warn(
		"subtree work item transient failure, re-publishing for retry",
		"subtreeHash", workMsg.SubtreeHash,
		"blockHash", workMsg.BlockHash,
		"subtreeIndex", workMsg.SubtreeIndex,
		"attemptCount", workMsg.AttemptCount,
		"nextAttempt", nextAttempt,
		"error", cause,
	)
	workMsg.AttemptCount = nextAttempt
	data, encErr := workMsg.Encode()
	if encErr != nil {
		return fmt.Errorf("encoding subtree work message for retry: %w", encErr)
	}
	// PublishWithHashKey (not Publish) so the retried item keys IDENTICALLY to the
	// block-processor's fan-out, which publishes subtree-work via HashBatchEntry
	// (a SHA256-derived key). A raw key here would be hashed differently by the
	// broker and land the retry on a DIFFERENT partition than its original —
	// harmless for these independent units, but it breaks same-subtree partition
	// stability. Keep the keying consistent across fan-out and retry.
	if pubErr := s.retryProducer.PublishWithHashKey(workMsg.SubtreeHash, data); pubErr != nil {
		return fmt.Errorf("re-publishing subtree work message for retry: %w", pubErr)
	}
	// Intentionally do NOT decrement on retry — only success or DLQ counts.
	return nil
}

// decrementCounterAndMaybeEmit drives the per-block subtree counter and emits
// BLOCK_PROCESSED when the last subtree finishes.
//
// Returns a non-nil error if either the underlying counter store's Decrement
// fails (F-013) or if BLOCK_PROCESSED emission fails (F-014). The error MUST
// be propagated by callers so the work item is redelivered:
//   - F-013: a Decrement failure left the counter > 0 forever and
//     BLOCK_PROCESSED never emitted for the affected block.
//   - F-014: a callback-topic publish failure during emit silently dropped
//     BLOCK_PROCESSED — the counter had already hit 0, the work item was
//     ack'd, and the registered callback endpoint never got the notification.
//
// Note: when emit fails on the success path, the counter has already been
// decremented to 0. On redelivery the counter will go to -1 and emit will
// fire again (we treat remaining<=0 as "last subtree, emit now"). Duplicate
// BLOCK_PROCESSED messages are deduplicated at the callback delivery service
// (keyed by blockHash + callbackURL + BLOCK_PROCESSED), so the receiver sees
// at most one BLOCK_PROCESSED per (block, callbackURL) pair.
//
// If no counter store is configured (test/dry-run), this is a no-op.
//
// overrideURL/overrideToken are propagated from a /reprocess request. When
// overrideURL is non-empty, the counter is keyed by (blockHash|overrideURL)
// so reprocess and live processing don't share state, and the
// BLOCK_PROCESSED emission targets only that one URL/token.
func (s *SubtreeWorkerService) decrementCounterAndMaybeEmit(blockHash, overrideURL, overrideToken string) error {
	if s.subtreeCounter == nil {
		return nil
	}
	counterKey := SubtreeCounterKey(blockHash, overrideURL)
	remaining, blockData, err := s.subtreeCounter.Decrement(counterKey)
	if err != nil {
		if errors.Is(err, store.ErrCounterNotFound) {
			// The per-block counter is gone (TTL expired before the block
			// finished). A worker cannot recreate it, so retrying this work
			// item is futile — that futile retry, repeated across every
			// remaining subtree, was the unbounded subtree-work republish
			// loop. Ack the item (return nil) and emit a loud ALERT: the
			// block must be reprocessed to rebuild its counter and re-emit
			// BLOCK_PROCESSED. The subtree's STUMP/MINED callbacks have
			// already been published by this point, so only the
			// BLOCK_PROCESSED coordination is lost.
			s.Logger.Error(
				"ALERT: subtree counter missing (TTL expired); acking work item without retry — block must be reprocessed",
				"blockHash", blockHash,
				"counterKey", counterKey,
			)
			return nil
		}
		s.Logger.Error(
			"failed to decrement subtree counter",
			"blockHash", blockHash,
			"counterKey", counterKey,
			"error", err,
		)
		return err
	}
	// remaining<=0 covers both the normal "last subtree" case (==0) and the
	// retry-after-emit-failure case (<0): if a previous attempt decremented to
	// 0 but failed to publish BLOCK_PROCESSED, the redelivered work item will
	// drive the counter negative; we still need to emit so the notification is
	// not silently lost. Receiver-side dedup handles the duplicate.
	if remaining <= 0 {
		if emitErr := s.emitBlockProcessed(blockHash, overrideURL, overrideToken, blockData); emitErr != nil {
			s.Logger.Error(
				"failed to emit BLOCK_PROCESSED; work item will be redelivered",
				"blockHash", blockHash,
				"remaining", remaining,
				"error", emitErr,
			)
			return fmt.Errorf("emitting BLOCK_PROCESSED for block %s: %w", blockHash, emitErr)
		}
	}
	return nil
}

// publishSubtreeCallbacks publishes one CallbackTopicMessage per callbackURL per subtree.
// The STUMP bytes are written once to the blob store (content-addressed, so the
// same blob is reused across every callback URL for this subtree), and each
// Kafka message carries only the reference.
//
// Returns a non-nil error if the blob-store write fails OR if any per-URL
// encode/publish fails. The loop continues past a single per-URL failure so
// independent callbacks still go out (partial-success), but the first error
// encountered is returned to the caller so handleMessage can re-drive the
// work item through handleTransientFailure rather than silently acking and
// decrementing the counter — see F-012.
func (s *SubtreeWorkerService) publishSubtreeCallbacks(workMsg *kafka.SubtreeWorkMessage, result *SubtreeResult) error {
	if s.stumpStore == nil {
		s.Logger.Error(
			"stump store not configured; cannot publish STUMP callbacks",
			"blockHash", workMsg.BlockHash,
			"subtreeIndex", workMsg.SubtreeIndex,
		)
		return fmt.Errorf("stump store not configured for block %s subtree %d",
			workMsg.BlockHash, workMsg.SubtreeIndex)
	}

	stumpRef, err := s.stumpStore.Put(result.StumpData, uint64(workMsg.BlockHeight))
	if err != nil {
		// Without a ref, downstream delivery can't fetch the STUMP — skip this
		// subtree's callbacks entirely rather than publishing broken messages.
		s.Logger.Error(
			"failed to store STUMP blob; skipping subtree callbacks",
			"blockHash", workMsg.BlockHash,
			"subtreeIndex", workMsg.SubtreeIndex,
			"callbackURLs", len(result.CallbackGroups),
			"error", err,
		)
		return fmt.Errorf("storing STUMP blob for block %s subtree %d: %w",
			workMsg.BlockHash, workMsg.SubtreeIndex, err)
	}

	// Track the first error so the caller can re-drive the whole work item,
	// while still encoding the remaining URLs (each callback target is
	// independent — an encode hiccup on one shouldn't deny delivery to the
	// others on this attempt). Valid messages go out in ONE batch publish
	// (throughput review F-6) instead of one broker-acked RTT per URL; a batch
	// error re-drives the work item, and the delivery-side dedup absorbs any
	// records that did land.
	var firstErr error
	entries := make([]kafka.BatchEntry, 0, len(result.CallbackGroups))
	for callbackURL := range result.CallbackGroups {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL:   callbackURL,
			CallbackToken: result.CallbackTokens[callbackURL],
			Type:          kafka.CallbackStump,
			BlockHash:     workMsg.BlockHash,
			SubtreeHash:   workMsg.SubtreeHash,
			SubtreeIndex:  workMsg.SubtreeIndex,
			StumpRef:      stumpRef,
		}
		data, encErr := msg.Encode()
		if encErr != nil {
			s.Logger.Error("failed to encode STUMP callback message",
				"callbackURL", callbackURL, "error", encErr)
			if firstErr == nil {
				firstErr = fmt.Errorf("encoding STUMP callback for %s: %w", callbackURL, encErr)
			}
			continue
		}
		entries = append(entries, kafka.HashBatchEntry(msg.PartitionKey(), data))
	}
	if pubErr := s.callbackProducer.PublishBatch(entries); pubErr != nil {
		s.Logger.Error("failed to publish STUMP callback batch",
			"count", len(entries), "error", pubErr)
		if firstErr == nil {
			firstErr = fmt.Errorf("publishing STUMP callback batch (%d URLs): %w", len(entries), pubErr)
		}
	}
	return firstErr
}

// emitBlockProcessed publishes a BLOCK_PROCESSED message to every registered
// callback URL.
//
// Returns a non-nil error if the URL-registry lookup fails OR if any per-URL
// encode/publish fails. The loop continues past a single per-URL failure so
// independent callbacks still go out (partial-success), but the first error
// encountered is returned to the caller so decrementCounterAndMaybeEmit can
// propagate it back through handleMessage → handleTransientFailure rather
// than silently swallowing it. Without this, a transient callback-topic
// outage when the LAST subtree's decrement-to-zero triggers emit would
// permanently drop BLOCK_PROCESSED — the counter has already reached zero,
// the work item is ack'd, and the registered endpoint never receives the
// notification (F-014).
//
// On retry, the redelivered work item drives the counter past zero and
// emit fires again; duplicate BLOCK_PROCESSED messages are deduplicated at
// the callback delivery service (keyed by blockHash + callbackURL + type).
func (s *SubtreeWorkerService) emitBlockProcessed(blockHash, overrideURL, overrideToken string, blockData *store.BlockProcessedData) error {
	return emitBlockProcessedCallbacks(s.Logger, s.urlRegistry, s.expectedStumps, s.callbackProducer, blockHash, overrideURL, overrideToken, blockData)
}

// callbackPublisher is the narrow surface emitBlockProcessedCallbacks needs.
// Implemented by *kafka.Producer in production; mocked in tests.
type callbackPublisher interface {
	PublishBatch(entries []kafka.BatchEntry) error
}

// emitBlockProcessedCallbacks publishes BLOCK_PROCESSED messages to every
// callback URL that should learn about the block.
//
// When overrideURL is non-empty (the /reprocess path), exactly one
// message is published to that URL with overrideToken — the global
// callback URL registry is NOT consulted, so other arcades never see
// this past block via the reprocess flow. Otherwise the message is
// fanned out to every URL in urlRegistry.
//
// Used by:
//   - SubtreeWorkerService when the per-block subtree counter decrements
//     to zero (the normal path for blocks with subtrees).
//   - Block-processor's handleMessage when a block has zero subtrees
//     (coinbase-only blocks). Without this path arcade would never get
//     a BLOCK_PROCESSED callback for empty blocks and would retry
//     /reprocess indefinitely.
//
// Returns the first per-URL encode/publish error encountered so the
// caller can re-drive the originating work item; later URLs in the same
// call still get a best-effort delivery attempt before the error is
// returned. Duplicates created by retries are deduplicated downstream
// at the callback delivery service (keyed by blockHash + callbackURL +
// type).
func emitBlockProcessedCallbacks(
	logger *slog.Logger,
	urlRegistry store.CallbackURLRegistry,
	expectedStumps store.ExpectedStumpStore,
	producer callbackPublisher,
	blockHash, overrideURL, overrideToken string,
	blockData *store.BlockProcessedData,
) error {
	if producer == nil {
		return nil
	}

	var entries []store.CallbackEntry
	if overrideURL != "" {
		entries = []store.CallbackEntry{{URL: overrideURL, Token: overrideToken}}
	} else {
		if urlRegistry == nil {
			return nil
		}
		var err error
		entries, err = urlRegistry.GetAll()
		if err != nil {
			logger.Error("failed to get callback URLs for BLOCK_PROCESSED", "error", err)
			return fmt.Errorf("getting callback URLs for BLOCK_PROCESSED on block %s: %w", blockHash, err)
		}
	}
	if len(entries) == 0 {
		return nil
	}

	var firstErr error
	batch := make([]kafka.BatchEntry, 0, len(entries))
	for _, entry := range entries {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL:   entry.URL,
			CallbackToken: entry.Token,
			Type:          kafka.CallbackBlockProcessed,
			BlockHash:     blockHash,
		}
		// Attach the block-level enrichment (merkle root, subtree list, coinbase
		// BUMP) when the producer captured it. Absent for blocks processed
		// before this shipped, or when the counter record lost the data — the
		// consumer then falls back to a datahub.
		if blockData != nil {
			msg.MerkleRoot = blockData.MerkleRoot
			msg.SubtreeCount = &blockData.SubtreeCount
			msg.SubtreeHashes = blockData.SubtreeHashes
			msg.CoinbaseBUMP = blockData.CoinbaseBUMP
		}
		// Attach the set of subtree indices that produced a STUMP for this URL so
		// the receiver can detect a missing one. A read failure must not ship a
		// BLOCK_PROCESSED without its expected set (that would silently disable
		// detection for the block), so skip this URL and surface the error — the
		// caller re-drives and the downstream dedup absorbs any duplicate.
		if expectedStumps != nil {
			indices, idxErr := expectedStumps.GetSubtreeIndices(blockHash, entry.URL)
			if idxErr != nil {
				logger.Error("failed to read expected-STUMP indices for BLOCK_PROCESSED",
					"blockHash", blockHash, "callbackURL", entry.URL, "error", idxErr)
				if firstErr == nil {
					firstErr = fmt.Errorf("reading expected-STUMP indices for %s: %w", entry.URL, idxErr)
				}
				continue
			}
			msg.ExpectedSubtreeIndices = indices
		}
		data, encErr := msg.Encode()
		if encErr != nil {
			logger.Error(
				"failed to encode BLOCK_PROCESSED message",
				"callbackURL", entry.URL,
				"error", encErr,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("encoding BLOCK_PROCESSED for %s: %w", entry.URL, encErr)
			}
			continue
		}
		batch = append(batch, kafka.HashBatchEntry(msg.PartitionKey(), data))
	}
	// One batch publish for every URL (throughput review F-6). On error the
	// caller re-drives via the counter/redelivery path and the delivery-side
	// dedup (blockHash + callbackURL + type) absorbs any records that landed.
	if pubErr := producer.PublishBatch(batch); pubErr != nil {
		logger.Error(
			"failed to publish BLOCK_PROCESSED batch",
			"blockHash", blockHash,
			"count", len(batch),
			"error", pubErr,
		)
		if firstErr == nil {
			firstErr = fmt.Errorf("publishing BLOCK_PROCESSED batch for block %s (%d URLs): %w", blockHash, len(batch), pubErr)
		}
	}

	if firstErr == nil {
		logger.Info(
			"emitted BLOCK_PROCESSED callbacks",
			"blockHash", blockHash,
			"callbackURLs", len(entries),
		)
	}
	return firstErr
}
