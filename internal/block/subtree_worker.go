package block

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"

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
	}
	s.InitBase("subtree-worker")
	if logger != nil {
		s.Logger = logger
	}
	return s
}

func (s *SubtreeWorkerService) Init(_ interface{}) error {
	s.dataHubClient = datahub.NewClientWithCaps(
		s.datahubCfg.TimeoutSec,
		s.datahubCfg.MaxRetries,
		s.datahubCfg.MaxBlockBytes,
		s.datahubCfg.MaxSubtreeBytes,
		s.Logger,
	)

	// Initialize block-time registration cache. A miss falls through to
	// Aerospike, so a cache failure is not fatal — log and proceed.
	if s.blockCfg.RegCacheMaxMB > 0 {
		regCache, err := cache.NewRegistrationCache(s.blockCfg.RegCacheMaxMB, s.Logger)
		if err != nil {
			s.Logger.Warn("failed to create block registration cache, proceeding without",
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
		s.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree-work consumer: %w", err)
	}
	s.consumer = consumer

	s.Logger.Info("subtree worker service initialized",
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
func (s *SubtreeWorkerService) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	workMsg, err := kafka.DecodeSubtreeWorkMessage(msg.Value)
	if err != nil {
		s.Logger.Error("failed to decode subtree work message, dropping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		// Return nil so the consumer marks the offset; a malformed payload at
		// the head of the partition cannot be recovered by re-driving.
		return nil
	}

	s.Logger.Debug("processing subtree work item",
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
	}

	// Successful processing — decrement the per-block counter. If the
	// counter store fails transiently here, route through the retry pipeline
	// so the work item is redelivered and the decrement is re-attempted; we
	// must not ack with a non-decremented counter, or BLOCK_PROCESSED will
	// never fire for this block (F-013).
	if err := s.decrementCounterAndMaybeEmit(workMsg.BlockHash); err != nil {
		return s.handleTransientFailure(workMsg, fmt.Errorf("decrementing subtree counter: %w", err))
	}
	return nil
}

// handleTransientFailure either re-publishes the work item to subtree-work for
// retry or, once max attempts is reached, parks it on subtree-work-dlq and
// decrements the counter so BLOCK_PROCESSED can still fire (with a missing
// STUMP that arcade will surface as a BUMP build error rather than silent loss).
//
// If the counter Decrement on the DLQ path fails, the error is returned to the
// caller so the consumer redelivers the work item rather than silently acking
// with a non-decremented counter (F-013). The work has already been published
// to the DLQ, so on redelivery the next attempt will see AttemptCount past
// max and DLQ-publish again — that is acceptable until the counter store
// recovers and Decrement succeeds; BLOCK_PROCESSED is delayed but not
// silently lost. Operators should treat repeated DLQ-publish-then-Decrement
// failures as alert-worthy (the loud Error-level log below).
func (s *SubtreeWorkerService) handleTransientFailure(workMsg *kafka.SubtreeWorkMessage, cause error) error {
	nextAttempt := workMsg.AttemptCount + 1
	maxAttempts := s.maxAttempts()

	if nextAttempt >= maxAttempts {
		s.Logger.Error("subtree work item exceeded max attempts, routing to DLQ",
			"subtreeHash", workMsg.SubtreeHash,
			"blockHash", workMsg.BlockHash,
			"subtreeIndex", workMsg.SubtreeIndex,
			"attemptCount", workMsg.AttemptCount,
			"maxAttempts", maxAttempts,
			"error", cause,
		)
		workMsg.AttemptCount = nextAttempt
		data, encErr := workMsg.Encode()
		if encErr != nil {
			// Encoding our own struct really shouldn't fail — return the
			// error so the consumer doesn't ack and we get a chance on the
			// next session.
			return fmt.Errorf("encoding subtree work message for DLQ: %w", encErr)
		}
		if pubErr := s.dlqProducer.Publish(workMsg.SubtreeHash, data); pubErr != nil {
			return fmt.Errorf("publishing subtree work message to DLQ: %w", pubErr)
		}
		// Counter decrement is required here: the work item is terminally
		// failed but we still need BLOCK_PROCESSED to fire so arcade isn't
		// stuck waiting for a STUMP that will never arrive. If the decrement
		// itself fails (counter-store transient hiccup), surface the error so
		// the consumer redelivers and we re-attempt; the DLQ publish above
		// will repeat, but that's preferable to silently acking with the
		// counter still > 0 and BLOCK_PROCESSED never firing (F-013).
		if decErr := s.decrementCounterAndMaybeEmit(workMsg.BlockHash); decErr != nil {
			s.Logger.Error("ALERT: subtree counter decrement failed on DLQ path; BLOCK_PROCESSED delayed until counter store recovers",
				"subtreeHash", workMsg.SubtreeHash,
				"blockHash", workMsg.BlockHash,
				"subtreeIndex", workMsg.SubtreeIndex,
				"error", decErr,
			)
			return fmt.Errorf("decrementing subtree counter on DLQ path for block %s: %w",
				workMsg.BlockHash, decErr)
		}
		return nil
	}

	s.Logger.Warn("subtree work item transient failure, re-publishing for retry",
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
	if pubErr := s.retryProducer.Publish(workMsg.SubtreeHash, data); pubErr != nil {
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
func (s *SubtreeWorkerService) decrementCounterAndMaybeEmit(blockHash string) error {
	if s.subtreeCounter == nil {
		return nil
	}
	remaining, err := s.subtreeCounter.Decrement(blockHash)
	if err != nil {
		s.Logger.Error("failed to decrement subtree counter",
			"blockHash", blockHash,
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
		if emitErr := s.emitBlockProcessed(blockHash); emitErr != nil {
			s.Logger.Error("failed to emit BLOCK_PROCESSED; work item will be redelivered",
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
		s.Logger.Error("stump store not configured; cannot publish STUMP callbacks",
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
		s.Logger.Error("failed to store STUMP blob; skipping subtree callbacks",
			"blockHash", workMsg.BlockHash,
			"subtreeIndex", workMsg.SubtreeIndex,
			"callbackURLs", len(result.CallbackGroups),
			"error", err,
		)
		return fmt.Errorf("storing STUMP blob for block %s subtree %d: %w",
			workMsg.BlockHash, workMsg.SubtreeIndex, err)
	}

	// Track the first error so the caller can re-drive the whole work item,
	// while still attempting the remaining URLs (each callback target is
	// independent — a hiccup on one shouldn't deny delivery to the others on
	// this attempt).
	var firstErr error
	for callbackURL := range result.CallbackGroups {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL:  callbackURL,
			Type:         kafka.CallbackStump,
			BlockHash:    workMsg.BlockHash,
			SubtreeIndex: workMsg.SubtreeIndex,
			StumpRef:     stumpRef,
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
		if pubErr := s.callbackProducer.PublishWithHashKey(callbackURL, data); pubErr != nil {
			s.Logger.Error("failed to publish STUMP callback",
				"callbackURL", callbackURL, "error", pubErr)
			if firstErr == nil {
				firstErr = fmt.Errorf("publishing STUMP callback for %s: %w", callbackURL, pubErr)
			}
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
func (s *SubtreeWorkerService) emitBlockProcessed(blockHash string) error {
	if s.urlRegistry == nil {
		return nil
	}

	urls, err := s.urlRegistry.GetAll()
	if err != nil {
		s.Logger.Error("failed to get callback URLs for BLOCK_PROCESSED", "error", err)
		return fmt.Errorf("getting callback URLs for BLOCK_PROCESSED on block %s: %w", blockHash, err)
	}
	if len(urls) == 0 {
		return nil
	}

	// Track the first error so the caller can re-drive the work item, while
	// still attempting the remaining URLs (each callback target is
	// independent — a hiccup on one shouldn't deny delivery to the others on
	// this attempt). Matches publishSubtreeCallbacks's partial-success
	// pattern from PR #77.
	var firstErr error
	for _, callbackURL := range urls {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL: callbackURL,
			Type:        kafka.CallbackBlockProcessed,
			BlockHash:   blockHash,
		}
		data, encErr := msg.Encode()
		if encErr != nil {
			s.Logger.Error("failed to encode BLOCK_PROCESSED message",
				"callbackURL", callbackURL,
				"error", encErr,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("encoding BLOCK_PROCESSED for %s: %w", callbackURL, encErr)
			}
			continue
		}
		if pubErr := s.callbackProducer.PublishWithHashKey(callbackURL, data); pubErr != nil {
			s.Logger.Error("failed to publish BLOCK_PROCESSED callback",
				"callbackURL", callbackURL,
				"error", pubErr,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("publishing BLOCK_PROCESSED for %s: %w", callbackURL, pubErr)
			}
		}
	}

	if firstErr == nil {
		s.Logger.Info("emitted BLOCK_PROCESSED callbacks",
			"blockHash", blockHash,
			"callbackURLs", len(urls),
		)
	}
	return firstErr
}
