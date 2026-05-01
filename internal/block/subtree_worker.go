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
	s.dataHubClient = datahub.NewClient(s.datahubCfg.TimeoutSec, s.datahubCfg.MaxRetries, s.Logger)

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
	if result != nil && len(result.CallbackGroups) > 0 {
		s.publishSubtreeCallbacks(workMsg, result)
	}

	// Successful processing — decrement the per-block counter.
	s.decrementCounterAndMaybeEmit(workMsg.BlockHash)
	return nil
}

// handleTransientFailure either re-publishes the work item to subtree-work for
// retry or, once max attempts is reached, parks it on subtree-work-dlq and
// decrements the counter so BLOCK_PROCESSED can still fire (with a missing
// STUMP that arcade will surface as a BUMP build error rather than silent loss).
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
		// stuck waiting for a STUMP that will never arrive.
		s.decrementCounterAndMaybeEmit(workMsg.BlockHash)
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
func (s *SubtreeWorkerService) decrementCounterAndMaybeEmit(blockHash string) {
	if s.subtreeCounter == nil {
		return
	}
	remaining, err := s.subtreeCounter.Decrement(blockHash)
	if err != nil {
		s.Logger.Error("failed to decrement subtree counter",
			"blockHash", blockHash,
			"error", err,
		)
		return
	}
	if remaining == 0 {
		s.emitBlockProcessed(blockHash)
	}
}

// publishSubtreeCallbacks publishes one CallbackTopicMessage per callbackURL per subtree.
// The STUMP bytes are written once to the blob store (content-addressed, so the
// same blob is reused across every callback URL for this subtree), and each
// Kafka message carries only the reference.
func (s *SubtreeWorkerService) publishSubtreeCallbacks(workMsg *kafka.SubtreeWorkMessage, result *SubtreeResult) {
	if s.stumpStore == nil {
		s.Logger.Error("stump store not configured; cannot publish STUMP callbacks",
			"blockHash", workMsg.BlockHash,
			"subtreeIndex", workMsg.SubtreeIndex,
		)
		return
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
		return
	}

	for callbackURL := range result.CallbackGroups {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL:  callbackURL,
			Type:         kafka.CallbackStump,
			BlockHash:    workMsg.BlockHash,
			SubtreeIndex: workMsg.SubtreeIndex,
			StumpRef:     stumpRef,
		}
		data, err := msg.Encode()
		if err != nil {
			s.Logger.Error("failed to encode STUMP callback message",
				"callbackURL", callbackURL, "error", err)
			continue
		}
		if err := s.callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
			s.Logger.Error("failed to publish STUMP callback",
				"callbackURL", callbackURL, "error", err)
		}
	}
}

// emitBlockProcessed publishes a BLOCK_PROCESSED message to every registered callback URL.
func (s *SubtreeWorkerService) emitBlockProcessed(blockHash string) {
	if s.urlRegistry == nil {
		return
	}

	urls, err := s.urlRegistry.GetAll()
	if err != nil {
		s.Logger.Error("failed to get callback URLs for BLOCK_PROCESSED", "error", err)
		return
	}
	if len(urls) == 0 {
		return
	}

	for _, callbackURL := range urls {
		msg := &kafka.CallbackTopicMessage{
			CallbackURL: callbackURL,
			Type:        kafka.CallbackBlockProcessed,
			BlockHash:   blockHash,
		}
		data, err := msg.Encode()
		if err != nil {
			s.Logger.Error("failed to encode BLOCK_PROCESSED message",
				"callbackURL", callbackURL,
				"error", err,
			)
			continue
		}
		if err := s.callbackProducer.PublishWithHashKey(callbackURL, data); err != nil {
			s.Logger.Error("failed to publish BLOCK_PROCESSED callback",
				"callbackURL", callbackURL,
				"error", err,
			)
		}
	}

	s.Logger.Info("emitted BLOCK_PROCESSED callbacks",
		"blockHash", blockHash,
		"callbackURLs", len(urls),
	)
}
