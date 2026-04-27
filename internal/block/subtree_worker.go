package block

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"

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
type SubtreeWorkerService struct {
	service.BaseService

	kafkaCfg       config.KafkaConfig
	blockCfg       config.BlockConfig
	datahubCfg     config.DataHubConfig
	consumer         *kafka.Consumer
	callbackProducer *kafka.Producer
	regStore         store.RegistrationStore
	subtreeStore     store.SubtreeStore
	stumpStore       store.StumpStore
	urlRegistry      store.CallbackURLRegistry
	subtreeCounter   store.SubtreeCounterStore
	dataHubClient    *datahub.Client
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

	// Create callback producer for STUMP callbacks.
	callbackProducer, err := kafka.NewProducer(
		s.kafkaCfg.Brokers,
		s.kafkaCfg.CallbackTopic,
		s.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback producer: %w", err)
	}
	s.callbackProducer = callbackProducer

	// Create consumer for the subtree-work topic.
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

func (s *SubtreeWorkerService) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	workMsg, err := kafka.DecodeSubtreeWorkMessage(msg.Value)
	if err != nil {
		s.Logger.Error("failed to decode subtree work message", "error", err)
		return fmt.Errorf("failed to decode subtree work message: %w", err)
	}

	s.Logger.Debug("processing subtree work item",
		"subtreeHash", workMsg.SubtreeHash,
		"blockHash", workMsg.BlockHash,
		"blockHeight", workMsg.BlockHeight,
	)

	// Process the subtree: registration lookup, STUMP build, callback grouping.
	result, err := ProcessBlockSubtree(
		ctx,
		workMsg.SubtreeHash,
		uint64(workMsg.BlockHeight),
		workMsg.BlockHash,
		workMsg.DataHubURL,
		s.dataHubClient,
		s.subtreeStore,
		s.regStore,
		s.blockCfg.PostMineTTLSec,
		s.Logger,
	)
	if err != nil {
		s.Logger.Error("failed to process subtree work item",
			"subtreeHash", workMsg.SubtreeHash,
			"blockHash", workMsg.BlockHash,
			"error", err,
		)
		// Don't return error — continue to decrement counter so BLOCK_PROCESSED
		// can still fire. The subtree processing failure is logged.
	}

	// Publish one STUMP callback per (callbackURL, subtree) combination.
	if result != nil && len(result.CallbackGroups) > 0 {
		s.publishSubtreeCallbacks(workMsg, result)
	}

	// Decrement the subtree counter. If it reaches zero, emit BLOCK_PROCESSED.
	if s.subtreeCounter != nil {
		remaining, err := s.subtreeCounter.Decrement(workMsg.BlockHash)
		if err != nil {
			s.Logger.Error("failed to decrement subtree counter",
				"blockHash", workMsg.BlockHash,
				"error", err,
			)
		} else if remaining == 0 {
			s.emitBlockProcessed(workMsg.BlockHash)
		}
	}

	return nil
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
