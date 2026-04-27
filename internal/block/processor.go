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

// Processor implements the block processor service.
type Processor struct {
	service.BaseService
	kafkaCfg             config.KafkaConfig
	blockCfg             config.BlockConfig
	datahubCfg           config.DataHubConfig
	consumer             *kafka.Consumer
	subtreeWorkProducer  *kafka.Producer
	regStore             store.RegistrationStore
	subtreeStore         store.SubtreeStore
	urlRegistry          store.CallbackURLRegistry
	subtreeCounter       store.SubtreeCounterStore
	dataHubClient        *datahub.Client
	dedupCache           *cache.DedupCache
}

func NewProcessor(
	kafkaCfg config.KafkaConfig,
	blockCfg config.BlockConfig,
	datahubCfg config.DataHubConfig,
	regStore store.RegistrationStore,
	subtreeStore store.SubtreeStore,
	urlRegistry store.CallbackURLRegistry,
	subtreeCounter store.SubtreeCounterStore,
	logger *slog.Logger,
) *Processor {
	p := &Processor{
		kafkaCfg:       kafkaCfg,
		blockCfg:       blockCfg,
		datahubCfg:     datahubCfg,
		regStore:       regStore,
		subtreeStore:   subtreeStore,
		urlRegistry:    urlRegistry,
		subtreeCounter: subtreeCounter,
	}
	p.InitBase("block-processor")
	if logger != nil {
		p.Logger = logger
	}
	return p
}

func (p *Processor) Init(cfg interface{}) error {
	p.dataHubClient = datahub.NewClient(p.datahubCfg.TimeoutSec, p.datahubCfg.MaxRetries, p.Logger)

	// Initialize message dedup cache.
	if p.blockCfg.DedupCacheSize > 0 {
		p.dedupCache = cache.NewDedupCache(p.blockCfg.DedupCacheSize)
	}

	// Create producer for dispatching subtree work items.
	subtreeWorkProducer, err := kafka.NewProducer(
		p.kafkaCfg.Brokers,
		p.kafkaCfg.SubtreeWorkTopic,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree-work producer: %w", err)
	}
	p.subtreeWorkProducer = subtreeWorkProducer

	consumer, err := kafka.NewConsumer(
		p.kafkaCfg.Brokers,
		p.kafkaCfg.ConsumerGroup+"-block",
		[]string{p.kafkaCfg.BlockTopic},
		p.handleMessage,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create block consumer: %w", err)
	}
	p.consumer = consumer
	return nil
}

func (p *Processor) Start(ctx context.Context) error {
	p.SetStarted(true)
	p.Logger.Info("starting block processor")
	return p.consumer.Start(ctx)
}

func (p *Processor) Stop() error {
	p.Logger.Info("stopping block processor")
	p.SetStarted(false)
	var firstErr error
	if err := p.consumer.Stop(); err != nil {
		firstErr = err
	}
	if p.subtreeWorkProducer != nil {
		if err := p.subtreeWorkProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *Processor) Health() service.HealthStatus {
	status := "healthy"
	if !p.IsStarted() {
		status = "unhealthy"
	}
	return service.HealthStatus{
		Name:   "block-processor",
		Status: status,
	}
}

func (p *Processor) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	blockMsg, err := kafka.DecodeBlockMessage(msg.Value)
	if err != nil {
		p.Logger.Error("failed to decode block message", "error", err)
		return err
	}

	p.Logger.Info("processing block announcement",
		"hash", blockMsg.Hash,
		"height", blockMsg.Height,
		"dataHubUrl", blockMsg.DataHubURL,
	)

	// Check dedup cache — skip if already successfully processed.
	if p.dedupCache != nil && p.dedupCache.Contains(blockMsg.Hash) {
		p.Logger.Debug("skipping duplicate block message", "hash", blockMsg.Hash)
		return nil
	}

	// 5.2: Fetch block metadata from DataHub.
	meta, err := p.dataHubClient.FetchBlockMetadata(ctx, blockMsg.DataHubURL, blockMsg.Hash)
	if err != nil {
		p.Logger.Error("failed to fetch block metadata", "hash", blockMsg.Hash, "error", err)
		return fmt.Errorf("fetching block metadata %s: %w", blockMsg.Hash, err)
	}

	// 5.3: Extract subtree hashes from block metadata.
	subtreeHashes := meta.Subtrees
	p.Logger.Info("block metadata fetched",
		"hash", blockMsg.Hash,
		"height", meta.Height,
		"subtreeCount", len(subtreeHashes),
	)

	if len(subtreeHashes) == 0 {
		return nil
	}

	// Initialize the subtree counter BEFORE publishing work messages to avoid
	// a race where workers decrement a counter that doesn't exist yet.
	if p.subtreeCounter != nil {
		if err := p.subtreeCounter.Init(blockMsg.Hash, len(subtreeHashes)); err != nil {
			p.Logger.Error("failed to init subtree counter",
				"blockHash", blockMsg.Hash,
				"count", len(subtreeHashes),
				"error", err,
			)
			return fmt.Errorf("failed to init subtree counter for block %s: %w", blockMsg.Hash, err)
		}
	}

	// Publish one SubtreeWorkMessage per subtree to the subtree-work topic.
	for i, stHash := range subtreeHashes {
		workMsg := &kafka.SubtreeWorkMessage{
			BlockHash:    blockMsg.Hash,
			BlockHeight:  meta.Height,
			SubtreeHash:  stHash,
			SubtreeIndex: i,
			DataHubURL:   blockMsg.DataHubURL,
		}
		data, err := workMsg.Encode()
		if err != nil {
			p.Logger.Error("failed to encode subtree work message",
				"subtreeHash", stHash,
				"blockHash", blockMsg.Hash,
				"error", err,
			)
			continue
		}
		if err := p.subtreeWorkProducer.PublishWithHashKey(stHash, data); err != nil {
			p.Logger.Error("failed to publish subtree work message",
				"subtreeHash", stHash,
				"blockHash", blockMsg.Hash,
				"error", err,
			)
		}
	}

	p.Logger.Info("dispatched subtree work items",
		"blockHash", blockMsg.Hash,
		"subtreeCount", len(subtreeHashes),
	)

	// Update subtree store block height for DAH pruning.
	p.subtreeStore.SetCurrentBlockHeight(uint64(meta.Height))

	// Mark block as successfully processed for dedup.
	if p.dedupCache != nil {
		p.dedupCache.Add(blockMsg.Hash)
	}

	return nil
}
