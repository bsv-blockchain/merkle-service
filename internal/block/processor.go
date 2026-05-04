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

	kafkaCfg            config.KafkaConfig
	blockCfg            config.BlockConfig
	datahubCfg          config.DataHubConfig
	consumer            *kafka.Consumer
	subtreeWorkProducer *kafka.Producer
	regStore            store.RegistrationStore
	subtreeStore        store.SubtreeStore
	urlRegistry         store.CallbackURLRegistry
	subtreeCounter      store.SubtreeCounterStore
	dataHubClient       *datahub.Client
	dedupCache          *cache.DedupCache
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
	p.dataHubClient = datahub.NewClientWithSSRFGuard(
		p.datahubCfg.TimeoutSec,
		p.datahubCfg.MaxRetries,
		p.datahubCfg.MaxBlockBytes,
		p.datahubCfg.MaxSubtreeBytes,
		p.datahubCfg.AllowPrivateIPs,
		p.Logger,
	)

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

// handleMessage processes a single block announcement.
//
// Correctness contract: the block is added to the dedup cache (and the Kafka
// message is ack'd by returning nil) ONLY after every subtree work message
// for the block has been durably published to the subtree-work topic. Any
// encode or publish failure returns a non-nil error so the consumer does not
// mark the offset, which surfaces the failure for retry on the next session
// and prevents silent data loss (F-011).
func (p *Processor) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	blockMsg, err := kafka.DecodeBlockMessage(msg.Value)
	if err != nil {
		// Malformed bytes (or a message that fails post-decode validation, e.g.
		// missing/invalid hash or DataHub URL) cannot be recovered by re-driving
		// — log and ack so the partition can advance. F-032 makes the decoder
		// return ErrInvalidMessage for poison-pill payloads instead of letting
		// them flow into HTTP fetches and downstream stores. Matches the same
		// drop-on-decode-error pattern used by the subtree-fetcher and
		// subtree-worker consumers.
		p.Logger.Error("failed to decode block message, dropping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		return nil
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
		// No subtree work to fan out, but the block has been observed and
		// metadata fetched — record it in dedup so a redelivery is skipped.
		if p.dedupCache != nil {
			p.dedupCache.Add(blockMsg.Hash)
		}
		return nil
	}

	// Pre-encode every SubtreeWorkMessage before initializing the counter or
	// publishing anything. Encoding is deterministic; failing now means the
	// payload is malformed and would fail again on retry, but we must not have
	// already initialized the counter (which would leave it pointing at work
	// that will never arrive). Returning an error keeps the offset un-acked so
	// Kafka redelivers; persistent failure should be caught by an upstream DLQ
	// policy on the block topic.
	type encodedWork struct {
		subtreeHash string
		payload     []byte
	}
	encoded := make([]encodedWork, 0, len(subtreeHashes))
	for i, stHash := range subtreeHashes {
		workMsg := &kafka.SubtreeWorkMessage{
			BlockHash:    blockMsg.Hash,
			BlockHeight:  meta.Height,
			SubtreeHash:  stHash,
			SubtreeIndex: i,
			DataHubURL:   blockMsg.DataHubURL,
		}
		data, encErr := workMsg.Encode()
		if encErr != nil {
			p.Logger.Error("failed to encode subtree work message",
				"subtreeHash", stHash,
				"blockHash", blockMsg.Hash,
				"subtreeIndex", i,
				"error", encErr,
			)
			return fmt.Errorf("encoding subtree work message for block %s subtree %s: %w", blockMsg.Hash, stHash, encErr)
		}
		encoded = append(encoded, encodedWork{subtreeHash: stHash, payload: data})
	}

	// Initialize the subtree counter BEFORE publishing work messages so that
	// workers cannot decrement a missing counter. The Aerospike-backed Init
	// uses RecordExistsAction=UPDATE (upsert), so on a redelivery this safely
	// overwrites any stale value left by a previous partial-publish attempt.
	if p.subtreeCounter != nil {
		if err := p.subtreeCounter.Init(blockMsg.Hash, len(encoded)); err != nil {
			p.Logger.Error("failed to init subtree counter",
				"blockHash", blockMsg.Hash,
				"count", len(encoded),
				"error", err,
			)
			return fmt.Errorf("failed to init subtree counter for block %s: %w", blockMsg.Hash, err)
		}
	}

	// Publish each pre-encoded SubtreeWorkMessage. On the first publish
	// failure we stop and return an error so the block message is NOT ack'd
	// and NOT added to the dedup cache. On redelivery the counter is
	// re-initialized (overwriting whatever the previous attempt left); some
	// subtree work may be re-published, but the SubtreeWorkMessage retry
	// pipeline (AttemptCount + subtree-work-dlq) is already idempotent, so
	// duplicate fan-out is safe.
	for i, ew := range encoded {
		if err := p.subtreeWorkProducer.PublishWithHashKey(ew.subtreeHash, ew.payload); err != nil {
			p.Logger.Error("failed to publish subtree work message",
				"subtreeHash", ew.subtreeHash,
				"blockHash", blockMsg.Hash,
				"subtreeIndex", i,
				"published", i,
				"total", len(encoded),
				"error", err,
			)
			return fmt.Errorf("publishing subtree work for block %s subtree %s (%d/%d): %w",
				blockMsg.Hash, ew.subtreeHash, i, len(encoded), err)
		}
	}

	p.Logger.Info("dispatched subtree work items",
		"blockHash", blockMsg.Hash,
		"subtreeCount", len(encoded),
	)

	// Update subtree store block height for DAH pruning. Only safe to do once
	// every subtree work message for this block has been successfully
	// published — otherwise an unpublished subtree could be pruned.
	p.subtreeStore.SetCurrentBlockHeight(uint64(meta.Height))

	// Mark block as successfully processed for dedup. This must be the last
	// step: anything that returned an error above leaves the cache untouched
	// so the block is retried on redelivery.
	if p.dedupCache != nil {
		p.dedupCache.Add(blockMsg.Hash)
	}

	return nil
}
