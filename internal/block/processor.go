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

// blockFetchPerPeerTimeout caps a single DataHub probe inside the
// block-processor failover loop. It mirrors reprocessProbeTimeout in the
// /reprocess flow so a bad peer cannot hold the block-processor for
// longer than the per-attempt HTTP timeout plus a single backoff.
const blockFetchPerPeerTimeout = 12 * time.Second

// Processor implements the block processor service.
type Processor struct {
	service.BaseService

	kafkaCfg            config.KafkaConfig
	blockCfg            config.BlockConfig
	datahubCfg          config.DataHubConfig
	consumer            *kafka.Consumer
	subtreeWorkProducer *kafka.Producer
	// callbackProducer publishes BLOCK_PROCESSED directly from the
	// block-processor for blocks that have zero subtrees (coinbase-only
	// blocks). Without this, empty blocks never produce a BLOCK_PROCESSED
	// callback — the subtree-worker emits it via decrementCounterAndMaybeEmit
	// but the counter is never initialized when there are no subtree-work
	// messages, so the path never fires. On /reprocess that meant arcade
	// got no acknowledgement and would retry indefinitely.
	callbackProducer *kafka.Producer
	regStore         store.RegistrationStore
	subtreeStore     store.SubtreeStore
	urlRegistry      store.CallbackURLRegistry
	dataHubRegistry  store.DataHubRegistry
	subtreeCounter   store.SubtreeCounterStore
	dataHubClient    *datahub.Client
	dedupCache       *cache.DedupCache
}

func NewProcessor(
	kafkaCfg config.KafkaConfig,
	blockCfg config.BlockConfig,
	datahubCfg config.DataHubConfig,
	regStore store.RegistrationStore,
	subtreeStore store.SubtreeStore,
	urlRegistry store.CallbackURLRegistry,
	dataHubRegistry store.DataHubRegistry,
	subtreeCounter store.SubtreeCounterStore,
	logger *slog.Logger,
) *Processor {
	p := &Processor{
		kafkaCfg:        kafkaCfg,
		blockCfg:        blockCfg,
		datahubCfg:      datahubCfg,
		regStore:        regStore,
		subtreeStore:    subtreeStore,
		urlRegistry:     urlRegistry,
		dataHubRegistry: dataHubRegistry,
		subtreeCounter:  subtreeCounter,
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
	p.dataHubClient.SetPeerHealth(datahub.NewPeerHealth(
		p.datahubCfg.PeerHealth.FailureThreshold,
		time.Duration(p.datahubCfg.PeerHealth.CooldownSec)*time.Second,
	))

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

	// Create producer for emitting BLOCK_PROCESSED on the empty-block path.
	callbackProducer, err := kafka.NewProducer(
		p.kafkaCfg.Brokers,
		p.kafkaCfg.CallbackTopic,
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create callback producer: %w", err)
	}
	p.callbackProducer = callbackProducer

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
	if p.callbackProducer != nil {
		if err := p.callbackProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
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
func (p *Processor) handleMessage(ctx context.Context, msg *kafka.Message) error {
	blockMsg, err := kafka.DecodeBlockMessage(msg.Value)
	if err != nil {
		p.Logger.Error("failed to decode block message", "error", err)
		return err
	}

	p.Logger.Info(
		"processing block announcement",
		"hash", blockMsg.Hash,
		"height", blockMsg.Height,
		"dataHubUrl", blockMsg.DataHubURL,
		"reprocess", blockMsg.OverrideCallbackURL != "",
	)

	// Check dedup cache — skip if already successfully processed. Reprocess
	// requests (BypassDedup) intentionally skip both the lookup and the later
	// Add so a hash already seen via P2P can still be reprocessed on demand.
	if !blockMsg.BypassDedup && p.dedupCache != nil && p.dedupCache.Contains(blockMsg.Hash) {
		p.Logger.Debug("skipping duplicate block message", "hash", blockMsg.Hash)
		return nil
	}

	// 5.2: Fetch block metadata from DataHub. The announcing peer is tried
	// first, then registered DataHubs are tried in turn if the announcing
	// peer is unhealthy or returns a transport error / 404. resolvedURL is
	// the URL that actually served the metadata — downstream subtree work
	// is stamped with that URL so workers don't inherit a known-bad peer.
	meta, resolvedURL, err := p.fetchBlockMetadataWithFailover(ctx, blockMsg.Hash, blockMsg.DataHubURL)
	if err != nil {
		p.Logger.Error("failed to fetch block metadata", "hash", blockMsg.Hash, "error", err)
		return fmt.Errorf("fetching block metadata %s: %w", blockMsg.Hash, err)
	}

	// Remember the URL that successfully served the block — the /reprocess
	// endpoint reads the registry to pick a candidate DataHub when serving
	// past-block requests, and we only want to remember URLs that actually
	// served a block successfully.
	if p.dataHubRegistry != nil && resolvedURL != "" {
		if regErr := p.dataHubRegistry.Add(resolvedURL); regErr != nil {
			p.Logger.Warn("failed to record DataHub URL in registry",
				"url", resolvedURL, "error", regErr)
		}
	}

	// 5.3: Extract subtree hashes from block metadata.
	subtreeHashes := meta.Subtrees
	p.Logger.Info(
		"block metadata fetched",
		"hash", blockMsg.Hash,
		"height", meta.Height,
		"subtreeCount", len(subtreeHashes),
	)

	if len(subtreeHashes) == 0 {
		// Coinbase-only block. There is no subtree-work to fan out, so the
		// subtree-worker's decrement-to-zero path will never run and
		// BLOCK_PROCESSED would never fire. Emit it directly here so:
		//   - /reprocess (OverrideCallbackURL != "") sends the arcade the
		//     acknowledgement it is blocking on; otherwise arcade retries
		//     /reprocess for this same block indefinitely.
		//   - live announcements (OverrideCallbackURL == "") broadcast
		//     "we processed this block" to every registered callback URL
		//     so downstream consumers stay in step with the chain tip.
		// A publish failure leaves the dedup cache untouched and returns
		// an error so the consumer redelivers on the next session.
		// Coinbase-only block: no subtrees, so the merkle root is the coinbase
		// txid. Still surface the merkle root (and subtreeCount=0) so consumers
		// can record it; there is no compound BUMP to build.
		emptyBlockData := p.buildBlockProcessedData(ctx, blockMsg, meta, resolvedURL)
		if err := emitBlockProcessedCallbacks(
			p.Logger,
			p.urlRegistry,
			p.callbackProducer,
			blockMsg.Hash,
			blockMsg.OverrideCallbackURL,
			blockMsg.OverrideCallbackToken,
			emptyBlockData,
		); err != nil {
			return fmt.Errorf("emitting BLOCK_PROCESSED for empty block %s: %w", blockMsg.Hash, err)
		}
		if !blockMsg.BypassDedup && p.dedupCache != nil {
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
			BlockHash:             blockMsg.Hash,
			BlockHeight:           meta.Height,
			SubtreeHash:           stHash,
			SubtreeIndex:          i,
			DataHubURL:            resolvedURL,
			OverrideCallbackURL:   blockMsg.OverrideCallbackURL,
			OverrideCallbackToken: blockMsg.OverrideCallbackToken,
		}
		data, encErr := workMsg.Encode()
		if encErr != nil {
			p.Logger.Error(
				"failed to encode subtree work message",
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
	//
	// On reprocess we scope the counter key by override URL so a /reprocess
	// for a hash already being processed live (or being reprocessed by a
	// different arcade) gets its own counter — see SubtreeCounterKey.
	// Build the BLOCK_PROCESSED enrichment (merkle root, subtree list, coinbase
	// BUMP) now, while the block header, coinbase, and metadata are in hand, and
	// stash it on the counter record so the worker that drains the counter to
	// zero can surface it on BLOCK_PROCESSED without re-deriving anything. This
	// is best-effort: buildBlockProcessedData degrades to partial data on any
	// failure and the consumer falls back to a datahub.
	blockData := p.buildBlockProcessedData(ctx, blockMsg, meta, resolvedURL)

	counterKey := SubtreeCounterKey(blockMsg.Hash, blockMsg.OverrideCallbackURL)
	if p.subtreeCounter != nil {
		if err := p.subtreeCounter.Init(counterKey, len(encoded), blockData); err != nil {
			p.Logger.Error(
				"failed to init subtree counter",
				"blockHash", blockMsg.Hash,
				"counterKey", counterKey,
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
	// Batched fan-out (throughput review F-6): one synchronous batch publish
	// instead of one broker-acked round-trip per subtree (~600-900 serial RTTs
	// for a teranode-default block). A batch error may leave SOME records
	// published — that is the same partial-publish state the serial loop could
	// leave, and the redelivery path above already handles it.
	entries := make([]kafka.BatchEntry, len(encoded))
	for i, ew := range encoded {
		entries[i] = kafka.HashBatchEntry(ew.subtreeHash, ew.payload)
	}
	if err := p.subtreeWorkProducer.PublishBatch(entries); err != nil {
		p.Logger.Error(
			"failed to publish subtree work batch",
			"blockHash", blockMsg.Hash,
			"total", len(encoded),
			"error", err,
		)
		return fmt.Errorf("publishing subtree work batch for block %s (%d subtrees): %w",
			blockMsg.Hash, len(encoded), err)
	}

	p.Logger.Info(
		"dispatched subtree work items",
		"blockHash", blockMsg.Hash,
		"subtreeCount", len(encoded),
	)

	// Update subtree store block height for DAH pruning. Only safe to do once
	// every subtree work message for this block has been successfully
	// published — otherwise an unpublished subtree could be pruned.
	p.subtreeStore.SetCurrentBlockHeight(uint64(meta.Height))

	// Mark block as successfully processed for dedup. This must be the last
	// step: anything that returned an error above leaves the cache untouched
	// so the block is retried on redelivery. Reprocess requests
	// (BypassDedup) are intentionally NOT recorded — a future live
	// announcement of the same hash must still flow through the live path.
	if !blockMsg.BypassDedup && p.dedupCache != nil {
		p.dedupCache.Add(blockMsg.Hash)
	}

	return nil
}

// fetchBlockMetadataWithFailover fetches block metadata, trying the
// announced peer first and then known-good peers from the DataHub
// registry. The announcing peer is honored even if it is currently
// marked unhealthy so a peer that recovered between announcements has a
// chance to serve again — its outcome is recorded by the client and
// will either reset or extend its unhealthy mark.
//
// Returns (meta, resolvedURL, nil) on the first peer that serves the
// block. Returns an error wrapping datahub.ErrNotFound if every
// attempted peer returned 404 (the block is confirmed missing from the
// network we can see). Returns a generic error if all peers failed for
// transport reasons.
func (p *Processor) fetchBlockMetadataWithFailover(
	ctx context.Context,
	blockHash, announcedURL string,
) (*datahub.BlockMetadata, string, error) {
	type attempt struct {
		url      string
		forceTry bool // announced URL: try even if currently unhealthy
	}

	tried := make(map[string]struct{})
	var attempts []attempt
	if announcedURL != "" {
		attempts = append(attempts, attempt{url: announcedURL, forceTry: true})
		tried[announcedURL] = struct{}{}
	}

	// Registry candidates only matter when there's an actual registry and
	// at least one URL is registered. A scan failure should not abort the
	// whole fetch — we still have the announced URL.
	if p.dataHubRegistry != nil {
		registered, regErr := p.dataHubRegistry.GetAll()
		if regErr != nil {
			p.Logger.Warn("failed to list DataHub registry for failover",
				"hash", blockHash, "error", regErr)
		}
		for _, u := range registered {
			if u == "" {
				continue
			}
			if _, ok := tried[u]; ok {
				continue
			}
			tried[u] = struct{}{}
			attempts = append(attempts, attempt{url: u})
		}
	}

	ph := p.dataHubClient.PeerHealth()
	var lastErr error
	for _, a := range attempts {
		if !a.forceTry && ph != nil && !ph.IsHealthy(a.url) {
			p.Logger.Debug("skipping unhealthy DataHub peer for block fetch",
				"hash", blockHash, "url", a.url)
			continue
		}

		probeCtx, cancel := context.WithTimeout(ctx, blockFetchPerPeerTimeout)
		meta, err := p.dataHubClient.FetchBlockMetadata(probeCtx, a.url, blockHash)
		cancel()
		if err == nil {
			if a.url != announcedURL {
				p.Logger.Info(
					"block metadata served by failover DataHub",
					"hash", blockHash,
					"announcedUrl", announcedURL,
					"resolvedUrl", a.url,
				)
			}
			return meta, a.url, nil
		}
		lastErr = err
		p.Logger.Debug(
			"DataHub failover candidate failed",
			"hash", blockHash,
			"url", a.url,
			"notFound", errors.Is(err, datahub.ErrNotFound),
			"error", err,
		)
	}

	if lastErr == nil {
		// No candidate was attempted (every registered peer was unhealthy
		// and the announced URL was empty). Return a clear error so the
		// caller doesn't ack a block it never tried to fetch.
		return nil, "", fmt.Errorf("no healthy DataHub peer available for block %s", blockHash)
	}
	// lastErr already wraps datahub.ErrNotFound when every probe returned
	// 404, so callers can still errors.Is(err, datahub.ErrNotFound).
	return nil, "", lastErr
}

// SubtreeCounterKey composes the per-block subtree counter store key.
// Live announcements key on blockHash alone (preserving today's behavior).
// Reprocess requests scope by overrideURL so two arcades reprocessing the
// same block, or a reprocess overlapping with a live announcement, do not
// share counter state and therefore cannot fire BLOCK_PROCESSED for each
// other. The counter store treats the key as opaque, so no interface
// change is needed.
func SubtreeCounterKey(blockHash, overrideURL string) string {
	if overrideURL == "" {
		return blockHash
	}
	return blockHash + "|" + overrideURL
}
