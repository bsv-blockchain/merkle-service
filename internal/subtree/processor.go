package subtree

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/logfields"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/retryutil"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/ssrfguard"
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

// SeenCounter abstracts peer-weighted seen scoring for testability.
// BatchAddPeer carries the store.SeenCounterStore partial-success contract:
// results for every txid that succeeded plus the first error (F-058).
type SeenCounter interface {
	AddPeer(txid, peerID string, weight int) (*store.IncrementResult, error)
	BatchAddPeer(txids []string, peerID string, weight int) (map[string]*store.IncrementResult, error)
}

// NodeWeights provides mining-node weights for SEEN_MULTIPLE_NODES scoring.
type NodeWeights interface {
	Ready() bool
	Weight(peerID string) int
}

// SubtreeAttributor records first-seen peer per subtree hash.
type SubtreeAttributor interface {
	TryAttribute(subtreeHash, peerID string) (attributedPeer string, first bool, err error)
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

	cfg                *config.Config
	consumer           *kafka.Consumer
	callbackProducer   *kafka.Producer
	retryProducer      *kafka.Producer // re-publishes to the subtree topic on transient failure
	dlqProducer        *kafka.Producer // publishes to subtree-dlq when MaxAttempts is exceeded
	registrationStore  RegistrationGetter
	seenCounterStore   SeenCounter
	subtreeStore       store.SubtreeStore
	nodeRegistry       NodeWeights
	subtreeAttribution SubtreeAttributor
	regCache           RegCache
	dedupCache         *cache.DedupCache
	dataHubClient      *datahub.Client
}

// NewProcessor creates a new subtree Processor. logger, when non-nil,
// overrides the default logger InitBase would otherwise install — this is
// what lets the configured LOG_LEVEL reach the subtree-fetcher instead of
// silently falling back to InitBase's hardcoded Info logger.
func NewProcessor(
	cfg *config.Config,
	registrationStore RegistrationGetter,
	seenCounterStore SeenCounter,
	subtreeStore store.SubtreeStore,
	nodeRegistry NodeWeights,
	subtreeAttribution SubtreeAttributor,
	logger *slog.Logger,
) *Processor {
	p := &Processor{
		cfg:                cfg,
		registrationStore:  registrationStore,
		seenCounterStore:   seenCounterStore,
		subtreeStore:       subtreeStore,
		nodeRegistry:       nodeRegistry,
		subtreeAttribution: subtreeAttribution,
	}
	p.InitBase("subtree-fetcher")
	if logger != nil {
		p.Logger = logger
	}
	return p
}

// Init initializes the subtree processor, setting up the Kafka consumer, producer, and registration cache.
func (p *Processor) Init(_ interface{}) error {
	p.InitBase("subtree-fetcher")

	// Initialize DataHub client. SSRF guard rejects peer-supplied URLs
	// that point at private/loopback/link-local destinations unless the
	// operator opts in via DataHub.AllowPrivateIPs (F-028). A PeerHealth
	// tracker is attached so subtree fetch outcomes feed into the same
	// "is this peer dead?" signal /reprocess uses — note the subtree
	// processor does not fail over to other peers itself (the announced
	// URL is authoritative for which peer has the subtree); it gates on
	// IsHealthy and records outcomes after classifying them by ctx state
	// and announcement age (see recordPeerFetchOutcome).
	p.dataHubClient = datahub.NewClientWithSSRFGuard(
		p.cfg.DataHub.TimeoutSec,
		p.cfg.DataHub.MaxRetries,
		p.cfg.DataHub.MaxBlockBytes,
		p.cfg.DataHub.MaxSubtreeBytes,
		p.cfg.DataHub.AllowPrivateIPs,
		p.Logger,
	)
	p.dataHubClient.SetPeerHealth(datahub.NewPeerHealth(
		p.cfg.DataHub.PeerHealth.FailureThreshold,
		time.Duration(p.cfg.DataHub.PeerHealth.CooldownSec)*time.Second,
	))

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
		p.cfg.Kafka.TopicPartitions(),
		p.cfg.Kafka.TopicRetention(),
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
		p.cfg.Kafka.TopicPartitions(),
		p.cfg.Kafka.TopicRetention(),
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
		p.cfg.Kafka.TopicPartitions(),
		p.cfg.Kafka.TopicRetention(),
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
		p.cfg.Kafka.TopicPartitions(),
		p.cfg.Kafka.TopicRetention(),
		p.Logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create subtree consumer: %w", err)
	}
	p.consumer = consumer

	p.Logger.Info(
		"subtree-fetcher initialized",
		"storageMode", p.cfg.Subtree.StorageMode,
		"subtreeTopic", p.cfg.Kafka.SubtreeTopic,
		"subtreeDLQTopic", p.cfg.Kafka.SubtreeDLQTopic,
		"callbackTopic", p.cfg.Kafka.CallbackTopic,
		"maxAttempts", p.cfg.Subtree.MaxAttempts,
		"retryBackoffBaseMs", p.cfg.Subtree.RetryBackoffBaseMs,
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
	p.Logger.Info(
		"subtree-fetcher stopped",
		"messagesProcessed", int64(testutil.ToFloat64(metrics.SubtreeMessagesTotal.WithLabelValues(metrics.OutcomeProcessed))),
		"messagesRetried", int64(testutil.ToFloat64(metrics.SubtreeMessagesTotal.WithLabelValues(metrics.OutcomeRetried))),
		"messagesDLQ", int64(testutil.ToFloat64(metrics.SubtreeMessagesTotal.WithLabelValues(metrics.OutcomeDLQ))),
		"messagesSkippedUnhealthy", int64(testutil.ToFloat64(metrics.SubtreeMessagesTotal.WithLabelValues(metrics.OutcomeSkippedUnhealthy))),
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
			"messagesProcessed":        fmt.Sprintf("%d", int64(testutil.ToFloat64(metrics.SubtreeMessagesTotal.WithLabelValues(metrics.OutcomeProcessed)))),
			"messagesSkippedUnhealthy": fmt.Sprintf("%d", int64(testutil.ToFloat64(metrics.SubtreeMessagesTotal.WithLabelValues(metrics.OutcomeSkippedUnhealthy)))),
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
func (p *Processor) handleMessage(ctx context.Context, msg *kafka.Message) error {
	start := time.Now()

	subtreeMsg, err := kafka.DecodeSubtreeMessage(msg.Value)
	if err != nil {
		// Malformed bytes at the head of the partition cannot be recovered by
		// re-driving — drop the offset by returning nil after logging. A
		// decode failure is not DLQ-able because we don't have a structured
		// message to wrap.
		p.Logger.Error(
			"failed to decode subtree message, dropping",
			"offset", msg.Offset,
			"partition", msg.Partition,
			"error", err,
		)
		metrics.ObserveSubtreeProcessing(metrics.OutcomeDecodeError, time.Since(start))
		metrics.IncKafkaConsumerError(msg.Topic, metrics.KafkaErrorDecode)
		return nil
	}

	p.Logger.Debug(
		"processing subtree announcement",
		logfields.SubtreeHash(subtreeMsg.Hash),
		logfields.DataHubURL(subtreeMsg.DataHubURL),
		"peerId", subtreeMsg.PeerID,
		"attemptCount", subtreeMsg.AttemptCount,
	)

	// Local dedup FIRST — O(1) memory, no shared-store RTT (high-TPS path).
	if p.dedupCache != nil && p.dedupCache.Contains(subtreeMsg.Hash) {
		p.Logger.Debug("skipping duplicate subtree message", logfields.SubtreeHash(subtreeMsg.Hash))
		metrics.ObserveSubtreeProcessing(metrics.OutcomeDedupHit, time.Since(start))
		return nil
	}

	// First-seen peer attribution (shared store). Losers skip heavy work.
	attributedPeer := subtreeMsg.PeerID
	if p.subtreeAttribution != nil && subtreeMsg.Hash != "" && subtreeMsg.PeerID != "" {
		peer, first, attrErr := p.subtreeAttribution.TryAttribute(subtreeMsg.Hash, subtreeMsg.PeerID)
		if attrErr != nil {
			p.Logger.Warn("subtree attribution failed; continuing with message peer",
				logfields.SubtreeHash(subtreeMsg.Hash), "error", attrErr)
		} else {
			attributedPeer = peer
			if !first {
				p.Logger.Debug("skipping subtree hash owned by first-seen peer",
					logfields.SubtreeHash(subtreeMsg.Hash), "peerId", peer)
				if p.dedupCache != nil {
					p.dedupCache.Add(subtreeMsg.Hash)
				}
				metrics.ObserveSubtreeProcessing(metrics.OutcomeDedupHit, time.Since(start))
				return nil
			}
		}
	}
	// Stash for emitBatchedSeenCallbacks via message field reuse — re-set PeerID
	// so downstream scoring uses the attributed peer.
	subtreeMsg.PeerID = attributedPeer

	// Peer-health gate: if the announcing peer has been failing recently,
	// skip the fetch and ack-and-drop. SEEN_ON_NETWORK detection is
	// best-effort and another healthy peer's announcement of the same
	// subtree will refill it. This prevents a continuously re-announcing
	// dead peer from generating a steady stream of retries and DLQ
	// entries on every announcement.
	if ph := p.dataHubClient.PeerHealth(); ph != nil && !ph.IsHealthy(subtreeMsg.DataHubURL) {
		p.Logger.Debug(
			"skipping subtree fetch: peer marked unhealthy",
			logfields.SubtreeHash(subtreeMsg.Hash),
			logfields.DataHubURL(subtreeMsg.DataHubURL),
		)
		metrics.ObserveSubtreeProcessing(metrics.OutcomeSkippedUnhealthy, time.Since(start))
		return nil
	}

	// 3.2: Fetch binary subtree data from DataHub. The client's internal
	// peer-health recording is suppressed for this call site: only the
	// processor knows the announcement's age, so it classifies the outcome
	// and records explicitly (see recordPeerFetchOutcome) — a blanket
	// "any error is a peer failure" rule is exactly what poisoned the
	// breaker on dev-ovh-1 (caller cancellations and lag-aged 404s).
	fetchStart := time.Now()
	rawData, err := p.dataHubClient.FetchSubtreeRaw(ctx, subtreeMsg.DataHubURL, subtreeMsg.Hash, datahub.WithoutPeerRecording())
	metrics.ObserveSubtreeDataHubFetch(subtreeMsg.DataHubURL, time.Since(fetchStart), len(rawData), err)
	p.recordPeerFetchOutcome(ctx, subtreeMsg, err)
	if err != nil {
		// A 404 from the announcing peer is permanent for that peer: subtrees
		// are content-addressable, so retrying the same URL cannot recover.
		// Route straight to DLQ. Peer-health attribution happened in
		// recordPeerFetchOutcome above: only a 404 on a fresh announcement
		// counts against the peer (and, at the threshold, short-circuits
		// subsequent announcements from the same host at the IsHealthy gate);
		// a stale announcement's 404 is our own consumer lag and is not
		// attributed, though the message still lands here.
		if errors.Is(err, datahub.ErrNotFound) {
			return p.handlePermanentFailure(ctx, subtreeMsg, "fetching subtree from DataHub", err, start)
		}
		// SSRF/DNS validation failures are equally permanent for that URL:
		// the announcing peer advertised an address we can never resolve or
		// are policy-bound to refuse (e.g. its cluster-internal service name,
		// http://asset:8090). Retrying cannot help; burning the retry budget
		// here is what used to land these in outcome="dlq" and page on-call.
		// Intake-side validation (p2p client) drops these announcements
		// before Kafka now — this guards messages already in the topic and
		// any future intake gap.
		if errors.Is(err, ssrfguard.ErrInvalidURL) || errors.Is(err, ssrfguard.ErrBlockedAddress) {
			return p.handlePermanentFailure(ctx, subtreeMsg, "fetching subtree from DataHub", err, start)
		}
		return p.handleTransientFailure(ctx, subtreeMsg, "fetching subtree from DataHub", err, start)
	}

	// 3.3: Store raw binary data in the subtree blob store.
	if p.cfg.Subtree.StorageMode == "realtime" {
		if err = p.subtreeStore.StoreSubtree(subtreeMsg.Hash, rawData, 0); err != nil {
			return p.handleTransientFailure(ctx, subtreeMsg, "storing subtree", err, start)
		}
	}

	// 3.4: Parse raw binary data into txid list.
	// DataHub returns concatenated 32-byte hashes, not full go-subtree Serialize() format.
	txids, err := datahub.ParseRawTxids(rawData)
	if err != nil {
		return p.handleTransientFailure(ctx, subtreeMsg, "parsing subtree txids", err, start)
	}
	p.Logger.Debug("processing subtree txids", "length", len(txids), logfields.SubtreeHash(subtreeMsg.Hash))
	metrics.ObserveSubtreeCounts(len(txids), 0)

	if len(txids) == 0 {
		if p.dedupCache != nil {
			p.dedupCache.Add(subtreeMsg.Hash)
		}
		metrics.ObserveSubtreeAttemptCount(subtreeMsg.AttemptCount)
		metrics.ObserveSubtreeProcessing(metrics.OutcomeProcessed, time.Since(start))
		return nil
	}

	// 4.2-4.4: Check registrations via cache and Aerospike.
	registeredTxids, err := p.findRegisteredTxids(txids)
	if err != nil {
		return p.handleTransientFailure(ctx, subtreeMsg, "checking registrations", err, start)
	}
	metrics.ObserveSubtreeCounts(0, len(registeredTxids))

	// 4.5-4.6: Emit batched callbacks grouped by callbackURL.
	//
	// A failure to encode/publish a SEEN_ON_NETWORK or SEEN_MULTIPLE_NODES
	// notification must NOT be silently swallowed: route through the same
	// retry/DLQ pipeline as a processing failure so the subtree message is
	// either retried or terminally DLQ'd. Otherwise downstream callback
	// consumers permanently lose SEEN notifications during a Kafka
	// callback-topic outage (F-057).
	if err := p.emitBatchedSeenCallbacks(ctx, registeredTxids, subtreeMsg.Hash, subtreeMsg.PeerID); err != nil {
		return p.handleTransientFailure(ctx, subtreeMsg, "publishing batched SEEN callbacks", err, start)
	}

	// Mark subtree as successfully processed for dedup.
	if p.dedupCache != nil {
		p.dedupCache.Add(subtreeMsg.Hash)
	}

	metrics.ObserveSubtreeAttemptCount(subtreeMsg.AttemptCount)
	metrics.ObserveSubtreeProcessing(metrics.OutcomeProcessed, time.Since(start))
	return nil
}

// recordPeerFetchOutcome records a subtree fetch outcome against the peer
// health tracker, replacing the client's internal recording (suppressed via
// WithoutPeerRecording) for this call site. Classification:
//
//   - caller ctx dead at record time → record NOTHING, success or failure:
//     a shutdown/rebalance/partition-loss abort says nothing about the peer
//     (a rollout used to trip the breaker on fresh pods within minutes);
//   - success → RecordSuccess;
//   - 404 on an announcement older than the stale-404 grace → record
//     NOTHING: consumer lag aged the message past teranode's ~2h
//     asset-cache retention, so the 404 is our lag, not a lying peer. The
//     message itself still routes to handlePermanentFailure/DLQ exactly as
//     before — only the peer attribution changes;
//   - anything else (404 on fresh or unstamped announcements, transport,
//     5xx, parse) → RecordFailure: a peer lying about FRESH data is still
//     unhealthy. A breaker-opening failure is WARN-logged.
func (p *Processor) recordPeerFetchOutcome(ctx context.Context, subtreeMsg *kafka.SubtreeMessage, fetchErr error) {
	ph := p.dataHubClient.PeerHealth()
	if ph == nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	if fetchErr == nil {
		ph.RecordSuccess(subtreeMsg.DataHubURL)
		return
	}
	if errors.Is(fetchErr, datahub.ErrNotFound) &&
		isStaleAnnouncement(subtreeMsg.AnnouncedAtUnixMs, time.Now(), p.stale404Grace()) {
		p.Logger.Debug(
			"suppressing peer-health failure: 404 on a stale announcement is our consumer lag, not the peer",
			logfields.SubtreeHash(subtreeMsg.Hash),
			logfields.DataHubURL(subtreeMsg.DataHubURL),
			"announcedAtUnixMs", subtreeMsg.AnnouncedAtUnixMs,
			"graceSec", int(p.stale404Grace().Seconds()),
		)
		return
	}
	if tripped := ph.RecordFailure(subtreeMsg.DataHubURL); tripped {
		p.Logger.Warn(
			"DataHub peer marked unhealthy: consecutive-failure threshold reached; announcements from this host will be ack-and-dropped until the cooldown expires",
			logfields.DataHubURL(subtreeMsg.DataHubURL),
			"failureThreshold", ph.Threshold(),
			"cooldown", ph.Cooldown().String(),
			"error", fetchErr,
		)
	}
}

// isStaleAnnouncement reports whether a subtree announcement stamped at
// announcedAtUnixMs is strictly older than grace at time now. A zero or
// negative stamp (messages published before the field existed) means "age
// unknown" and reads as fresh, so their 404s keep counting against the
// peer — backward compatible in the conservative direction.
func isStaleAnnouncement(announcedAtUnixMs int64, now time.Time, grace time.Duration) bool {
	if announcedAtUnixMs <= 0 {
		return false
	}
	return now.Sub(time.UnixMilli(announcedAtUnixMs)) > grace
}

// stale404Grace returns the configured stale-404 attribution grace. A nil
// cfg (struct-literal test processors) or a non-positive configured value
// selects datahub.DefaultStale404Grace — a zero grace would classify every
// stamped 404 as stale and blind the breaker to genuinely lying peers.
func (p *Processor) stale404Grace() time.Duration {
	if p.cfg == nil || p.cfg.DataHub.PeerHealth.Stale404GraceSec <= 0 {
		return datahub.DefaultStale404Grace
	}
	return time.Duration(p.cfg.DataHub.PeerHealth.Stale404GraceSec) * time.Second
}

// subtreeRetryBackoffCap bounds the exponential retry backoff (see
// retryBackoff) so a high AttemptCount can never park a partition worker for
// minutes at a time.
const subtreeRetryBackoffCap = 30 * time.Second

// retryBackoff returns how long to wait before retry attempt `attempt`
// (1-based): RetryBackoffBaseMs doubling per attempt, capped at
// subtreeRetryBackoffCap. 0 when the backoff is disabled (base <= 0, or a
// struct-literal test cfg). On dev-ovh-1 the whole 3-attempt budget burned in
// ~300ms against a disk that stayed full for 15 minutes — retries must span
// real time to have any chance of outliving the condition they're retrying.
func (p *Processor) retryBackoff(attempt int) time.Duration {
	if p.cfg == nil {
		return 0
	}
	return retryutil.Backoff(p.cfg.Subtree.RetryBackoffBaseMs, attempt, subtreeRetryBackoffCap)
}

// waitBackoff sleeps for d, returning early with ctx.Err() when the context
// dies first (shutdown, lost partition — see the kafka consumer's
// partitions-lost cancellation). d <= 0 returns immediately.
func waitBackoff(ctx context.Context, d time.Duration) error {
	return retryutil.Wait(ctx, d)
}

// isDiskFull reports whether err is a full-filesystem condition: ENOSPC (via
// the wrapped errno chain) or a quota/space error that only survived as text.
func isDiskFull(err error) bool {
	return retryutil.IsDiskFull(err)
}

// handleTransientFailure bumps AttemptCount and either re-publishes the
// message to the subtree topic (after an exponential backoff — see
// retryBackoff) or, once MaxAttempts has been reached, parks it on
// subtree-dlq. Returns nil on successful hand-off so the consumer acks the
// original offset; returns an error only when the producer itself is broken
// (partition stall is preferable to silent loss in that case).
//
// Full-disk failures are the exception to all of the above: they are an
// operational condition, not bad data, so they never consume the retry
// budget and never route to the DLQ (which has no replay — on dev-ovh-1 a
// 15-minute ENOSPC window dead-lettered 1,406 subtrees, permanently losing
// every registered tx's callbacks). Instead the message is PARKED: an error
// is returned, the consumer doesn't advance past the record, and topic
// retention keeps it safe in Kafka until the disk recovers.
func (p *Processor) handleTransientFailure(ctx context.Context, subtreeMsg *kafka.SubtreeMessage, stage string, cause error, start time.Time) error {
	if isDiskFull(cause) {
		p.Logger.Warn(
			"blob store full; parking subtree message in Kafka until space recovers (never DLQ)",
			logfields.SubtreeHash(subtreeMsg.Hash),
			"stage", stage,
			"attemptCount", subtreeMsg.AttemptCount,
			"error", cause,
		)
		metrics.ObserveSubtreeProcessing(metrics.OutcomeParkedDiskFull, time.Since(start))
		// Throttle the redelivery loop; AttemptCount is not bumped (the
		// message is never re-published), so this stays near the base delay
		// per redelivery on top of the consumer's own rewind backoff.
		_ = waitBackoff(ctx, p.retryBackoff(subtreeMsg.AttemptCount+1))
		return fmt.Errorf("%s: blob store full, parking message for redelivery: %w", stage, cause)
	}

	nextAttempt := subtreeMsg.AttemptCount + 1
	maxAttempts := p.cfg.Subtree.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}

	if nextAttempt >= maxAttempts {
		p.Logger.Error(
			"subtree message exceeded max attempts, routing to DLQ",
			logfields.SubtreeHash(subtreeMsg.Hash),
			"stage", stage,
			"attemptCount", subtreeMsg.AttemptCount,
			"maxAttempts", maxAttempts,
			"error", cause,
		)
		subtreeMsg.AttemptCount = nextAttempt
		if err := p.publishToDLQ(ctx, subtreeMsg); err != nil {
			return err
		}
		metrics.ObserveSubtreeProcessing(metrics.OutcomeDLQ, time.Since(start))
		return nil
	}

	backoff := p.retryBackoff(nextAttempt)
	p.Logger.Warn(
		"subtree message transient failure, re-publishing for retry",
		logfields.SubtreeHash(subtreeMsg.Hash),
		"stage", stage,
		"attemptCount", subtreeMsg.AttemptCount,
		"nextAttempt", nextAttempt,
		"backoffMs", backoff.Milliseconds(),
		"error", cause,
	)
	// Wait BEFORE the hand-off so the retry budget spans real time (1s/2s/4s…
	// rather than back-to-back republish→refetch cycles). An interrupted wait
	// returns an error without publishing: the unacked original is redelivered,
	// so nothing is lost or duplicated by aborting here.
	if waitErr := waitBackoff(ctx, backoff); waitErr != nil {
		return fmt.Errorf("interrupted while backing off before subtree retry: %w", waitErr)
	}
	subtreeMsg.AttemptCount = nextAttempt
	data, encErr := subtreeMsg.Encode()
	if encErr != nil {
		return fmt.Errorf("encoding subtree message for retry: %w", encErr)
	}
	// WithoutCancel: this republish is a durable retry hand-off that must not
	// be dropped by the originating consumer ctx being canceled mid-flight;
	// only the trace context is preserved.
	if pubErr := p.retryProducer.Publish(context.WithoutCancel(ctx), subtreeMsg.Hash, data); pubErr != nil {
		return fmt.Errorf("re-publishing subtree message for retry: %w", pubErr)
	}
	metrics.ObserveSubtreeProcessing(metrics.OutcomeRetried, time.Since(start))
	return nil
}

// handlePermanentFailure routes a subtree message straight to subtree-dlq
// without consuming the retry budget. Used when retrying the same peer
// cannot recover the failure — currently a DataHub 404, meaning the
// announcing peer does not actually serve the subtree it announced. The
// AttemptCount is left at its incoming value so a DLQ entry with
// AttemptCount=0 is distinguishable from a transient-exhausted entry.
// Returns nil on successful hand-off so the consumer acks the original
// offset.
func (p *Processor) handlePermanentFailure(ctx context.Context, subtreeMsg *kafka.SubtreeMessage, stage string, cause error, start time.Time) error {
	p.Logger.Warn(
		"subtree message permanent failure, routing to DLQ",
		logfields.SubtreeHash(subtreeMsg.Hash),
		"stage", stage,
		logfields.DataHubURL(subtreeMsg.DataHubURL),
		"attemptCount", subtreeMsg.AttemptCount,
		"error", cause,
	)
	if err := p.publishToDLQ(ctx, subtreeMsg); err != nil {
		return err
	}
	metrics.ObserveSubtreeProcessing(metrics.OutcomePermanentFailure, time.Since(start))
	return nil
}

// publishToDLQ encodes subtreeMsg and publishes it to subtree-dlq. The
// caller is responsible for setting AttemptCount as it wants the DLQ entry
// to reflect (handleTransientFailure bumps it before calling;
// handlePermanentFailure leaves it as-is). Increments messagesDLQ on
// success. Returns an error only on encode or publish failure — the caller
// must NOT ack the source message in that case.
func (p *Processor) publishToDLQ(ctx context.Context, subtreeMsg *kafka.SubtreeMessage) error {
	data, encErr := subtreeMsg.Encode()
	if encErr != nil {
		return fmt.Errorf("encoding subtree message for DLQ: %w", encErr)
	}
	// WithoutCancel: DLQ hand-off must survive a canceled consumer ctx.
	if pubErr := p.dlqProducer.Publish(context.WithoutCancel(ctx), subtreeMsg.Hash, data); pubErr != nil {
		return fmt.Errorf("publishing subtree message to DLQ: %w", pubErr)
	}
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
		metrics.ObserveDBBatchSize(metrics.StoreRegistration, metrics.OpBatchGet, len(uncached))
		t := metrics.StartDB(p.backendLabel(), metrics.StoreRegistration, metrics.OpBatchGet)
		registeredFromStore, err = p.registrationStore.BatchGet(uncached)
		t.End(err)
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
		metrics.ObserveDBBatchSize(metrics.StoreRegistration, metrics.OpBatchGet, len(cachedRegistered))
		t := metrics.StartDB(p.backendLabel(), metrics.StoreRegistration, metrics.OpBatchGet)
		cachedEntries, err := p.registrationStore.BatchGet(cachedRegistered)
		t.End(err)
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
func (p *Processor) emitBatchedSeenCallbacks(ctx context.Context, registeredTxids map[string][]store.CallbackEntry, subtreeID, peerID string) error {
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
	// payload stays comfortably under Kafka brokers' default message.max.bytes
	// (1MB). All chunks across all URLs go out in ONE batch publish
	// (throughput review F-6) instead of one broker-acked RTT per chunk.
	if err := p.emitSeenBatch(ctx, seenGroups, urlTokens, subtreeID, kafka.CallbackSeenOnNetwork, metrics.SeenKindOnNetwork); err != nil && firstErr == nil {
		firstErr = err
	}

	// 4.6: Peer-weighted SEEN_MULTIPLE_NODES scoring.
	// Warm-up: do not score until the node registry has a full tip window.
	// Unknown peers (weight 0) do not contribute.
	thresholdGroups := make(map[string][]string) // callbackURL → threshold-reached txids
	if p.nodeRegistry != nil && p.nodeRegistry.Ready() {
		weight := p.nodeRegistry.Weight(peerID)
		if weight > 0 && len(registeredTxids) > 0 {
			txids := make([]string, 0, len(registeredTxids))
			for txid := range registeredTxids {
				txids = append(txids, txid)
			}
			metrics.ObserveDBBatchSize(metrics.StoreSeenCounter, metrics.OpIncrement, len(txids))
			incStart := time.Now()
			results, incErr := p.seenCounterStore.BatchAddPeer(txids, peerID, weight)
			metrics.ObserveDB(p.backendLabel(), metrics.StoreSeenCounter, metrics.OpIncrement, incStart, incErr)
			if incErr != nil {
				p.Logger.Error("failed to batch-add peer to seen counters",
					logfields.SubtreeHash(subtreeID), "peerId", peerID, logfields.TxIDCount(len(txids)), "succeeded", len(results), "error", incErr)
				if firstErr == nil {
					firstErr = fmt.Errorf("adding peer to seen counters for subtree %s: %w", subtreeID, incErr)
				}
			}
			for txid, result := range results {
				if result.ThresholdReached {
					for _, e := range registeredTxids[txid] {
						thresholdGroups[e.URL] = append(thresholdGroups[e.URL], txid)
					}
				}
			}
		}
	} else {
		p.Logger.Debug("skipping SEEN_MULTIPLE_NODES scoring: node registry not ready or peer unknown",
			"peerId", peerID)
	}

	// Emit one batched SEEN_MULTIPLE_NODES per callbackURL, chunked to fit
	// broker limits, again as a single batch publish.
	if err := p.emitSeenBatch(ctx, thresholdGroups, urlTokens, subtreeID, kafka.CallbackSeenMultipleNodes, metrics.SeenKindMultipleNodes); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// emitSeenBatch encodes one CallbackTopicMessage per (callbackURL, txid-chunk)
// and publishes every message in a single batch produce. Encode failures are
// per-URL (remaining URLs still go out); a publish failure covers the whole
// batch and is surfaced for redelivery — the delivery-side dedup absorbs any
// records that landed.
func (p *Processor) emitSeenBatch(
	ctx context.Context,
	groups map[string][]string,
	urlTokens map[string]string,
	subtreeID string,
	cbType kafka.CallbackType,
	seenKind string,
) error {
	if len(groups) == 0 {
		return nil
	}

	var firstErr error
	type pending struct {
		callbackURL string
		chunk       []string
	}
	var meta []pending
	var entries []kafka.BatchEntry
	emitStart := time.Now()

	for callbackURL, txids := range groups {
		for _, chunk := range chunkTxIDs(txids, callbackBatchChunkSize) {
			msg := &kafka.CallbackTopicMessage{
				CallbackURL:   callbackURL,
				CallbackToken: urlTokens[callbackURL],
				Type:          cbType,
				SubtreeHash:   subtreeID,
				TxIDs:         chunk,
			}
			data, err := msg.Encode()
			if err != nil {
				p.Logger.Error("failed to encode batched seen callback",
					"type", cbType, logfields.CallbackURL(callbackURL), "error", err)
				if firstErr == nil {
					firstErr = fmt.Errorf("encoding %s for %s: %w", cbType, callbackURL, err)
				}
				continue
			}
			entries = append(entries, kafka.HashBatchEntry(msg.PartitionKey(), data))
			meta = append(meta, pending{callbackURL: callbackURL, chunk: chunk})
		}
	}

	pubErr := p.callbackProducer.PublishBatch(ctx, entries)
	if pubErr != nil {
		p.Logger.Error("failed to publish seen callback batch",
			"type", cbType, "count", len(entries), "error", pubErr)
		if firstErr == nil {
			firstErr = fmt.Errorf("publishing %s batch (%d messages): %w", cbType, len(entries), pubErr)
		}
	}

	// Observe per message with the batch duration amortized across entries so
	// the per-URL emit metric stays comparable with the previous serial path.
	if len(entries) > 0 {
		per := time.Since(emitStart) / time.Duration(len(entries))
		for _, m := range meta {
			metrics.ObserveSubtreeEmitSeen(m.callbackURL, seenKind, per)
		}
	}

	// Log matched txids only once the batch is durably published — a failed
	// publish is surfaced above and redriven by the caller, so logging success
	// here too would be misleading. One Info line per (callbackURL, chunk)
	// keeps a SEEN_ON_NETWORK/SEEN_MULTIPLE_NODES txid searchable in Coralogix
	// without having to reconstruct it from downstream callback delivery logs.
	if pubErr == nil {
		maxLog := p.seenTxidLogMax()
		for _, m := range meta {
			fields := []any{
				logfields.SubtreeHash(subtreeID),
				logfields.CallbackURL(m.callbackURL),
				"type", string(cbType),
				logfields.TxIDCount(len(m.chunk)),
			}
			truncated := false
			if maxLog > 0 {
				txids := m.chunk
				if len(txids) > maxLog {
					txids = txids[:maxLog]
					truncated = true
				}
				fields = append(fields, logfields.TxIDs(txids), logfields.TxIDsTruncated(truncated))
			}
			p.Logger.Info("seen callback batch published", fields...)
		}
	}

	return firstErr
}

// seenTxidLogMax returns the configured cap on how many matched txids are
// included on the SEEN batch-published log (see emitSeenBatch). 0 means log
// counts only. Falls back to 0 (counts-only) when cfg is nil, which unit
// tests that construct Processor via struct literal rely on.
func (p *Processor) seenTxidLogMax() int {
	if p.cfg == nil {
		return 0
	}
	return p.cfg.Subtree.SeenTxidLogMax
}

// backendLabel returns the store-backend label for DB metrics: "aerospike"
// or "sql" depending on cfg.Store.Backend. Falls back to "aerospike" (the
// default) when cfg is nil or unset, matching config.Load() behavior.
func (p *Processor) backendLabel() string {
	if p.cfg != nil && p.cfg.Store.Backend == config.BackendSQL {
		return metrics.BackendSQL
	}
	return metrics.BackendAerospike
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
