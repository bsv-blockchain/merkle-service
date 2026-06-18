package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// exitFunc is the process-termination hook used when the consumer goroutine
// exits unexpectedly. Indirected through a package variable so tests can
// substitute it without taking down the test runner.
var exitFunc = func(code int) { os.Exit(code) }

// MessageHandler is called for each consumed message.
type MessageHandler func(ctx context.Context, msg *Message) error

// handlerErrorBackoff is how long a partition worker waits before re-fetching
// a partition whose handler failed. It throttles the redeliver-and-fail cycle
// when the underlying problem (Aerospike blip, DLQ producer hiccup) persists.
// Under sarama the equivalent throttle was the session-teardown/rebalance
// cycle triggered by returning an error from ConsumeClaim.
const handlerErrorBackoff = 500 * time.Millisecond

// workerChannelDepth bounds how many fetched batches may queue per partition
// worker before the poll loop blocks dispatching to it (backpressure). Matches
// the franz-go goroutine-per-partition example. Sarama had the same shape: a
// bounded per-partition message channel that back-pressured the fetcher.
const workerChannelDepth = 5

// consumerOpts returns the franz-go client options used by every consumer group
// created by this package. Extracted so unit tests can verify the invariants we
// care about (notably the F-031 initial-offset policy and the explicit
// consumer timeouts) without standing up a real Kafka broker.
//
// Unlike sarama.NewConfig(), franz applies no implicit consumer timeouts on
// direct construction (teranode #633), so SessionTimeout / HeartbeatInterval /
// RebalanceTimeout / FetchMaxWait are all set explicitly here. Constraint:
// SessionTimeout must be >= 3x HeartbeatInterval.
func consumerOpts(brokers []string, groupID string, topics []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		// Preserve the prior round-robin assignment strategy. (A running group
		// cannot mix balancers, so this matches existing deployments.)
		kgo.Balancers(kgo.RoundRobinBalancer()),
		// F-031: start new consumer groups at the OLDEST available offset so a
		// group with no committed offsets (renamed group, lost offsets, fresh
		// environment) still processes the durable backlog instead of silently
		// jumping to the topic head and dropping queued work.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// F-030: commit only on handler success. Disable franz auto-commit and
		// commit each successfully-handled record explicitly (see
		// partitionWorker.process).
		kgo.DisableAutoCommit(),
		// Explicit timeout defaults sarama provided for free (teranode #633).
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(3 * time.Second),
		kgo.RebalanceTimeout(60 * time.Second),
		kgo.FetchMaxWait(100 * time.Millisecond),
		// Sarama parity: sarama's default config sets
		// Metadata.AllowAutoTopicCreation=true, so consumers of a
		// not-yet-existing topic triggered broker-side auto-creation and
		// received partitions once created. kgo defaults this OFF; without it
		// a consumer group over a missing topic joins with an empty
		// assignment and Start blocks on readiness forever.
		kgo.AllowAutoTopicCreation(),
	}
}

// topicPartition identifies one partition worker.
type topicPartition struct {
	topic     string
	partition int32
}

// Consumer wraps a franz-go consumer-group client and runs ONE worker
// goroutine per assigned partition — the same concurrency model sarama's
// ConsumerGroupHandler provided via per-claim ConsumeClaim goroutines. The
// poll loop only fetches and dispatches; all handler execution, offset
// commits, and failure rewinds happen on the per-partition workers, so a slow
// or failing handler on one partition never stalls the others.
type Consumer struct {
	client  *kgo.Client
	groupID string
	topics  []string
	handler MessageHandler
	logger  *slog.Logger

	readyOnce sync.Once
	ready     chan struct{}

	// workers is owned by the poll goroutine: partition-assigned/revoked/lost
	// callbacks run inside PollFetches (BlockRebalanceOnPoll), and the final
	// teardown runs on the poll goroutine after the loop exits, so no mutex is
	// needed.
	workers map[topicPartition]*partitionWorker

	cancelMu   sync.Mutex // teranode #638: guard the cancel func against races
	cancel     context.CancelFunc
	consumeCtx context.Context //nolint:containedctx // handed to workers created by rebalance callbacks
	wg         sync.WaitGroup

	closeMu sync.Mutex // teranode #720: guard against double Close
	closed  bool
}

// NewConsumer creates a new Kafka consumer group wrapper.
//
// Initial offset policy (F-031): a consumer group with no committed offsets
// starts at the OLDEST offset so it processes every message already queued on
// the topic. Starting at the newest offset silently skipped work whenever a
// group was renamed, its committed offsets were lost, or the service was
// deployed into a fresh environment with topics that already had durable
// backlogs (subtree, subtree-worker, block, callback). For the work topics
// this service consumes, replaying from the earliest available offset is always
// correct: the handlers are idempotent and the backlog must be processed, never
// dropped.
// partitions optionally maps a subscribed topic name to the partition count it
// should be created with (and grown to) by the startup EnsureTopics call; topics
// absent from the map default to 1. Pass nil to create/keep every subscribed
// topic at 1 partition.
func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, partitions map[string]int32, logger *slog.Logger) (*Consumer, error) {
	c := &Consumer{
		groupID: groupID,
		topics:  topics,
		handler: handler,
		logger:  logger,
		ready:   make(chan struct{}),
		workers: make(map[topicPartition]*partitionWorker),
	}

	// Ensure the subscribed topics exist (at their configured partition count)
	// before joining the group, so partition assignment is immediate rather than
	// waiting for a metadata refresh to discover a lazily-auto-created topic (see
	// EnsureTopics — this is what left /reprocess block messages unconsumed under
	// franz). Best-effort: a transient failure is logged, not fatal — the consumer
	// still works via metadata refresh + producer-side auto-creation, just less
	// promptly (and at the default partition count until a later start grows it).
	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if eErr := EnsureTopics(ensureCtx, brokers, topics, partitions, logger); eErr != nil && logger != nil {
		logger.Warn("could not pre-create consumer topics; relying on auto-create",
			"groupID", groupID, "topics", topics, "error", eErr)
	}
	cancel()

	opts := append(
		consumerOpts(brokers, groupID, topics),
		// Rebalances are processed only inside PollFetches, so the
		// assigned/revoked/lost callbacks below run on the poll goroutine and
		// partitions are never moved while a dispatch is in flight.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.partitionsAssigned),
		kgo.OnPartitionsRevoked(c.partitionsRevoked),
		kgo.OnPartitionsLost(c.partitionsLost),
	)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group %s: %w", groupID, err)
	}
	c.client = client
	return c, nil
}

// Start begins consuming messages. It returns once the consumer is ready; the
// poll loop runs in a background goroutine until the context is canceled or
// Stop is called.
func (c *Consumer) Start(parent context.Context) error {
	// teranode #638: create the cancel context here (not inside the goroutine)
	// and store the cancel func under a mutex to avoid a race with Stop.
	ctx, cancel := context.WithCancel(parent)
	c.cancelMu.Lock()
	c.cancel = cancel
	c.consumeCtx = ctx
	c.cancelMu.Unlock()

	// Shutdown watcher: when the consume context dies (Stop or parent cancel),
	// initiate the client close. The poll loop deliberately keeps polling until
	// IsClientClosed: with BlockRebalanceOnPoll, the leave-group triggered by
	// Close can only revoke partitions while polls are still allowing
	// rebalances — closing from a goroutine while the loop keeps polling is
	// the franz-documented clean-shutdown sequence. Exiting the loop first and
	// closing after deadlocks the leave forever.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-ctx.Done()
		_ = c.closeClient()
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// F-053: if this goroutine exits while the consumer is neither stopped
		// nor canceled, the consumer is no longer running but the process is.
		// That's a zombie state — the broker drops us from the group, no
		// messages are consumed, and k8s sees a Running pod with no restarts.
		// Crash the process so the orchestrator restarts a healthy replica.
		//
		// Crucially (teranode #636/#638), franz self-heals by closing and
		// reconnecting its client internally; PollFetches reporting
		// ErrClientClosed is therefore only ever OUR close (shutdown), never
		// spontaneous, so this guard never fires on franz's normal recovery.
		defer func() {
			if ctx.Err() == nil && !c.isClosed() {
				c.logger.Error("consumer goroutine exited without context cancel; crashing process so k8s restarts the pod")
				exitFunc(1)
			}
		}()
		// Tear down every partition worker once the loop exits (shutdown).
		// Runs on the poll goroutine, preserving single-threaded ownership of
		// the workers map.
		defer c.stopAllWorkers()

		c.pollLoop(ctx)
	}()

	<-c.ready
	c.logger.Info("consumer ready", "topics", c.topics)
	return nil
}

// pollLoop fetches records and dispatches each partition's batch to that
// partition's worker. It does no handler work itself: commits and failure
// rewinds live on the workers (see partitionWorker), which is what restores
// sarama's per-partition concurrency (one ConsumeClaim goroutine per claim).
//
// The loop exits ONLY when the client is closed, never on ctx alone: during
// shutdown the close-initiated group leave needs these polls to keep allowing
// rebalances (BlockRebalanceOnPoll) so partition revocation can complete. The
// shutdown watcher in Start translates ctx cancellation into a client close.
func (c *Consumer) pollLoop(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(context.Background())

		// teranode #636/#638: client-closed is franz's self-healing reconnect or
		// our own shutdown — recover, do not treat as a fatal goroutine exit.
		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			fatal := false
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, kgo.ErrClientClosed) {
					fatal = true
					continue
				}
				metrics.IncKafkaConsumerError(e.Topic, metrics.KafkaErrorBroker)
				c.logger.Error("franz fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			if fatal {
				return
			}
			// Transient fetch errors: let the client self-heal and poll again.
			c.client.AllowRebalance()
			continue
		}

		// First healthy poll signals readiness.
		c.signalReady()

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			w, ok := c.workers[topicPartition{p.Topic, p.Partition}]
			if !ok {
				// No worker (partition raced a revoke). Records are
				// uncommitted, so the new owner redelivers them.
				return
			}
			select {
			case w.recs <- p.Records:
			case <-ctx.Done():
			}
		})

		// With BlockRebalanceOnPoll, rebalances wait until we explicitly allow
		// them — after dispatch, so partitions never move mid-dispatch.
		c.client.AllowRebalance()
	}
}

// partitionsAssigned spawns one worker per newly assigned partition. Runs on
// the poll goroutine (BlockRebalanceOnPoll).
func (c *Consumer) partitionsAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.cancelMu.Lock()
	ctx := c.consumeCtx
	c.cancelMu.Unlock()
	if ctx == nil {
		// Not started yet; no polls happen before Start, so this is defensive.
		ctx = context.Background()
	}
	for topic, parts := range assigned {
		for _, part := range parts {
			tp := topicPartition{topic, part}
			if old, ok := c.workers[tp]; ok {
				old.stop()
			}
			w := newPartitionWorker(c, tp, ctx)
			c.workers[tp] = w
			go w.run()
		}
	}
	// Sarama parity: ConsumeClaim's Setup() signaled readiness on partition
	// assignment. Signaling here (not on first fetched records) keeps Start
	// from blocking when the assigned topic is empty.
	c.signalReady()
}

// partitionsRevoked stops the workers for revoked partitions and waits for
// them to finish their in-flight batch (whose successes they commit) before
// the rebalance hands the partitions to another member.
func (c *Consumer) partitionsRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.stopWorkers(revoked)
}

// partitionsLost is like revoked, but the partitions are already owned by
// others (session expiry, fencing) — workers are stopped; their final commits
// may fail, which is fine: uncommitted work is redelivered (at-least-once).
func (c *Consumer) partitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.stopWorkers(lost)
}

func (c *Consumer) stopWorkers(tps map[string][]int32) {
	var stopped []*partitionWorker
	for topic, parts := range tps {
		for _, part := range parts {
			tp := topicPartition{topic, part}
			if w, ok := c.workers[tp]; ok {
				w.signalStop()
				stopped = append(stopped, w)
				delete(c.workers, tp)
			}
		}
	}
	for _, w := range stopped {
		<-w.done
	}
}

func (c *Consumer) stopAllWorkers() {
	for tp, w := range c.workers {
		w.signalStop()
		delete(c.workers, tp)
		<-w.done
	}
}

// handleMetrics records the per-message metrics around a handler invocation.
// Split out so processBatch stays a pure, broker-free function for testing.
func (c *Consumer) handleMetrics(topic string, valueLen int) func(outcome string, dur time.Duration, handlerErr error) {
	metrics.ObserveKafkaConsumed(topic, c.groupID, valueLen)
	gauge := metrics.KafkaInFlight(topic, c.groupID)
	gauge.Inc()
	return func(outcome string, dur time.Duration, handlerErr error) {
		gauge.Dec()
		metrics.ObserveKafkaHandle(topic, outcome, dur)
		if handlerErr != nil {
			metrics.IncKafkaConsumerError(topic, metrics.KafkaErrorHandler)
		}
	}
}

// partitionWorker consumes one partition's record batches in order — the
// franz-go equivalent of one sarama ConsumeClaim goroutine. It owns the
// partition's commit and failure-rewind logic, so a handler stall or failure
// affects only this partition.
type partitionWorker struct {
	c    *Consumer
	tp   topicPartition
	ctx  context.Context //nolint:containedctx // worker lifetime == consume ctx
	recs chan []*kgo.Record

	quitOnce sync.Once
	quit     chan struct{}
	done     chan struct{}

	// discardUntil, when >= 0, marks a pending rewind: batches whose first
	// offset is GREATER than it were fetched before the rewind took effect and
	// must be dropped; the refetched batch starts exactly at discardUntil.
	// Only touched from the worker goroutine.
	discardUntil int64
}

func newPartitionWorker(c *Consumer, tp topicPartition, ctx context.Context) *partitionWorker {
	return &partitionWorker{
		c:            c,
		tp:           tp,
		ctx:          ctx,
		recs:         make(chan []*kgo.Record, workerChannelDepth),
		quit:         make(chan struct{}),
		done:         make(chan struct{}),
		discardUntil: -1,
	}
}

func (w *partitionWorker) signalStop() {
	w.quitOnce.Do(func() { close(w.quit) })
}

// stop signals and waits.
func (w *partitionWorker) stop() {
	w.signalStop()
	<-w.done
}

func (w *partitionWorker) run() {
	defer close(w.done)
	for {
		select {
		case <-w.quit:
			return
		case recs := <-w.recs:
			w.process(recs)
		}
	}
}

// process runs the handler over one in-order batch, commits the successful
// prefix, and rewinds the partition on the first failure (F-030: a failed
// record and everything after it in the partition are redelivered; committed
// offsets never advance past a failure).
func (w *partitionWorker) process(recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	if w.discardUntil >= 0 {
		if recs[0].Offset > w.discardUntil {
			return // stale batch fetched before the rewind took effect
		}
		w.discardUntil = -1
	}

	committable, failed := processBatch(w.ctx, recs, w.c.handler, w.c.handleMetrics, w.c.logger, w.c.groupID)

	if len(committable) > 0 {
		if err := w.c.client.CommitRecords(w.ctx, committable...); err != nil {
			// Commit failure leaves offsets uncommitted; on the next
			// rebalance/restart the group resumes from the last committed
			// offset, so already-handled records are simply redelivered
			// (at-least-once; handlers are idempotent).
			if !errors.Is(err, context.Canceled) {
				w.c.logger.Error("offset commit failed",
					"group", w.c.groupID, "topic", w.tp.topic, "partition", w.tp.partition, "error", err)
			}
		}
	}
	if failed != nil {
		w.rewind(failed)
	}
}

// rewind resets this partition's fetch position back to the failed record so
// it (and everything after it) is redelivered, then backs off before resuming.
//
// This is load-bearing for at-least-once delivery. Unlike sarama — where an
// uncommitted offset was automatically re-fetched after the session ended —
// kgo advances its in-memory fetch position as records are returned from
// PollFetches, independent of commits. Merely withholding the commit does NOT
// cause redelivery within a running session; without the rewind the failed
// record would be silently and permanently lost as soon as a later record
// committed. Sequence per franz-go guidance: pause (no new fetches mid-reset),
// SetOffsets to the failed record (purges records already buffered client-side
// for the partition), back off, resume. Only this partition stalls; sarama, by
// contrast, tore down the entire session.
func (w *partitionWorker) rewind(rec *kgo.Record) {
	w.c.logger.Warn(
		"rewinding partition to redeliver failed record",
		"group", w.c.groupID,
		"topic", rec.Topic,
		"partition", rec.Partition,
		"offset", rec.Offset,
	)

	paused := map[string][]int32{rec.Topic: {rec.Partition}}
	w.c.client.PauseFetchPartitions(paused)
	w.c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		rec.Topic: {rec.Partition: {Epoch: rec.LeaderEpoch, Offset: rec.Offset}},
	})
	w.discardUntil = rec.Offset

	select {
	case <-time.After(handlerErrorBackoff):
	case <-w.quit:
	case <-w.ctx.Done():
	}

	// Resume even when stopping: leaving a partition paused on a live client
	// would silently stop consumption; on a closing client it is a no-op.
	w.c.client.ResumeFetchPartitions(paused)
}

// processBatch runs the handler over one partition's records in order and
// returns the prefix of records that should be committed plus the first
// failed record (nil if none) — the rewind point for the partition.
//
// F-030 fidelity: a record is committable ONLY if its handler returned nil AND
// every earlier record in the batch also succeeded. On the first handler
// error the batch stops (no later record is handled or committed, and the
// failed record is reported for rewind), so a later success can never advance
// the committed offset past a failed one. Per-handler retry/DLQ logic
// (subtree-fetcher, subtree-worker, block-processor, callback-delivery)
// classifies the failure and either re-publishes for retry, routes to a DLQ,
// or returns an error to deliberately stall the partition until the underlying
// problem resolves — exactly as under sarama's "return error from
// ConsumeClaim, leave offset uncommitted" model.
//
// metricsHook may be nil (used by unit tests); when non-nil it is called once
// per record and returns a completion callback invoked after the handler.
func processBatch(
	ctx context.Context,
	recs []*kgo.Record,
	handler MessageHandler,
	metricsHook func(topic string, valueLen int) func(outcome string, dur time.Duration, handlerErr error),
	logger *slog.Logger,
	groupID string,
) (committable []*kgo.Record, failed *kgo.Record) {
	for _, rec := range recs {
		var done func(string, time.Duration, error)
		if metricsHook != nil {
			done = metricsHook(rec.Topic, len(rec.Value))
		}

		start := time.Now()
		err := handler(ctx, recordToMessage(rec))
		outcome := metrics.OutcomeSuccess
		if err != nil {
			outcome = metrics.OutcomeHandlerError
		}
		if done != nil {
			done(outcome, time.Since(start), err)
		}

		if err != nil {
			if logger != nil {
				logger.Error(
					"failed to handle message, rewinding partition to redeliver",
					"group", groupID,
					"topic", rec.Topic,
					"partition", rec.Partition,
					"offset", rec.Offset,
					"error", err,
				)
			}
			return committable, rec
		}
		committable = append(committable, rec)
	}
	return committable, nil
}

func (c *Consumer) signalReady() {
	c.readyOnce.Do(func() { close(c.ready) })
}

// Stop gracefully shuts down the consumer. Canceling the consume context
// unblocks any in-flight handlers AND triggers the shutdown watcher, which
// closes the client; the poll loop keeps polling until the close-initiated
// group leave completes (see Start), then exits and tears down the workers.
func (c *Consumer) Stop() error {
	c.cancelMu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
	return c.closeClient() // idempotent; normally already closed by the watcher
}

// closeClient closes the underlying franz client at most once (teranode #720).
func (c *Consumer) closeClient() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.client.Close()
	return nil
}

// isClosed reports whether closeClient has run (i.e. any client-closed signal
// observed by the poll loop is our own deliberate shutdown).
func (c *Consumer) isClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}
