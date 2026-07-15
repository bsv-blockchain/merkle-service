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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// exitFunc is the process-termination hook used when the consumer goroutine
// exits unexpectedly. Indirected through a package variable so tests can
// substitute it without taking down the test runner.
var exitFunc = func(code int) { os.Exit(code) }

// MessageHandler is called for each consumed message.
type MessageHandler func(ctx context.Context, msg *Message) error

// handlerErrorBackoff is how long a partition whose handler failed stays
// paused before its fetches resume (and how long its worker sleeps before
// taking more batches). It throttles the redeliver-and-fail cycle when the
// underlying problem (Aerospike blip, DLQ producer hiccup) persists.
// Under sarama the equivalent throttle was the session-teardown/rebalance
// cycle triggered by returning an error from ConsumeClaim.
const handlerErrorBackoff = 500 * time.Millisecond

// workerChannelDepth bounds how many fetched batches may queue per partition
// worker before the poll loop blocks dispatching to it (backpressure). Matches
// the franz-go goroutine-per-partition example. Sarama had the same shape: a
// bounded per-partition message channel that back-pressured the fetcher.
const workerChannelDepth = 5

// commitEvery is how many records a partition worker handles before it
// commits the successful prefix of its in-flight batch. Commits used to
// happen only after an ENTIRE fetched batch completed; under the dev-ovh-1
// backlog (50k messages, 1-2s per record on DataHub fetches) a single batch
// took hours, the committed offset stayed frozen for 40+ minutes while
// workers demonstrably processed work, and every pod restart reprocessed the
// whole batch from scratch. Committing every commitEvery records bounds the
// redelivery window to at most commitEvery records per partition without
// measurably increasing broker commit load (one OffsetCommit per ~50 handler
// calls). Prefix-commit semantics (F-030) are unchanged: a chunk's commit
// still only covers records whose handler succeeded, in order.
const commitEvery = 50

// fetchMaxPartitionBytes / fetchMaxBytes bound how much data one poll may
// return per partition and in total. kgo's defaults (1 MiB per partition,
// 50 MiB total) let a deep backlog of small subtree announcements (~250 B
// each) hand a worker thousands of records in a single batch — hours of work
// at 1-2s per record. Bounding the fetch keeps dispatched batches to at most
// a few hundred records so rewinds discard less and buffered memory stays at
// worst workerChannelDepth x fetchMaxPartitionBytes per partition. A single
// record/batch larger than the partition cap is still returned whole (kgo
// guarantees progress), so large callback-topic records cannot wedge a
// partition.
const (
	fetchMaxPartitionBytes int32 = 256 << 10 // 256 KiB
	fetchMaxBytes          int32 = 8 << 20   // 8 MiB
)

// pollTick bounds how long a single PollFetches call may block waiting for
// records. The poll loop must keep turning even when nothing is fetchable —
// every partition paused mid-rewind, or simply no traffic — because rewind
// application and resumption run on the poll goroutine between polls (see
// applyRewinds). Without a deadline an all-paused consumer would sit in
// PollFetches forever and never resume its partitions.
const pollTick = 250 * time.Millisecond

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
		// Bound per-poll fetch sizes so a deep backlog cannot hand a worker an
		// hours-long batch (see fetchMaxPartitionBytes).
		kgo.FetchMaxPartitionBytes(fetchMaxPartitionBytes),
		kgo.FetchMaxBytes(fetchMaxBytes),
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
// ConsumerGroupHandler provided via per-claim ConsumeClaim goroutines.
// Handler execution and offset commits happen on the per-partition workers,
// so a slow or failing handler on one partition never stalls the others; the
// poll loop fetches, dispatches, and applies worker-recorded failure rewinds
// (see rewindReqs / applyRewinds).
type Consumer struct {
	client  *kgo.Client
	groupID string
	topics  []string
	handler MessageHandler
	logger  *slog.Logger

	// commitRecords is the offset-commit seam: kgo.Client.CommitRecords in
	// production, a capture in unit tests (same indirection pattern as
	// exitFunc). Partition workers call it for every committed chunk.
	commitRecords func(ctx context.Context, recs ...*kgo.Record) error

	readyOnce sync.Once
	ready     chan struct{}

	// workers is owned by the poll goroutine: partition-assigned/revoked/lost
	// callbacks run inside PollFetches (BlockRebalanceOnPoll), and the final
	// teardown runs on the poll goroutine after the loop exits, so no mutex is
	// needed.
	workers map[topicPartition]*partitionWorker

	// rewindReqs holds the failed record each partition worker wants its
	// partition rewound to. Workers only RECORD the request (under rewindMu);
	// the client calls that perform it — PauseFetchPartitions, SetOffsets,
	// ResumeFetchPartitions — run on the poll goroutine in applyRewinds.
	// kgo's SetOffsets mutates the consumer's partition assignment and its
	// docs require callers to block concurrent revokes while issuing it;
	// calling it from worker goroutines (as this consumer originally did)
	// raced group rebalances and could deadlock the whole consumer: poll
	// goroutine blocked dispatching, rebalance waiting on the poll, worker
	// waiting inside SetOffsets on state the rebalance owns. On the poll
	// goroutine, between a PollFetches return and AllowRebalance, rebalances
	// are provably quiescent (BlockRebalanceOnPoll) — the exact window the
	// docs ask for.
	rewindMu   sync.Mutex
	rewindReqs map[topicPartition]*kgo.Record

	// resumeAt schedules the post-rewind resume (handlerErrorBackoff after
	// the rewind is applied) per paused partition. Poll-goroutine-owned like
	// workers; no mutex needed.
	resumeAt map[topicPartition]time.Time

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
	// A nil logger is replaced with slog.Default(): Start, the poll loop, and
	// the partition workers log unconditionally, so accepting nil here without
	// defaulting would panic at the first consumed message.
	if logger == nil {
		logger = slog.Default()
	}

	c := &Consumer{
		groupID:    groupID,
		topics:     topics,
		handler:    handler,
		logger:     logger,
		ready:      make(chan struct{}),
		workers:    make(map[topicPartition]*partitionWorker),
		rewindReqs: make(map[topicPartition]*kgo.Record),
		resumeAt:   make(map[topicPartition]time.Time),
	}

	// Ensure the subscribed topics exist (at their configured partition count)
	// before joining the group, so partition assignment is immediate rather than
	// waiting for a metadata refresh to discover a lazily-auto-created topic (see
	// EnsureTopics — this is what left /reprocess block messages unconsumed under
	// franz). Best-effort: a transient failure is logged, not fatal — the consumer
	// still works via metadata refresh + producer-side auto-creation, just less
	// promptly (and at the default partition count until a later start grows it).
	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if eErr := EnsureTopics(ensureCtx, brokers, topics, partitions, logger); eErr != nil {
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
	c.commitRecords = client.CommitRecords
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
// partition's worker. It does no handler work itself: handler execution and
// commits live on the workers (see partitionWorker), which is what restores
// sarama's per-partition concurrency (one ConsumeClaim goroutine per claim).
// Failure rewinds are recorded by the workers but APPLIED here, between a
// poll return and AllowRebalance (see applyRewinds).
//
// The loop exits ONLY when the client is closed, never on ctx alone: during
// shutdown the close-initiated group leave needs these polls to keep allowing
// rebalances (BlockRebalanceOnPoll) so partition revocation can complete. The
// shutdown watcher in Start translates ctx cancellation into a client close.
func (c *Consumer) pollLoop(ctx context.Context) {
	for {
		// Deadline-bounded poll (see pollTick): the loop must keep turning to
		// apply and resume rewinds even when nothing is fetchable.
		pollCtx, cancelPoll := context.WithTimeout(context.Background(), pollTick)
		fetches := c.client.PollFetches(pollCtx)
		cancelPoll()

		// teranode #636/#638: client-closed is franz's self-healing reconnect or
		// our own shutdown — recover, do not treat as a fatal goroutine exit.
		if fetches.IsClientClosed() {
			return
		}

		// A zero-length Fetches is kgo's one poll return that does NOT hold
		// the rebalance-blocking poller (its fill raced a rebalance that
		// drained the readied buffers and un-registered the poller), so the
		// post-poll window is NOT revoke-safe here. Skip rewind application
		// until the next tick, at most pollTick away.
		if len(fetches) == 0 {
			c.signalReady()
			c.client.AllowRebalance()
			continue
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			fatal := false
			tickOnly := true
			for _, e := range errs {
				// Our own pollTick deadline: an idle tick, not a broker error.
				// Only the fake fetch kgo injects for the poll context (no
				// topic, partition -1) qualifies — a real topic/partition
				// error that merely wraps DeadlineExceeded (e.g. an
				// offset-validation timeout) must still be counted and
				// logged below.
				if e.Topic == "" && e.Partition < 0 && errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				tickOnly = false
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
			if !tickOnly {
				// Transient fetch errors: let the client self-heal and poll again.
				c.applyRewinds()
				c.client.AllowRebalance()
				continue
			}
			// Idle tick: fall through — rewind application and AllowRebalance
			// below still run.
		} else {
			// First healthy poll signals readiness.
			c.signalReady()
		}

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

		// Apply worker-recorded rewinds while rebalances are still blocked
		// (post-poll, pre-AllowRebalance) — the only window where SetOffsets
		// cannot race a revoke. See rewindReqs.
		c.applyRewinds()

		// With BlockRebalanceOnPoll, rebalances wait until we explicitly allow
		// them — after dispatch, so partitions never move mid-dispatch.
		c.client.AllowRebalance()
	}
}

// applyRewinds performs the client side of every pending rewind and resumes
// partitions whose post-rewind backoff has elapsed. MUST run on the poll
// goroutine between a PollFetches return and AllowRebalance: rebalances are
// blocked there, satisfying SetOffsets' requirement that no revoke runs
// concurrently, and giving safe access to the poll-goroutine-owned workers
// and resumeAt maps.
func (c *Consumer) applyRewinds() {
	c.rewindMu.Lock()
	var reqs map[topicPartition]*kgo.Record
	if len(c.rewindReqs) > 0 {
		reqs = c.rewindReqs
		c.rewindReqs = make(map[topicPartition]*kgo.Record)
	}
	c.rewindMu.Unlock()

	for tp, rec := range reqs {
		if _, ok := c.workers[tp]; !ok {
			// Partition revoked after the worker recorded the request; the
			// new owner resumes from the last committed offset, which is
			// before the failed record (commits never advance past a
			// failure), so redelivery still happens — there.
			continue
		}
		c.logger.Warn(
			"rewinding partition to redeliver failed record",
			"group", c.groupID,
			"topic", tp.topic,
			"partition", tp.partition,
			"offset", rec.Offset,
		)
		paused := map[string][]int32{tp.topic: {tp.partition}}
		c.client.PauseFetchPartitions(paused)
		c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
			tp.topic: {tp.partition: {Epoch: rec.LeaderEpoch, Offset: rec.Offset}},
		})
		c.resumeAt[tp] = time.Now().Add(handlerErrorBackoff)
	}

	if len(c.resumeAt) == 0 {
		return
	}
	now := time.Now()
	for tp, at := range c.resumeAt {
		if now.Before(at) {
			continue
		}
		c.client.ResumeFetchPartitions(map[string][]int32{tp.topic: {tp.partition}})
		delete(c.resumeAt, tp)
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
	// Drop rewind state for the departing partitions and un-pause them:
	// pause is client-local, so a partition revoked mid-backoff and later
	// re-assigned to this client would otherwise never fetch again. This
	// runs AFTER the workers have fully drained: a worker caught
	// mid-process by signalStop can still queue one final rewind on its
	// way out, and cleaning before the wait would leave that stale request
	// to be applied if the partition were promptly re-assigned here.
	// Serialized with applyRewinds by BlockRebalanceOnPoll, so touching
	// resumeAt is safe; Resume on a never-paused client is a no-op.
	for topic, parts := range tps {
		for _, part := range parts {
			tp := topicPartition{topic, part}
			c.rewindMu.Lock()
			delete(c.rewindReqs, tp)
			c.rewindMu.Unlock()
			if _, paused := c.resumeAt[tp]; paused {
				delete(c.resumeAt, tp)
				c.client.ResumeFetchPartitions(map[string][]int32{tp.topic: {tp.partition}})
			}
		}
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
// partition's commit logic and records failure rewinds (applied on the poll
// goroutine), so a handler stall or failure affects only this partition.
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

// process runs the handler over one in-order batch in chunks of commitEvery
// records, committing each chunk's successful prefix as it completes, and
// rewinds the partition on the first failure (F-030: a failed record and
// everything after it in the partition are redelivered; committed offsets
// never advance past a failure).
//
// Chunked commits are the fix for the dev-ovh-1 commit freeze (see
// commitEvery): progress becomes durable DURING a long batch, not only after
// it, so a restart or rebalance mid-batch redelivers at most the un-committed
// tail instead of the whole batch. Between chunks the worker also checks for
// a stop signal (revoke/lost/shutdown) and abandons the remaining chunks —
// their records are uncommitted and are redelivered to the partition's next
// owner.
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

	for start := 0; start < len(recs); start += commitEvery {
		end := start + commitEvery
		if end > len(recs) {
			end = len(recs)
		}

		committable, failed := processBatch(w.ctx, recs[start:end], w.c.handler, w.c.handleMetrics, w.c.logger, w.c.groupID)

		if len(committable) > 0 {
			w.commit(committable)
		}
		if failed != nil {
			w.rewind(failed)
			return // later chunks are redelivered after the rewind
		}

		// Stop between chunks when the worker is quitting: the uncommitted
		// tail is redelivered, and the revoke/shutdown that signaled us is
		// not held for the rest of a potentially huge batch.
		select {
		case <-w.quit:
			return
		case <-w.ctx.Done():
			return
		default:
		}
	}
}

// commit commits one in-order chunk of successfully-handled records. A commit
// failure leaves the offsets uncommitted; on the next rebalance/restart the
// group resumes from the last committed offset, so already-handled records
// are simply redelivered (at-least-once; handlers are idempotent). That makes
// a failed commit WARN-worthy, not fatal — but it MUST be visible: on
// dev-ovh-1, members fenced during slow processing kept working partitions
// they no longer owned and their rejected commits were the only signal.
func (w *partitionWorker) commit(recs []*kgo.Record) {
	if err := w.c.commitRecords(w.ctx, recs...); err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown/lost-partition teardown, not a broker rejection
		}
		w.c.logger.Warn("offset commit failed; records will be redelivered",
			"group", w.c.groupID,
			"topic", w.tp.topic,
			"partition", w.tp.partition,
			"firstOffset", recs[0].Offset,
			"lastOffset", recs[len(recs)-1].Offset,
			"error", err,
		)
	}
}

// rewind records that this partition's fetch position must be reset to the
// failed record so it (and everything after it) is redelivered, then backs
// off before processing more batches.
//
// This is load-bearing for at-least-once delivery. Unlike sarama — where an
// uncommitted offset was automatically re-fetched after the session ended —
// kgo advances its in-memory fetch position as records are returned from
// PollFetches, independent of commits. Merely withholding the commit does NOT
// cause redelivery within a running session; without the rewind the failed
// record would be silently and permanently lost as soon as a later record
// committed.
//
// The worker only RECORDS the request; the pause/SetOffsets/resume sequence
// runs on the poll goroutine (applyRewinds), where rebalances are provably
// blocked. Issuing SetOffsets from here — as this method originally did —
// violated kgo's documented requirement to block concurrent revokes and
// could deadlock the consumer under a rebalance-during-rewind-storm (the
// dev-ovh-1 subtree-fetcher wedge of 8 Jul 2026). Batches fetched between
// recording and application are dropped by the discardUntil guard, so the
// deferred application does not weaken redelivery.
func (w *partitionWorker) rewind(rec *kgo.Record) {
	w.discardUntil = rec.Offset
	w.c.queueRewind(topicPartition{rec.Topic, rec.Partition}, rec)

	// Throttle the redeliver-and-fail cycle while the underlying problem
	// (Aerospike blip, DLQ hiccup) persists; abandoned early on stop.
	select {
	case <-time.After(handlerErrorBackoff):
	case <-w.quit:
	case <-w.ctx.Done():
	}
}

// queueRewind records the failed record a partition must be rewound to,
// keeping the earliest offset if one is already pending (defensive — the
// worker is serial, so two pending requests for one partition should not
// happen). Safe from any goroutine; applied by the poll goroutine.
func (c *Consumer) queueRewind(tp topicPartition, rec *kgo.Record) {
	c.rewindMu.Lock()
	defer c.rewindMu.Unlock()
	if existing, ok := c.rewindReqs[tp]; ok && existing.Offset <= rec.Offset {
		return
	}
	c.rewindReqs[tp] = rec
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
		err := dispatchRecord(ctx, rec, handler)
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

// dispatchRecord extracts any inbound trace context carried by rec's Kafka
// headers (a producer-injected traceparent — see injectTraceContext) and,
// only when that context is valid, runs the handler inside a
// SpanKindConsumer span named "<topic> process" (topic only — never an
// offset/key — to keep span names low-cardinality), recording the handler's
// error on the span before ending it.
//
// Zero-cost when disabled (mirrors the producer's injectTraceContext
// IsValid guard): extractTraceContext short-circuits without touching the
// propagator when the record carries no headers, and when telemetry is
// disabled fleet-wide no producer injects a traceparent, so records arrive
// header-less → the extracted SpanContext is invalid → no span is started
// and the only per-record cost is the (pre-existing) *Message alloc. See
// TestDispatchRecord_NoAllocBeyondHandlerOnNoContextPath.
//
// Semantics: a record with no inbound trace context cannot be correlated
// with any trace, so it is intentionally NOT spanned — even when telemetry
// is enabled (e.g. a record from an un-instrumented producer, or produced
// before this shipped). When a producer DID inject context, the consumer
// span continues that trace, and any Kafka produce or outbound HTTP call
// the handler makes with msgCtx continues it further — which is what closes
// the arcade->merkle->arcade trace across a hop through Kafka.
func dispatchRecord(ctx context.Context, rec *kgo.Record, handler MessageHandler) error {
	msgCtx := extractTraceContext(ctx, rec)
	if !trace.SpanContextFromContext(msgCtx).IsValid() {
		// No inbound trace context to correlate with: run the handler
		// directly, no consumer span. This is the disabled/no-context hot
		// path and must stay allocation-free beyond the handler itself.
		return handler(msgCtx, recordToMessage(rec))
	}

	msgCtx, span := otel.Tracer(tracerName).Start(
		msgCtx,
		rec.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(rec.Topic),
		),
	)
	defer span.End()

	err := handler(msgCtx, recordToMessage(rec))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
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
