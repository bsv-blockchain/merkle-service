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
		// commit each successfully-handled record explicitly (see commit).
		kgo.DisableAutoCommit(),
		// Explicit timeout defaults sarama provided for free (teranode #633).
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(3 * time.Second),
		kgo.RebalanceTimeout(60 * time.Second),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}
}

// Consumer wraps a franz-go consumer-group client.
type Consumer struct {
	client  *kgo.Client
	groupID string
	topics  []string
	handler MessageHandler
	logger  *slog.Logger

	readyOnce sync.Once
	ready     chan struct{}

	cancelMu sync.Mutex // teranode #638: guard the cancel func against races
	cancel   context.CancelFunc
	wg       sync.WaitGroup

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
func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, logger *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(consumerOpts(brokers, groupID, topics)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group %s: %w", groupID, err)
	}

	return &Consumer{
		client:  client,
		groupID: groupID,
		topics:  topics,
		handler: handler,
		logger:  logger,
		ready:   make(chan struct{}),
	}, nil
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
	c.cancelMu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// F-053: if this goroutine exits without our context being canceled, the
		// consumer is no longer running but the process is. That's a zombie
		// state — the broker drops us from the group, no messages are consumed,
		// and k8s sees a Running pod with no restarts. Crash the process so the
		// orchestrator restarts a healthy replica.
		//
		// Crucially (teranode #636/#638), franz self-heals by closing and
		// reconnecting its client internally; PollFetches reporting
		// ErrClientClosed / context.Canceled is therefore RECOVERY or shutdown,
		// never a fatal exit. The loop below only returns when ctx is canceled,
		// so this guard never fires on franz's normal recovery.
		defer func() {
			if ctx.Err() == nil {
				c.logger.Error("consumer goroutine exited without context cancel; crashing process so k8s restarts the pod")
				exitFunc(1)
			}
		}()

		c.pollLoop(ctx)
	}()

	<-c.ready
	c.logger.Info("consumer ready", "topics", c.topics)
	return nil
}

// handlerErrorBackoff is how long the poll loop waits before re-fetching a
// partition whose handler failed. It throttles the redeliver-and-fail cycle
// when the underlying problem (Aerospike blip, DLQ producer hiccup) persists.
// Under sarama the equivalent throttle was the session-teardown/rebalance
// cycle triggered by returning an error from ConsumeClaim.
const handlerErrorBackoff = 500 * time.Millisecond

// pollLoop is the franz PollFetches consume loop. It commits offsets only for
// records whose handler returned nil (F-030), and explicitly REWINDS the fetch
// position of any partition whose handler failed so the failed record is
// redelivered.
//
// The rewind is load-bearing for at-least-once delivery. Unlike sarama —
// where an uncommitted offset was automatically re-fetched after the session
// ended — kgo advances its in-memory fetch position as records are returned
// from PollFetches, independent of commits. Merely withholding the commit
// does NOT cause redelivery within a running session: the next poll continues
// past the failed batch, and a later successful commit would advance the
// committed offset past the failed record, losing it permanently. To preserve
// F-030/F-021 we pause the failed partition, SetOffsets back to the failed
// record (which also drops any already-buffered records for that partition),
// back off briefly, and resume.
func (c *Consumer) pollLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		fetches := c.client.PollFetches(ctx)

		// teranode #636/#638: client-closed is franz's self-healing reconnect or
		// our own shutdown — recover, do not treat as a fatal goroutine exit.
		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, kgo.ErrClientClosed) {
					return
				}
				metrics.IncKafkaConsumerError(e.Topic, metrics.KafkaErrorBroker)
				c.logger.Error("franz fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			// Transient fetch errors: let the client self-heal and poll again.
			continue
		}

		// First healthy poll signals readiness.
		c.signalReady()

		committable, failed := processRecords(ctx, fetches, c.handler, c.handleMetrics, c.logger, c.groupID)
		if len(committable) > 0 {
			if err := c.client.CommitRecords(ctx, committable...); err != nil {
				// Commit failure leaves offsets uncommitted; on the next
				// rebalance/restart the group resumes from the last committed
				// offset, so already-handled records are simply redelivered
				// (at-least-once; handlers are idempotent).
				if !errors.Is(err, context.Canceled) {
					c.logger.Error("offset commit failed", "group", c.groupID, "error", err)
				}
			}
		}
		if len(failed) > 0 {
			c.rewindFailed(ctx, failed)
		}
	}
}

// rewindFailed resets the fetch position of every partition in failed back to
// its first failed record so the record (and everything after it in that
// partition) is redelivered, then backs off briefly before resuming fetches.
//
// Sequence per franz-go guidance: pause the partitions first so no new fetch
// is issued mid-reset, SetOffsets to the failed record's offset (this purges
// records already buffered for those partitions), wait out the backoff, then
// resume. The backoff stalls this consumer's whole poll loop, which matches
// sarama's behavior of tearing down the entire session on a handler error.
func (c *Consumer) rewindFailed(ctx context.Context, failed []*kgo.Record) {
	paused := make(map[string][]int32, len(failed))
	rewind := make(map[string]map[int32]kgo.EpochOffset, len(failed))
	for _, rec := range failed {
		paused[rec.Topic] = append(paused[rec.Topic], rec.Partition)
		if rewind[rec.Topic] == nil {
			rewind[rec.Topic] = make(map[int32]kgo.EpochOffset)
		}
		rewind[rec.Topic][rec.Partition] = kgo.EpochOffset{
			Epoch:  rec.LeaderEpoch,
			Offset: rec.Offset,
		}
		c.logger.Warn(
			"rewinding partition to redeliver failed record",
			"group", c.groupID,
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
		)
	}

	c.client.PauseFetchPartitions(paused)
	c.client.SetOffsets(rewind)

	select {
	case <-time.After(handlerErrorBackoff):
	case <-ctx.Done():
	}

	// Resume even when ctx is done: Stop()/shutdown closes the client, and
	// leaving partitions paused on a live client after a transient cancel
	// would silently stop consumption.
	c.client.ResumeFetchPartitions(paused)
}

// handleMetrics records the per-message metrics around a handler invocation.
// Split out so processRecords stays a pure, broker-free function for testing.
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

// processRecords runs the handler over every fetched record in poll order and
// returns (a) the records that should be committed and (b) the FIRST failed
// record of each partition that had a handler error — the rewind point the
// caller must reset that partition's fetch position to (see rewindFailed).
//
// F-030 fidelity: a record is committable ONLY if its handler returned nil AND
// every earlier record in the SAME partition also succeeded. On the first
// handler error in a partition, that partition stops (no later record from it
// is committed, and the failed record is reported for rewind), so a later
// success can never advance the committed offset past a failed one. Per-handler
// retry/DLQ logic (subtree-fetcher, subtree-worker, block-processor,
// callback-delivery) classifies the failure and either re-publishes for retry,
// routes to a DLQ, or returns an error to deliberately stall the partition
// until the underlying problem resolves — exactly as under sarama's "return
// error from ConsumeClaim, leave offset uncommitted" model. The stall is only
// real because the caller rewinds: kgo's fetch position advances independently
// of commits, so without the rewind the failed record would never be re-polled
// in this session.
//
// metricsHook may be nil (used by unit tests); when non-nil it is called once
// per record and returns a completion callback invoked after the handler.
func processRecords(
	ctx context.Context,
	fetches kgo.Fetches,
	handler MessageHandler,
	metricsHook func(topic string, valueLen int) func(outcome string, dur time.Duration, handlerErr error),
	logger *slog.Logger,
	groupID string,
) (committable, failed []*kgo.Record) {
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, rec := range p.Records {
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
				// Stop this partition: do not commit this record or any later
				// record from the same partition (F-030), and report it as the
				// partition's rewind point.
				failed = append(failed, rec)
				return
			}
			committable = append(committable, rec)
		}
	})

	return committable, failed
}

func (c *Consumer) signalReady() {
	c.readyOnce.Do(func() { close(c.ready) })
}

// Stop gracefully shuts down the consumer.
func (c *Consumer) Stop() error {
	c.cancelMu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
	return c.closeClient()
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
