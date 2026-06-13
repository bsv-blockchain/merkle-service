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
		// Restore implicit broker-side topic auto-creation (sarama defaulted
		// Metadata.AllowAutoTopicCreation=true). merkle-service does not create
		// its topics in production code and relies on broker auto-create; without
		// this a consumer group on a fresh broker never triggers creation of a
		// topic that no producer has touched yet. Matches the producer option.
		kgo.AllowAutoTopicCreation(),
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
	// Ensure the subscribed topics exist before joining the group, so partition
	// assignment is immediate rather than waiting for a metadata refresh to
	// discover a lazily-auto-created topic (see EnsureTopics — this is what left
	// /reprocess block messages unconsumed under franz). Best-effort: a transient
	// failure is logged, not fatal — the consumer still works via metadata
	// refresh + producer-side auto-creation, just less promptly.
	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if eErr := EnsureTopics(ensureCtx, brokers, topics, logger); eErr != nil && logger != nil {
		logger.Warn("could not pre-create consumer topics; relying on auto-create",
			"groupID", groupID, "topics", topics, "error", eErr)
	}
	cancel()

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

	// Wait for readiness, but honor the caller's context. franz self-heals an
	// unreachable broker by retrying the dial indefinitely, so PollFetches (and
	// therefore signalReady) may never fire; without this guard Start would
	// block forever even after the caller's context is canceled or times out.
	select {
	case <-c.ready:
		c.logger.Info("consumer ready", "topics", c.topics)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("consumer start canceled before ready: %w", ctx.Err())
	}
}

// pollLoop is the franz PollFetches consume loop. It commits offsets only for
// records whose handler returned nil (F-030).
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

		committable := processRecords(ctx, fetches, c.handler, c.handleMetrics, c.logger, c.groupID)
		if len(committable) > 0 {
			if err := c.client.CommitRecords(ctx, committable...); err != nil {
				// Commit failure leaves offsets uncommitted; the records will be
				// redelivered on the next poll/rebalance (at-least-once).
				if !errors.Is(err, context.Canceled) {
					c.logger.Error("offset commit failed", "group", c.groupID, "error", err)
				}
			}
		}
	}
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
// returns the records that should be committed.
//
// F-030 fidelity: a record is committable ONLY if its handler returned nil AND
// every earlier record in the SAME partition also succeeded. On the first
// handler error in a partition, that partition stops (no later record from it
// is committed), so a later success can never advance the committed offset past
// a failed one. Per-handler retry/DLQ logic (subtree-fetcher, subtree-worker,
// block-processor, callback-delivery) classifies the failure and either
// re-publishes for retry, routes to a DLQ, or returns an error to deliberately
// stall the partition until the underlying problem resolves — exactly as under
// sarama's "return error from ConsumeClaim, leave offset uncommitted" model.
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
) []*kgo.Record {
	var committable []*kgo.Record

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
						"failed to handle message, leaving offset uncommitted",
						"group", groupID,
						"topic", rec.Topic,
						"partition", rec.Partition,
						"offset", rec.Offset,
						"error", err,
					)
				}
				// Stop this partition: do not commit this record or any later
				// record from the same partition (F-030).
				return
			}
			committable = append(committable, rec)
		}
	})

	return committable
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
