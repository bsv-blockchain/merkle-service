package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

// exitFunc is the process-termination hook used when the consumer goroutine
// exits unexpectedly. Indirected through a package variable so tests can
// substitute it without taking down the test runner.
var exitFunc = func(code int) { os.Exit(code) }

// MessageHandler is called for each consumed message.
type MessageHandler func(ctx context.Context, msg *sarama.ConsumerMessage) error

// newConsumerConfig returns the sarama configuration used by every consumer
// group created by this package. It is extracted so unit tests can verify the
// invariants we care about (notably the F-031 initial-offset policy) without
// having to stand up a real Kafka broker.
func newConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	// F-031: start new consumer groups at the OLDEST available offset so a
	// group with no committed offsets (renamed group, lost offsets, fresh
	// environment) still processes the durable backlog instead of silently
	// jumping to the topic head and dropping queued work.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// F-053: surface sarama-level errors (broker disconnects, heartbeat
	// failures, generation expiration) on group.Errors(). The default of
	// false silently drops these and is the root cause of zombie consumers
	// where the pod stays Running but the broker reports the group as
	// Empty. Start() drains the channel and logs every error.
	config.Consumer.Return.Errors = true
	return config
}

// Consumer wraps a Sarama consumer group.
type Consumer struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler MessageHandler
	logger  *slog.Logger
	ready   chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewConsumer creates a new Kafka consumer group wrapper.
//
// Initial offset policy (F-031): a consumer group with no committed offsets
// starts at sarama.OffsetOldest so it processes every message already queued
// on the topic. The previous default of sarama.OffsetNewest silently skipped
// work whenever a group was renamed, its committed offsets were lost, or the
// service was deployed into a fresh environment with topics that already had
// durable backlogs (subtree, subtree-worker, block, callback). For the work
// topics this service consumes, replaying from the earliest available offset
// is always correct: the handlers are idempotent and the backlog must be
// processed, never dropped.
func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, logger *slog.Logger) (*Consumer, error) {
	config := newConsumerConfig()

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group %s: %w", groupID, err)
	}

	return &Consumer{
		group:   group,
		topics:  topics,
		handler: handler,
		logger:  logger,
		ready:   make(chan struct{}),
	}, nil
}

// Start begins consuming messages. Blocks until context is canceled.
func (c *Consumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

	// Drain group.Errors(). With Consumer.Return.Errors=true (set in
	// newConsumerConfig), every sarama-level error is surfaced here. Without
	// a reader, the channel buffer fills and sarama back-pressures or drops;
	// either way the operator gets no signal that the session is unhealthy.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-c.group.Errors():
				if !ok {
					return
				}
				c.logger.Error("sarama consumer group error", "error", err)
			}
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// F-053: if this goroutine exits without our context being canceled,
		// the consumer is no longer running but the process is. That's a
		// zombie state — the broker drops us from the group, no messages
		// are consumed, and k8s sees a Running pod with no restarts. Crash
		// the process so the orchestrator restarts a healthy replica.
		defer func() {
			if ctx.Err() == nil {
				c.logger.Error("consumer goroutine exited without context cancel; crashing process so k8s restarts the pod")
				exitFunc(1)
			}
		}()
		for {
			handler := &consumerGroupHandler{
				handler: c.handler,
				logger:  c.logger,
				ready:   c.ready,
			}
			if err := c.group.Consume(ctx, c.topics, handler); err != nil {
				c.logger.Error("consumer group error", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
			c.ready = make(chan struct{})
		}
	}()

	<-c.ready
	c.logger.Info("consumer ready", "topics", c.topics)
	return nil
}

// Stop gracefully shuts down the consumer.
func (c *Consumer) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return c.group.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	handler MessageHandler
	logger  *slog.Logger
	ready   chan struct{}
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim drives the per-partition message loop. On handler error we
// return the error WITHOUT calling MarkMessage, which:
//
//  1. Leaves the failed offset uncommitted so sarama re-delivers it in the
//     next session. Per-handler retry/DLQ logic (subtree-fetcher,
//     subtree-worker, block-processor, callback-delivery) classifies the
//     failure and either re-publishes for retry, routes to a DLQ, or returns
//     an error to deliberately stall the partition until the underlying
//     Kafka/storage problem is resolved.
//  2. Stops processing later messages in the same claim. The previous
//     implementation logged the error and continued; a later successful
//     message's MarkMessage call would then advance the committed offset
//     past the failed one, permanently dropping the failed work (F-030).
//
// Sarama treats a non-nil ConsumeClaim return as a session-level error and
// triggers a rebalance; the next session will resume from the last committed
// offset, which is the failed message's offset.
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := h.handler(session.Context(), msg); err != nil {
				h.logger.Error("failed to handle message, stopping claim to preserve offset",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
				)
				return err
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
