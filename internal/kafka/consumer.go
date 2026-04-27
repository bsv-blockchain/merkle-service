package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
)

// MessageHandler is called for each consumed message.
type MessageHandler func(ctx context.Context, msg *sarama.ConsumerMessage) error

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
func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, logger *slog.Logger) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

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

// Start begins consuming messages. Blocks until context is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
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

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := h.handler(session.Context(), msg); err != nil {
				h.logger.Error("failed to handle message",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
				)
				continue
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
