package kafka

import (
	"log/slog"

	"github.com/IBM/sarama"
)

// NewTestProducer creates a Producer with a custom sarama.SyncProducer for testing.
func NewTestProducer(sp sarama.SyncProducer, topic string, logger *slog.Logger) *Producer {
	return &Producer{
		producer: sp,
		topic:    topic,
		logger:   logger,
	}
}
