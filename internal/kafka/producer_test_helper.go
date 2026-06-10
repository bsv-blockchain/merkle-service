package kafka

import (
	"log/slog"
)

// NewTestProducer creates a Producer with a custom Publisher for testing.
// Tests inject a fake Publisher implementing Produce/Close instead of standing
// up a real franz-go client.
func NewTestProducer(pub Publisher, topic string, logger *slog.Logger) *Producer {
	return &Producer{
		pub:    pub,
		topic:  topic,
		logger: logger,
	}
}
