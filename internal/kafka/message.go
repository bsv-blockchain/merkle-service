package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is the merkle-owned consumed-message type handed to a MessageHandler.
//
// It deliberately replaces the previous *sarama.ConsumerMessage in the public
// handler contract so that no Kafka-client type leaks into business code. The
// field set mirrors exactly what handlers across the pipeline read
// (block/subtree/callback): Value plus Topic/Partition/Offset for logging and
// the callback-dedup key.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// recordToMessage converts a franz-go *kgo.Record into the package-local
// Message handed to handlers.
func recordToMessage(r *kgo.Record) *Message {
	return &Message{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
	}
}
