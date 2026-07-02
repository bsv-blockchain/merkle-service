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
//
// Record headers (notably a W3C traceparent) are deliberately NOT exposed
// here: trace context is extracted directly off the *kgo.Record by the
// consumer (see dispatchRecord) before the handler runs, so business code
// never needs them — and copying them onto every consumed Message would be
// an unused per-record slice allocation.
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
