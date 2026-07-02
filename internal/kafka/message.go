package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header is one Kafka record header (key/value pair). Mirrors
// kgo.RecordHeader's shape so callers never need to import franz-go directly
// to read headers off a consumed Message.
type Header struct {
	Key   string
	Value []byte
}

// Message is the merkle-owned consumed-message type handed to a MessageHandler.
//
// It deliberately replaces the previous *sarama.ConsumerMessage in the public
// handler contract so that no Kafka-client type leaks into business code. The
// field set mirrors exactly what handlers across the pipeline read
// (block/subtree/callback): Value plus Topic/Partition/Offset for logging and
// the callback-dedup key. Headers carries the record's raw Kafka headers
// (notably a W3C traceparent, when the producer injected one) — the consumer
// extracts trace context from these before invoking the handler; business
// code does not otherwise need to read them.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Headers   []Header
}

// recordToMessage converts a franz-go *kgo.Record into the package-local
// Message handed to handlers.
func recordToMessage(r *kgo.Record) *Message {
	var headers []Header
	if len(r.Headers) > 0 {
		headers = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	return &Message{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
		Headers:   headers,
	}
}
