package kafka

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

// Producer wraps a Sarama sync producer with topic and partition configuration.
type Producer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *slog.Logger
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string, topic string, logger *slog.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Partitioner = sarama.NewHashPartitioner
	// Defense-in-depth: callback payloads are claim-checked (stump bytes live in
	// the blob store, Kafka carries only a reference) so messages should be tiny,
	// but raise the cap well above Sarama's ~1MB default to avoid silent
	// regressions if future code accidentally inlines large data. Brokers must be
	// configured with message.max.bytes >= this value.
	config.Producer.MaxMessageBytes = 10 * 1024 * 1024

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for topic %s: %w", topic, err)
	}

	return &Producer{
		producer: producer,
		topic:    topic,
		logger:   logger,
	}, nil
}

// Publish sends a message to the topic with the given partition key.
func (p *Producer) Publish(key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", p.topic, err)
	}

	p.logger.Debug("published message", "topic", p.topic, "partition", partition, "offset", offset, "key", key)
	return nil
}

// PublishWithHashKey sends a message using a SHA256 hash of the key for partitioning.
// Useful for callback URL-based partitioning.
func (p *Producer) PublishWithHashKey(key string, value []byte) error {
	hash := sha256.Sum256([]byte(key))
	hashKey := fmt.Sprintf("%x", hash[:8])
	return p.Publish(hashKey, value)
}

// Close closes the producer.
func (p *Producer) Close() error {
	return p.producer.Close()
}

// HashPartitionKey generates a consistent hash key from a string (e.g., callback URL).
func HashPartitionKey(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash[:8])
}

// Int32FromHash converts first 4 bytes of hash to int32 for partition selection.
func Int32FromHash(s string) int32 {
	hash := sha256.Sum256([]byte(s))
	return int32(binary.BigEndian.Uint32(hash[:4]))
}
