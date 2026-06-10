package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// defaultBatchMaxBytes is the broker's default max.message.bytes (1 MiB). It is
// used as the floor for ProducerBatchMaxBytes — see clampBatchMaxBytes.
const defaultBatchMaxBytes int32 = 1_048_576

// clampBatchMaxBytes ports teranode #660 (4c5ad1c190). Unlike Sarama's
// Producer.Flush.Bytes — an eager-flush *trigger* that never rejected larger
// records — franz's ProducerBatchMaxBytes is a HARD CAP: any record exceeding
// it is rejected with MESSAGE_TOO_LARGE. A naively mapped small/flush-style
// value would therefore reject every normal record in production.
//
// The rule: any requested value <= 1 MiB is treated as "use the safe broker
// default", NOT as a hard cap. Only an explicit value strictly above 1 MiB is
// honoured as a real batch-size override (capped at MaxInt32).
func clampBatchMaxBytes(requested int) int32 {
	if requested <= int(defaultBatchMaxBytes) {
		return defaultBatchMaxBytes
	}
	if requested > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(requested) //nolint:gosec // bounded above by the MaxInt32 check
}

// Publisher is the produce seam. The real implementation wraps a *kgo.Client;
// it is exported so tests in other packages can inject a fake (see
// NewTestProducer). Produce returns the assigned partition and offset.
type Publisher interface {
	Produce(key string, value []byte) (partition int32, offset int64, err error)
	Close() error
}

// kgoPublisher is the production Publisher backed by a franz-go client.
type kgoPublisher struct {
	client *kgo.Client
	topic  string

	closeMu sync.Mutex // teranode #720: guard against double Close
	closed  bool
}

// Produce synchronously publishes a single record, mirroring the previous
// sarama SyncProducer.SendMessage semantics.
func (k *kgoPublisher) Produce(key string, value []byte) (int32, int64, error) {
	rec := &kgo.Record{
		Topic: k.topic,
		Value: value, // raw []byte: franz takes no encoder (teranode #611)
	}
	// teranode #527: only set Key when non-empty. A typed-nil/empty key would
	// defeat the partitioner's keyless round-robin and pin traffic to one
	// partition. Leaving Key nil lets franz's default partitioner spread
	// keyless records; a non-empty key is hashed (matches sarama's
	// NewHashPartitioner behaviour).
	if key != "" {
		rec.Key = []byte(key)
	}

	res := k.client.ProduceSync(context.Background(), rec)
	if err := res.FirstErr(); err != nil {
		return 0, 0, err
	}
	r, err := res.First()
	if err != nil {
		return 0, 0, err
	}
	return r.Partition, r.Offset, nil
}

// Close flushes buffered records on a detached, bounded context (teranode #683
// drain pattern, so caller-context cancellation cannot drop buffered work) and
// then closes the client. Idempotent (teranode #720).
func (k *kgoPublisher) Close() error {
	k.closeMu.Lock()
	defer k.closeMu.Unlock()
	if k.closed {
		return nil
	}
	k.closed = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.client.Flush(ctx); err != nil {
		// best-effort: surface but still close the client below
		err = fmt.Errorf("producer flush on close (topic %s): %w", k.topic, err)
		k.client.Close()
		return err
	}
	k.client.Close()
	return nil
}

// Producer wraps a Kafka publisher with topic configuration. The public API
// (Publish / PublishWithHashKey / Close) is unchanged from the Sarama
// implementation; only the underlying client changed.
type Producer struct {
	pub    Publisher
	topic  string
	logger *slog.Logger
}

// NewProducer creates a new Kafka producer backed by franz-go.
func NewProducer(brokers []string, topic string, logger *slog.Logger) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// WaitForAll -> AllISRAcks: strongest consistency, preserves the prior
		// RequiredAcks=WaitForAll guarantee. franz keeps idempotent writes on by
		// default under these acks, so retries cannot duplicate.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Producer.Retry.Max = 3 in the Sarama config.
		kgo.RecordRetries(3),
		// Synchronous semantics: no added linger before a batch is sent.
		kgo.ProducerLinger(0),
		// Defense-in-depth: callback payloads are claim-checked (stump bytes live
		// in the blob store, Kafka carries only a reference) so messages should be
		// tiny, but raise the cap well above the 1 MiB broker default to avoid
		// silent regressions if future code inlines large data. Fed through the
		// clamp (teranode #660) so the cap can never accidentally fall to a value
		// that rejects normal records. Brokers must set message.max.bytes >= this.
		kgo.ProducerBatchMaxBytes(clampBatchMaxBytes(10 * 1024 * 1024)),
		// Default partitioner hashes a non-nil key and round-robins a nil key —
		// equivalent to sarama.NewHashPartitioner for our always-keyed produces.
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for topic %s: %w", topic, err)
	}

	return &Producer{
		pub:    &kgoPublisher{client: client, topic: topic},
		topic:  topic,
		logger: logger,
	}, nil
}

// Publish sends a message to the topic with the given partition key.
func (p *Producer) Publish(key string, value []byte) error {
	start := time.Now()
	partition, offset, err := p.pub.Produce(key, value)
	metrics.ObserveKafkaProduce(p.topic, len(value), time.Since(start), err)
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
	return p.pub.Close()
}

// HashPartitionKey generates a consistent hash key from a string (e.g., callback URL).
func HashPartitionKey(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash[:8])
}

// Int32FromHash converts first 4 bytes of hash to int32 for partition selection.
func Int32FromHash(s string) int32 {
	hash := sha256.Sum256([]byte(s))
	return int32(binary.BigEndian.Uint32(hash[:4])) //nolint:gosec // intentional wrapping for partition key
}
