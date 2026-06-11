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
// honored as a real batch-size override (capped at MaxInt32).
func clampBatchMaxBytes(requested int) int32 {
	if requested <= int(defaultBatchMaxBytes) {
		return defaultBatchMaxBytes
	}
	if requested > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(requested)
}

// Publisher is the produce seam. The real implementation wraps a *kgo.Client;
// it is exported so tests in other packages can inject a fake (see
// NewTestProducer). Produce returns the assigned partition and offset.
type Publisher interface {
	Produce(key string, value []byte) (partition int32, offset int64, err error)
	Close() error
}

// BatchEntry is one record of a PublishBatch call.
type BatchEntry struct {
	Key   string
	Value []byte
}

// batchPublisher is an optional Publisher capability: implementations that can
// publish many records in a single broker round-trip. Producer.PublishBatch
// type-asserts for it and falls back to per-record Produce otherwise, so test
// fakes only ever need the 2-method Publisher interface.
type batchPublisher interface {
	ProduceBatch(entries []BatchEntry) error
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
	// NewHashPartitioner behavior).
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

// ProduceBatch publishes every entry in ONE synchronous franz call. kgo packs
// the records into per-leader produce requests, collapsing the N broker-acked
// round-trips of a serial Publish loop into ~1 per leader (throughput review
// F-6: ProduceSync batches across concurrent callers, but the pipeline's
// fan-out loops are single-goroutine, so serial Publish never batched). Any
// record failure is reported via the returned error; callers treat the batch
// as retry-on-redelivery, which is safe because the downstream pipeline
// dedups (idempotent handlers, AttemptCount retry/DLQ).
func (k *kgoPublisher) ProduceBatch(entries []BatchEntry) error {
	recs := make([]*kgo.Record, len(entries))
	for i, e := range entries {
		rec := &kgo.Record{
			Topic: k.topic,
			Value: e.Value,
		}
		// teranode #527: leave Key nil when empty (see Produce).
		if e.Key != "" {
			rec.Key = []byte(e.Key)
		}
		recs[i] = rec
	}
	return k.client.ProduceSync(context.Background(), recs...).FirstErr()
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

// PublishBatch sends every entry in one synchronous call when the underlying
// publisher supports batching (the production kgo client), collapsing N
// broker-acked round-trips into ~1 per partition leader. Test publishers that
// only implement the 2-method Publisher interface fall back to a per-entry
// Publish loop, preserving capture/failure-injection behavior.
//
// Entry keys partition exactly like Publish: pre-hash with HashPartitionKey
// for the PublishWithHashKey equivalent. An error means one or more records
// may not be durable — callers must NOT ack the triggering Kafka message, so
// the whole (idempotent) batch is re-published on redelivery.
func (p *Producer) PublishBatch(entries []BatchEntry) error {
	if len(entries) == 0 {
		return nil
	}

	bp, ok := p.pub.(batchPublisher)
	if !ok {
		// Match the real batch semantics: attempt EVERY entry (kgo's
		// ProduceSync tries all records and reports the first failure), so one
		// bad entry doesn't suppress the rest on this attempt; return the
		// first error so the caller redelivers the (idempotent) batch.
		var firstErr error
		for _, e := range entries {
			if err := p.Publish(e.Key, e.Value); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	start := time.Now()
	err := bp.ProduceBatch(entries)
	// Amortize the batch duration across entries so per-message produce
	// latency metrics stay comparable with the serial path.
	per := time.Since(start) / time.Duration(len(entries))
	for _, e := range entries {
		metrics.ObserveKafkaProduce(p.topic, len(e.Value), per, err)
	}
	if err != nil {
		return fmt.Errorf("failed to publish batch of %d to %s: %w", len(entries), p.topic, err)
	}

	p.logger.Debug("published batch", "topic", p.topic, "count", len(entries))
	return nil
}

// HashBatchEntry builds a BatchEntry whose key is the SHA256-derived partition
// key for partitionKey — the batch equivalent of PublishWithHashKey.
func HashBatchEntry(partitionKey string, value []byte) BatchEntry {
	return BatchEntry{Key: HashPartitionKey(partitionKey), Value: value}
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
