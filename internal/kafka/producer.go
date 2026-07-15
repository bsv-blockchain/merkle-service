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
//
// ctx is used both as the franz-go ProduceSync context (so a caller's
// cancellation can abort the broker round-trip) and as the source of any
// active OTEL span injected into the record's headers — see Producer.Publish
// for the WithoutCancel guidance retry/DLQ callers must follow.
type Publisher interface {
	Produce(ctx context.Context, key string, value []byte) (partition int32, offset int64, err error)
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
	ProduceBatch(ctx context.Context, entries []BatchEntry) error
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
//
// ctx is passed straight to franz's ProduceSync (so caller cancellation
// aborts the broker round-trip) and, when it carries a valid OTEL span, its
// trace context is injected into the record's Kafka headers before sending —
// see injectTraceContext for the zero-cost-when-disabled guarantee.
func (k *kgoPublisher) Produce(ctx context.Context, key string, value []byte) (int32, int64, error) {
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
	injectTraceContext(ctx, rec)

	res := k.client.ProduceSync(ctx, rec)
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
//
// The same ctx is used for every record in the batch — both as the
// ProduceSync context and, when it carries a valid span, as the trace
// context injected into every record's headers (see injectTraceContext).
func (k *kgoPublisher) ProduceBatch(ctx context.Context, entries []BatchEntry) error {
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
		injectTraceContext(ctx, rec)
		recs[i] = rec
	}
	return k.client.ProduceSync(ctx, recs...).FirstErr()
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

// NewProducer creates a new Kafka producer backed by franz-go. A nil logger
// is replaced with slog.Default(): Publish and PublishBatch log unconditionally,
// so accepting nil here without defaulting would turn the first publish into a
// panic.
//
// retentionMs optionally maps topic names to the retention.ms a topic should
// be CREATED with (source: KafkaConfig.TopicRetention); existing topics are
// never altered. This matters most here: the DLQ topics are producer-only, so
// this call is the only place they are ever created — created with nil
// configs on a fresh cluster they inherit the broker default retention
// (15 minutes on dev-ovh-1), silently expiring dead letters. Pass nil only in
// unit tests.
func NewProducer(brokers []string, topic string, retentionMs map[string]int64, logger *slog.Logger) (*Producer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	// Ensure the target topic exists before the first publish, exactly as
	// NewConsumer does for its subscribed topics. Consumers only pre-create
	// what they consume, so a producer-only topic (a DLQ) otherwise depends
	// on broker-side auto-creation — and with that disabled every publish
	// fails with UNKNOWN_TOPIC_OR_PARTITION. That is how the subtree-fetcher
	// collapsed on dev-ovh-1: its disk-full overflow path had nowhere to
	// park failures (subtree-dlq was never created), so every one fell
	// through to the partition rewind path instead. Best-effort, matching
	// NewConsumer: a transient failure is logged, not fatal — auto-creating
	// brokers still work without it.
	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if eErr := EnsureTopics(ensureCtx, brokers, []string{topic}, nil, retentionMs, logger); eErr != nil {
		logger.Warn("could not pre-create producer topic; relying on broker auto-create",
			"topic", topic, "error", eErr)
	}
	cancel()

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
		// Sarama parity: sarama's default Metadata.AllowAutoTopicCreation=true
		// auto-created topics on first produce; kgo defaults this off and
		// fails with UNKNOWN_TOPIC_OR_PARTITION instead.
		kgo.AllowAutoTopicCreation(),
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
//
// ctx is threaded to the underlying franz-go ProduceSync call and, when it
// carries a valid OTEL span, propagated into the record's Kafka headers.
// Callers publishing a retry/DLQ hand-off that MUST survive the originating
// request/consumer context being canceled should pass
// context.WithoutCancel(ctx) — this keeps the trace context while dropping
// cancellation, so a canceled caller can't drop a durable hand-off.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	start := time.Now()
	partition, offset, err := p.pub.Produce(ctx, key, value)
	metrics.ObserveKafkaProduce(p.topic, len(value), time.Since(start), err)
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", p.topic, err)
	}

	p.logger.Debug("published message", "topic", p.topic, "partition", partition, "offset", offset, "key", key)
	return nil
}

// PublishWithHashKey sends a message keyed by the SHA256-derived partition key
// of key — the single-record equivalent of HashBatchEntry. Both go through
// HashPartitionKey, so a record published this way lands on the same partition
// as one fanned out via HashBatchEntry for the same key (the property the
// subtree-work retry path and the callback emit/retry paths rely on).
//
// See Publish for the ctx/WithoutCancel contract.
func (p *Producer) PublishWithHashKey(ctx context.Context, key string, value []byte) error {
	return p.Publish(ctx, HashPartitionKey(key), value)
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
//
// See Publish for the ctx/WithoutCancel contract; the same ctx is applied to
// every record in the batch.
func (p *Producer) PublishBatch(ctx context.Context, entries []BatchEntry) error {
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
			if err := p.Publish(ctx, e.Key, e.Value); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	start := time.Now()
	err := bp.ProduceBatch(ctx, entries)
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
