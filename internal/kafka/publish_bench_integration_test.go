//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// Before/after fan-out benchmark for the P1 batch-publish work
// (docs/kafka-throughput-review.md F-6) against a REAL Kafka broker.
//
// "Serial" drives the legacy pattern: one broker-acked Publish per record
// (acks=all, idempotent producer) — what every fan-out loop did before.
// "Batch" drives the new PublishBatch: same records, one synchronous call.
//
// Run:
//
//	KAFKA_BROKERS=localhost:9192 \
//	  go test -tags integration -run XXX -bench Publish -benchtime 5x ./internal/kafka/
func BenchmarkPublishFanout(b *testing.B) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	topic := fmt.Sprintf("bench_publish_%d", time.Now().UnixNano())

	// Pre-create the topic: auto-creation can outlast the client's metadata
	// retries on a fresh broker.
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		b.Skipf("Kafka not available at %s: %v", brokers, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(adminClient).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		adminClient.Close()
		b.Skipf("creating bench topic on %s: %v", brokers, err)
	}
	adminClient.Close()

	prod, err := kafka.NewProducer([]string{brokers}, topic, nil, logger)
	if err != nil {
		b.Skipf("Kafka not available at %s: %v", brokers, err)
	}
	b.Cleanup(func() { _ = prod.Close() })

	payload := make([]byte, 512) // typical claim-checked work-message size
	// Warm-up publish: forces topic auto-creation + metadata so neither
	// variant pays the one-time cost inside the timed loop.
	if err := prod.Publish(context.Background(), "warmup", payload); err != nil {
		b.Skipf("warm-up publish failed (broker not ready?): %v", err)
	}

	for _, size := range []int{100, 1000} {
		entries := make([]kafka.BatchEntry, size)
		for i := range entries {
			entries[i] = kafka.HashBatchEntry(fmt.Sprintf("subtree-%d", i), payload)
		}

		b.Run(fmt.Sprintf("serial/records=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, e := range entries {
					if err := prod.Publish(context.Background(), e.Key, e.Value); err != nil {
						b.Fatalf("Publish: %v", err)
					}
				}
			}
			b.ReportMetric(float64(size)/b.Elapsed().Seconds()*float64(b.N), "msgs/s")
		})
		b.Run(fmt.Sprintf("batch/records=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := prod.PublishBatch(context.Background(), entries); err != nil {
					b.Fatalf("PublishBatch: %v", err)
				}
			}
			b.ReportMetric(float64(size)/b.Elapsed().Seconds()*float64(b.N), "msgs/s")
		})
	}
}
