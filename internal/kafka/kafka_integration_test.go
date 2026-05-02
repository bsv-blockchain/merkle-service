//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

var brokers = []string{"localhost:9092"}

func uniqueTopic(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// ensureTopicExists creates a topic via the Kafka admin API so that
// producers and consumers can use it immediately.
func ensureTopicExists(t *testing.T, topic string) {
	t.Helper()
	cfg := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		// Ignore "already exists" errors.
		t.Logf("create topic %s: %v (may already exist)", topic, err)
	}
}

func TestKafka_ProduceConsumeRoundTrip(t *testing.T) {
	topic := uniqueTopic(t, "integ_roundtrip")
	ensureTopicExists(t, topic)

	producer, err := kafka.NewProducer(brokers, topic, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer producer.Close()

	groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())
	expectedKey := "test-key-1"
	expectedValue := []byte("hello-world-roundtrip")

	var received []byte
	var mu sync.Mutex
	done := make(chan struct{})

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		mu.Lock()
		defer mu.Unlock()
		received = msg.Value
		close(done)
		return nil
	}

	consumer, err := kafka.NewConsumer(brokers, groupID, []string{topic}, handler, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := consumer.Start(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer consumer.Stop()

	// Allow consumer group to stabilize.
	time.Sleep(3 * time.Second)

	if err := producer.Publish(expectedKey, expectedValue); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	mu.Lock()
	defer mu.Unlock()
	if string(received) != string(expectedValue) {
		t.Fatalf("expected %q, got %q", string(expectedValue), string(received))
	}
}

func TestKafka_ProduceMultipleConsumeInOrder(t *testing.T) {
	topic := uniqueTopic(t, "integ_order")
	ensureTopicExists(t, topic)

	producer, err := kafka.NewProducer(brokers, topic, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer producer.Close()

	groupID := fmt.Sprintf("test-group-order-%d", time.Now().UnixNano())
	msgCount := 5

	var mu sync.Mutex
	var received []string
	allDone := make(chan struct{})

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, string(msg.Value))
		if len(received) == msgCount {
			close(allDone)
		}
		return nil
	}

	consumer, err := kafka.NewConsumer(brokers, groupID, []string{topic}, handler, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := consumer.Start(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer consumer.Stop()

	time.Sleep(3 * time.Second)

	// Publish messages with the same key to ensure they go to the same partition
	// and thus maintain ordering.
	for i := 0; i < msgCount; i++ {
		val := fmt.Sprintf("msg-%d", i)
		if err := producer.Publish("same-key", []byte(val)); err != nil {
			t.Fatalf("Publish #%d failed: %v", i, err)
		}
	}

	select {
	case <-allDone:
	case <-time.After(15 * time.Second):
		mu.Lock()
		t.Fatalf("timed out waiting for messages, received %d/%d: %v", len(received), msgCount, received)
		mu.Unlock()
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < msgCount; i++ {
		expected := fmt.Sprintf("msg-%d", i)
		if received[i] != expected {
			t.Errorf("message[%d] = %q, want %q", i, received[i], expected)
		}
	}
}

func TestKafka_ConsumerGroupOffsetManagement(t *testing.T) {
	topic := uniqueTopic(t, "integ_offset")
	ensureTopicExists(t, topic)

	producer, err := kafka.NewProducer(brokers, topic, slog.Default())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer producer.Close()

	// Use a consistent group ID so the second consumer resumes from committed offsets.
	groupID := fmt.Sprintf("test-group-offset-%d", time.Now().UnixNano())

	// --- Phase 1: Consume first batch ---
	var mu1 sync.Mutex
	var received1 []string
	firstBatchDone := make(chan struct{})
	firstBatchCount := 3

	handler1 := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		mu1.Lock()
		defer mu1.Unlock()
		received1 = append(received1, string(msg.Value))
		if len(received1) == firstBatchCount {
			close(firstBatchDone)
		}
		return nil
	}

	consumer1, err := kafka.NewConsumer(brokers, groupID, []string{topic}, handler1, slog.Default())
	if err != nil {
		t.Fatalf("failed to create consumer1: %v", err)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()

	if err := consumer1.Start(ctx1); err != nil {
		t.Fatalf("failed to start consumer1: %v", err)
	}

	time.Sleep(3 * time.Second)

	for i := 0; i < firstBatchCount; i++ {
		if err := producer.Publish("key", []byte(fmt.Sprintf("batch1-msg-%d", i))); err != nil {
			t.Fatalf("Publish batch1 #%d failed: %v", i, err)
		}
	}

	select {
	case <-firstBatchDone:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for first batch")
	}

	// Stop consumer1 to commit offsets.
	if err := consumer1.Stop(); err != nil {
		t.Fatalf("failed to stop consumer1: %v", err)
	}
	cancel1()

	// --- Phase 2: Produce more messages, start a new consumer with same group ---
	secondBatchCount := 2
	for i := 0; i < secondBatchCount; i++ {
		if err := producer.Publish("key", []byte(fmt.Sprintf("batch2-msg-%d", i))); err != nil {
			t.Fatalf("Publish batch2 #%d failed: %v", i, err)
		}
	}

	var mu2 sync.Mutex
	var received2 []string
	secondBatchDone := make(chan struct{})

	handler2 := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		mu2.Lock()
		defer mu2.Unlock()
		received2 = append(received2, string(msg.Value))
		if len(received2) == secondBatchCount {
			close(secondBatchDone)
		}
		return nil
	}

	consumer2, err := kafka.NewConsumer(brokers, groupID, []string{topic}, handler2, slog.Default())
	if err != nil {
		t.Fatalf("failed to create consumer2: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	if err := consumer2.Start(ctx2); err != nil {
		t.Fatalf("failed to start consumer2: %v", err)
	}
	defer consumer2.Stop()

	time.Sleep(3 * time.Second)

	select {
	case <-secondBatchDone:
	case <-time.After(15 * time.Second):
		mu2.Lock()
		t.Fatalf("timed out waiting for second batch, received %d/%d: %v", len(received2), secondBatchCount, received2)
		mu2.Unlock()
	}

	mu2.Lock()
	defer mu2.Unlock()

	// Verify we only got the second batch messages, not the first batch again.
	for _, msg := range received2 {
		for i := 0; i < firstBatchCount; i++ {
			if msg == fmt.Sprintf("batch1-msg-%d", i) {
				t.Errorf("received re-delivered message from first batch: %q", msg)
			}
		}
	}

	// Verify content of second batch.
	if len(received2) != secondBatchCount {
		t.Fatalf("expected %d messages in second batch, got %d", secondBatchCount, len(received2))
	}
	for i := 0; i < secondBatchCount; i++ {
		expected := fmt.Sprintf("batch2-msg-%d", i)
		if received2[i] != expected {
			t.Errorf("second batch message[%d] = %q, want %q", i, received2[i], expected)
		}
	}
}
