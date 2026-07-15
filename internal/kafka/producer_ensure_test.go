package kafka

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestNewProducer_EnsuresTopicOnStartup reproduces the dev-ovh-1 incident of
// 8 Jul 2026: the subtree-fetcher's DLQ producer targeted subtree-dlq, a
// topic nothing had created — consumers pre-create only the topics they
// subscribe to — on a broker with auto-creation disabled. Every DLQ publish
// failed with UNKNOWN_TOPIC_OR_PARTITION, which turned the disk-full
// overflow path into a partition rewind storm.
//
// The contract pinned here: NewProducer ensures its target topic exists,
// exactly the way NewConsumer ensures its subscribed topics, so a
// producer-only topic (a DLQ) works on brokers with auto-creation off. The
// kfake cluster below has auto-creation off and does NOT seed the topic.
func TestNewProducer_EnsuresTopicOnStartup(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	p, err := NewProducer(cluster.ListenAddrs(), "ensure-dlq-test", nil, nil, logger)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Publish(context.Background(), "key", []byte("parked message")); err != nil {
		t.Fatalf("publish to producer-only topic must succeed on a broker with auto-create disabled: %v", err)
	}
}

// TestNewProducer_EnsuresTopicAtConfiguredPartitionCount pins the partition
// width of producer-created topics. Until the scale-ovh incident of 15 Jul
// 2026, NewProducer ensured its topic with a nil partitions map, so a
// producer that started before any consumer created the work topic at 1
// partition — observed live on dev-ovh-1: the subtree topic's on-disk
// partition dirs showed partition 0 created by one controller command and
// partitions 1+ grafted on later by a consumer's grow, remapping every key
// mid-stream. A producer must create its topic at the same configured width
// consumers use (config.KafkaConfig.TopicPartitions).
func TestNewProducer_EnsuresTopicAtConfiguredPartitionCount(t *testing.T) {
	const topic = "work-topic"
	const want = 24

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	p, err := NewProducer(brokers, topic, map[string]int32{topic: want}, nil, logger)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = p.Close() }()

	adm, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer adm.Close()
	details, err := kadm.NewClient(adm).ListTopics(context.Background(), topic)
	if err != nil {
		t.Fatalf("listing topics: %v", err)
	}
	got := len(details[topic].Partitions)
	if got != want {
		t.Fatalf("producer-created topic %s has %d partitions, want %d", topic, got, want)
	}
}
