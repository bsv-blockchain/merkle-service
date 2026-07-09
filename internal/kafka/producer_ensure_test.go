package kafka

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
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

	p, err := NewProducer(cluster.ListenAddrs(), "ensure-dlq-test", logger)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Publish(context.Background(), "key", []byte("parked message")); err != nil {
		t.Fatalf("publish to producer-only topic must succeed on a broker with auto-create disabled: %v", err)
	}
}
