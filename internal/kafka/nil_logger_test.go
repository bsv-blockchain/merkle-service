package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
)

// TestNewProducer_NilLoggerIsSafe pins the constructor contract that a nil
// logger is defaulted rather than stored: Publish logs unconditionally, so
// without the default the first publish would panic on a nil *slog.Logger.
func TestNewProducer_NilLoggerIsSafe(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()

	p, err := NewProducer(cluster.ListenAddrs(), "nil-logger-producer-test", nil, nil, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = p.Close() }()

	if p.logger == nil {
		t.Fatal("NewProducer stored a nil logger; Publish would panic")
	}

	if err := p.Publish(context.Background(), "key", []byte("message")); err != nil {
		t.Fatalf("Publish with defaulted logger: %v", err)
	}
}

// TestNewConsumer_NilLoggerIsSafe pins the same contract for the consumer:
// Start, the poll loop, and the partition workers log unconditionally, so a
// stored nil logger would panic at the first consumed message.
func TestNewConsumer_NilLoggerIsSafe(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()

	handler := func(ctx context.Context, msg *Message) error { return nil }

	c, err := NewConsumer(cluster.ListenAddrs(), "nil-logger-group", []string{"nil-logger-consumer-test"}, handler, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() { _ = c.Stop() }()

	if c.logger == nil {
		t.Fatal("NewConsumer stored a nil logger; the poll loop would panic")
	}
}
