package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// partitionCount returns how many partitions the topic currently has on the
// cluster, via a metadata/admin lookup.
func partitionCount(t *testing.T, brokers []string, topic string) int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	td, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	return len(td[topic].Partitions)
}

// TestEnsureTopics_CreatesAtConfiguredWidth checks a fresh topic is created with
// the partition count from the map, and that a topic absent from the map falls
// back to the single-partition default.
func TestEnsureTopics_CreatesAtConfiguredWidth(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	ctx := context.Background()
	wide, narrow := "subtree-work", "block"
	if err := EnsureTopics(ctx, brokers, []string{wide, narrow}, map[string]int32{wide: 8}, nil); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	if got := partitionCount(t, brokers, wide); got != 8 {
		t.Errorf("%s created with %d partitions, want 8", wide, got)
	}
	if got := partitionCount(t, brokers, narrow); got != 1 {
		t.Errorf("%s (absent from map) created with %d partitions, want default 1", narrow, got)
	}
}

// TestEnsureTopics_GrowsExistingTopic checks the one-way grow: an existing
// narrow topic is widened to the configured count, the call is idempotent when
// already at target, and a target below the current count never shrinks it.
func TestEnsureTopics_GrowsExistingTopic(t *testing.T) {
	const topic = "subtree-work"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	ctx := context.Background()

	// Grow 2 -> 6.
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 6}, nil); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if got := partitionCount(t, brokers, topic); got != 6 {
		t.Fatalf("after grow: %d partitions, want 6", got)
	}

	// Idempotent: same target again is a no-op, not an error.
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 6}, nil); err != nil {
		t.Fatalf("re-ensure at same width: %v", err)
	}
	if got := partitionCount(t, brokers, topic); got != 6 {
		t.Fatalf("after re-ensure: %d partitions, want 6", got)
	}

	// Never shrink: a smaller target is ignored (InvalidPartitions treated as
	// "already wide enough"), and must not error.
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 3}, nil); err != nil {
		t.Fatalf("smaller target should be a benign no-op, got: %v", err)
	}
	if got := partitionCount(t, brokers, topic); got != 6 {
		t.Fatalf("after smaller target: %d partitions, want still 6 (never shrink)", got)
	}
}
