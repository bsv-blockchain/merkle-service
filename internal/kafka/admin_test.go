package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// topicRetentionMs returns the retention.ms EXPLICITLY set on the topic
// (dynamic topic config, i.e. what CreateTopic configs / AlterConfigs wrote),
// or nil when the topic only inherits the broker default.
func topicRetentionMs(t *testing.T, brokers []string, topic string) *string {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rcs, err := kadm.NewClient(cl).DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatalf("describe topic configs: %v", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		t.Fatalf("describe topic configs for %s: %v", topic, err)
	}
	for _, c := range rc.Configs {
		if c.Key == "retention.ms" && c.Source == kmsg.ConfigSourceDynamicTopicConfig {
			return c.Value
		}
	}
	return nil
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
	if err := EnsureTopics(ctx, brokers, []string{wide, narrow}, map[string]int32{wide: 8}, nil, nil); err != nil {
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
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 6}, nil, nil); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if got := partitionCount(t, brokers, topic); got != 6 {
		t.Fatalf("after grow: %d partitions, want 6", got)
	}

	// Idempotent: same target again is a no-op, not an error.
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 6}, nil, nil); err != nil {
		t.Fatalf("re-ensure at same width: %v", err)
	}
	if got := partitionCount(t, brokers, topic); got != 6 {
		t.Fatalf("after re-ensure: %d partitions, want 6", got)
	}

	// Never shrink: a smaller target is ignored (InvalidPartitions treated as
	// "already wide enough"), and must not error.
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 3}, nil, nil); err != nil {
		t.Fatalf("smaller target should be a benign no-op, got: %v", err)
	}
	if got := partitionCount(t, brokers, topic); got != 6 {
		t.Fatalf("after smaller target: %d partitions, want still 6 (never shrink)", got)
	}
}

// TestEnsureTopics_SetsRetentionAtCreate is the regression test for the
// dev-ovh-1 retention gap: topics were created with nil configs, so a fresh
// cluster's broker default retention applied silently — on that cluster the
// default was 15 MINUTES, which would expire queued work and dead letters
// out from under the service. Topics created by EnsureTopics must carry an
// explicit retention.ms from the retention map.
func TestEnsureTopics_SetsRetentionAtCreate(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	ctx := context.Background()
	work, dlq, unmapped := "subtree-work", "subtree-dlq", "block"
	retention := map[string]int64{work: 21_600_000, dlq: 604_800_000}
	if err := EnsureTopics(ctx, brokers, []string{work, dlq, unmapped}, nil, retention, nil); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	if got := topicRetentionMs(t, brokers, work); got == nil || *got != "21600000" {
		t.Errorf("%s retention.ms = %v, want 21600000", work, got)
	}
	if got := topicRetentionMs(t, brokers, dlq); got == nil || *got != "604800000" {
		t.Errorf("%s retention.ms = %v, want 604800000", dlq, got)
	}
	// A topic absent from the retention map keeps the pre-fix behavior:
	// created with no explicit retention config.
	if got := topicRetentionMs(t, brokers, unmapped); got != nil {
		t.Errorf("%s (absent from retention map) retention.ms = %q, want unset", unmapped, *got)
	}
}

// TestEnsureTopics_NeverAltersExistingTopicRetention pins the GitOps
// contract: retention is applied only at CREATE time. An already-existing
// topic (owned by Topic CRDs in the scale clusters) must never have its
// configs altered by a service startup.
func TestEnsureTopics_NeverAltersExistingTopicRetention(t *testing.T) {
	const topic = "subtree-work"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	before := topicRetentionMs(t, brokers, topic)

	ctx := context.Background()
	if err := EnsureTopics(ctx, brokers, []string{topic}, map[string]int32{topic: 6}, map[string]int64{topic: 21_600_000}, nil); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	after := topicRetentionMs(t, brokers, topic)
	switch {
	case before == nil && after != nil:
		t.Errorf("retention.ms was set on an existing topic: %q (must never alter existing topics)", *after)
	case before != nil && (after == nil || *after != *before):
		t.Errorf("retention.ms changed on an existing topic: before %v, after %v", *before, after)
	}
}
