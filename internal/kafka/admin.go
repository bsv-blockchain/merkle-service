package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic creation defaults. Single partition + replication factor 1 matches the
// broker defaults merkle-service has always relied on (and the integration
// test's ensureTopicExists). A topic with no entry in the partitions map is
// created at defaultTopicPartitions.
const (
	defaultTopicPartitions  int32 = 1
	defaultTopicReplication int16 = 1
)

// EnsureTopics creates each non-empty topic that does not already exist, and
// grows any existing topic that is narrower than its desired partition count, via
// the franz admin API (kadm).
//
// partitions maps a topic name to the number of partitions it should have; a
// topic absent from the map (or with a count < 1) is created with
// defaultTopicPartitions (1). Growing is one-way — Kafka only allows increasing a
// topic's partition count, never decreasing — so a topic already at or above its
// desired count is left untouched (the broker's "not an increase" rejection is
// treated as success). This makes the call safe to run on every startup, and
// safe even if a producer lazily auto-created the topic at 1 partition first: the
// next consumer start grows it to the configured width.
//
// It is idempotent: an already-existing topic is success (kerr.TopicAlreadyExists
// checked on both the call error and the per-topic response, per teranode #633 —
// not a string match), and an already-wide-enough topic is success
// (kerr.InvalidPartitions from the grow request).
//
// merkle-service calls this from NewConsumer so a consumer group's topics exist
// BEFORE it joins, giving it a stable partition assignment immediately. Without
// it, a consumer subscribed to a not-yet-created topic must wait for a metadata
// refresh to discover the partition once a producer lazily auto-creates it — and
// franz's default MetadataMaxAge is minutes, which left /reprocess block messages
// unconsumed (the previous sarama client created topics eagerly on first
// metadata request). Explicit creation also works on brokers with
// auto.create.topics.enable=false.
func EnsureTopics(ctx context.Context, brokers, topics []string, partitions map[string]int32, logger *slog.Logger) error {
	uniq := dedupeNonEmpty(topics)
	if len(uniq) == 0 {
		return nil
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("ensure topics: admin client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	for _, topic := range uniq {
		want := defaultTopicPartitions
		if p, ok := partitions[topic]; ok && p > 0 {
			want = p
		}

		resp, cErr := admin.CreateTopic(ctx, want, defaultTopicReplication, nil, topic)
		existed := errors.Is(cErr, kerr.TopicAlreadyExists) || errors.Is(resp.Err, kerr.TopicAlreadyExists)
		if cErr != nil && !existed {
			return fmt.Errorf("ensure topic %s: %w", topic, cErr)
		}
		if resp.Err != nil && !existed {
			return fmt.Errorf("ensure topic %s: %w", topic, resp.Err)
		}
		if !existed {
			if logger != nil {
				logger.Debug("created kafka topic", "topic", topic, "partitions", want)
			}
			continue
		}

		// Topic already existed. Grow it to `want` when wider than the default.
		// We don't read the current count first: UpdatePartitions sets the final
		// count, and the broker rejects anything that isn't a strict increase with
		// InvalidPartitions, which we treat as "already wide enough".
		if want <= defaultTopicPartitions {
			if logger != nil {
				logger.Debug("ensured kafka topic exists", "topic", topic, "partitions", want)
			}
			continue
		}
		gResp, gErr := admin.UpdatePartitions(ctx, int(want), topic)
		if gErr != nil {
			return fmt.Errorf("grow topic %s to %d partitions: %w", topic, want, gErr)
		}
		if tErr := gResp[topic].Err; tErr != nil && !errors.Is(tErr, kerr.InvalidPartitions) {
			return fmt.Errorf("grow topic %s to %d partitions: %w", topic, want, tErr)
		}
		if logger != nil {
			logger.Info("ensured kafka topic partition count", "topic", topic, "partitions", want)
		}
	}
	return nil
}

// dedupeNonEmpty returns the input with empty strings dropped and duplicates
// removed, preserving first-seen order.
func dedupeNonEmpty(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
