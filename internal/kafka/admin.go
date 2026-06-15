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
// test's ensureTopicExists).
const (
	defaultTopicPartitions  int32 = 1
	defaultTopicReplication int16 = 1
)

// EnsureTopics creates each non-empty topic that does not already exist, via the
// franz admin API (kadm). It is idempotent: an already-existing topic is treated
// as success (kerr.TopicAlreadyExists is checked on both the call error and the
// per-topic response, per teranode #633 — not a string match).
//
// merkle-service calls this from NewConsumer so a consumer group's topics exist
// BEFORE it joins, giving it a stable partition assignment immediately. Without
// it, a consumer subscribed to a not-yet-created topic must wait for a metadata
// refresh to discover the partition once a producer lazily auto-creates it — and
// franz's default MetadataMaxAge is minutes, which left /reprocess block messages
// unconsumed (the previous sarama client created topics eagerly on first
// metadata request). Explicit creation also works on brokers with
// auto.create.topics.enable=false.
func EnsureTopics(ctx context.Context, brokers, topics []string, logger *slog.Logger) error {
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
		resp, cErr := admin.CreateTopic(ctx, defaultTopicPartitions, defaultTopicReplication, nil, topic)
		if cErr != nil && !errors.Is(cErr, kerr.TopicAlreadyExists) {
			return fmt.Errorf("ensure topic %s: %w", topic, cErr)
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("ensure topic %s: %w", topic, resp.Err)
		}
		if logger != nil {
			logger.Debug("ensured kafka topic exists", "topic", topic)
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
