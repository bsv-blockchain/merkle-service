package config

import "testing"

// Topic names shared across the topic-map tests.
const (
	topicCallback     = "callback"
	topicCallbackSeen = "callback-seen"
)

func TestKafkaConfig_TopicPartitions(t *testing.T) {
	t.Run("defaults map only the two safe topics", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", BlockTopic: "block", CallbackTopic: topicCallback}
		m := k.TopicPartitions()
		if got := m["subtree"]; got != defaultSubtreePartitions {
			t.Errorf("subtree = %d, want %d", got, defaultSubtreePartitions)
		}
		if got := m["subtree-work"]; got != defaultSubtreeWorkPartitions {
			t.Errorf("subtree-work = %d, want %d", got, defaultSubtreeWorkPartitions)
		}
		// block and callback must be ABSENT so EnsureTopics defaults them to 1.
		if _, ok := m["block"]; ok {
			t.Error("block must not appear in the partition map (it stays at 1)")
		}
		if _, ok := m[topicCallback]; ok {
			t.Error("callback must not appear in the partition map (it stays at 1)")
		}
	})

	t.Run("explicit values win", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", SubtreePartitions: 4, SubtreeWorkPartitions: 32}
		m := k.TopicPartitions()
		if m["subtree"] != 4 || m["subtree-work"] != 32 {
			t.Errorf("explicit counts not honored: %+v", m)
		}
	})

	t.Run("non-positive falls back to default", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", SubtreePartitions: 0, SubtreeWorkPartitions: -5}
		m := k.TopicPartitions()
		if m["subtree"] != defaultSubtreePartitions || m["subtree-work"] != defaultSubtreeWorkPartitions {
			t.Errorf("non-positive should fall back to defaults: %+v", m)
		}
	})

	// The SEEN split's highest-risk line: TopicPartitions must key off the raw
	// CallbackSeenTopic field, never SeenCallbackTopic(). The helper falls back
	// to CallbackTopic when the SEEN topic is unset, so using it here would
	// silently widen 'callback' itself and break the STUMP → BLOCK_PROCESSED
	// ordering barrier that topic depends on.
	t.Run("seen topic absent when not configured", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", CallbackTopic: topicCallback}
		m := k.TopicPartitions()
		if _, ok := m[topicCallback]; ok {
			t.Error("callback must not appear in the partition map when callbackSeenTopic is unset")
		}
		if _, ok := m[topicCallbackSeen]; ok {
			t.Error("callback-seen must not appear in the partition map when callbackSeenTopic is unset")
		}
		if len(m) != 2 {
			t.Errorf("expected only subtree + subtree-work in the map, got %+v", m)
		}
	})

	t.Run("seen topic present and wide only when configured", func(t *testing.T) {
		k := KafkaConfig{
			SubtreeTopic:      "subtree",
			SubtreeWorkTopic:  "subtree-work",
			CallbackTopic:     topicCallback,
			CallbackSeenTopic: topicCallbackSeen,
		}
		m := k.TopicPartitions()
		if got := m[topicCallbackSeen]; got != defaultCallbackSeenPartitions {
			t.Errorf("callback-seen = %d, want %d", got, defaultCallbackSeenPartitions)
		}
		// Configuring the SEEN topic must NOT drag 'callback' into the map.
		if _, ok := m[topicCallback]; ok {
			t.Error("callback must stay absent from the partition map even when the SEEN split is enabled")
		}
	})

	t.Run("explicit seen partition count wins", func(t *testing.T) {
		k := KafkaConfig{CallbackTopic: topicCallback, CallbackSeenTopic: topicCallbackSeen, CallbackSeenPartitions: 3}
		if got := k.TopicPartitions()[topicCallbackSeen]; got != 3 {
			t.Errorf("callback-seen = %d, want 3", got)
		}
	})

	t.Run("non-positive seen count falls back to default", func(t *testing.T) {
		k := KafkaConfig{CallbackTopic: topicCallback, CallbackSeenTopic: topicCallbackSeen, CallbackSeenPartitions: -1}
		if got := k.TopicPartitions()[topicCallbackSeen]; got != defaultCallbackSeenPartitions {
			t.Errorf("callback-seen = %d, want %d", got, defaultCallbackSeenPartitions)
		}
	})
}

// TestKafkaConfig_SeenCallbackTopic pins the fallback contract that makes the
// SEEN split inert until configured: an empty CallbackSeenTopic must resolve
// to the shared CallbackTopic on BOTH the producer and the consumer side,
// which both call this one helper.
func TestKafkaConfig_SeenCallbackTopic(t *testing.T) {
	t.Run("falls back to the shared callback topic when unset", func(t *testing.T) {
		k := KafkaConfig{CallbackTopic: topicCallback}
		if got := k.SeenCallbackTopic(); got != topicCallback {
			t.Errorf("SeenCallbackTopic() = %q, want %q", got, topicCallback)
		}
	})

	t.Run("returns the dedicated topic when set", func(t *testing.T) {
		k := KafkaConfig{CallbackTopic: topicCallback, CallbackSeenTopic: topicCallbackSeen}
		if got := k.SeenCallbackTopic(); got != topicCallbackSeen {
			t.Errorf("SeenCallbackTopic() = %q, want %q", got, topicCallbackSeen)
		}
	})
}

// TestLoad_CallbackSeenTopicDefaultsInert asserts the change ships disabled:
// with no config file and no env var, SEEN callbacks still resolve to
// 'callback' and the shared topic is still absent from the partition map.
func TestLoad_CallbackSeenTopicDefaultsInert(t *testing.T) {
	t.Setenv("CONFIG_FILE", "/nonexistent/config.yaml")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Kafka.CallbackSeenTopic != "" {
		t.Errorf("kafka.callbackSeenTopic default = %q, want empty (inert)", cfg.Kafka.CallbackSeenTopic)
	}
	if got := cfg.Kafka.SeenCallbackTopic(); got != cfg.Kafka.CallbackTopic {
		t.Errorf("SeenCallbackTopic() = %q, want the shared callback topic %q", got, cfg.Kafka.CallbackTopic)
	}
	if _, ok := cfg.Kafka.TopicPartitions()[cfg.Kafka.CallbackTopic]; ok {
		t.Error("the shared callback topic must not be widened by default")
	}
	if cfg.Kafka.CallbackSeenPartitions != defaultCallbackSeenPartitions {
		t.Errorf("kafka.callbackSeenPartitions default = %d, want %d", cfg.Kafka.CallbackSeenPartitions, defaultCallbackSeenPartitions)
	}
}

// TestLoad_CallbackSeenTopicEnvOverride verifies the env plumbing operators
// will actually use to turn the split on.
func TestLoad_CallbackSeenTopicEnvOverride(t *testing.T) {
	t.Setenv("CONFIG_FILE", "/nonexistent/config.yaml")
	t.Setenv("KAFKA_CALLBACK_SEEN_TOPIC", topicCallbackSeen)
	t.Setenv("KAFKA_CALLBACK_SEEN_PARTITIONS", "6")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Kafka.CallbackSeenTopic != topicCallbackSeen {
		t.Errorf("KAFKA_CALLBACK_SEEN_TOPIC override = %q, want callback-seen", cfg.Kafka.CallbackSeenTopic)
	}
	if got := cfg.Kafka.SeenCallbackTopic(); got != topicCallbackSeen {
		t.Errorf("SeenCallbackTopic() = %q, want callback-seen", got)
	}
	if got := cfg.Kafka.TopicPartitions()[topicCallbackSeen]; got != 6 {
		t.Errorf("callback-seen partitions = %d, want 6", got)
	}
	if got := cfg.Kafka.TopicRetention()[topicCallbackSeen]; got != defaultTopicRetentionMs {
		t.Errorf("callback-seen retention = %d, want %d (work-topic retention)", got, defaultTopicRetentionMs)
	}
}
