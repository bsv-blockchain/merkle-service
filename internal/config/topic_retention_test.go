package config

import "testing"

func TestKafkaConfig_TopicRetention(t *testing.T) {
	k := KafkaConfig{
		SubtreeTopic:        "subtree",
		BlockTopic:          "block",
		CallbackTopic:       "callback",
		CallbackDLQTopic:    "callback-dlq",
		SubtreeDLQTopic:     "subtree-dlq",
		SubtreeWorkTopic:    "subtree-work",
		SubtreeWorkDLQTopic: "subtree-work-dlq",
	}

	t.Run("defaults: 6h for work topics, 7d for DLQs", func(t *testing.T) {
		m := k.TopicRetention()
		for _, topic := range []string{"subtree", "block", "callback", "subtree-work"} {
			if got := m[topic]; got != defaultTopicRetentionMs {
				t.Errorf("%s = %d, want %d", topic, got, defaultTopicRetentionMs)
			}
		}
		for _, topic := range []string{"callback-dlq", "subtree-dlq", "subtree-work-dlq"} {
			if got := m[topic]; got != defaultDLQRetentionMs {
				t.Errorf("%s = %d, want %d", topic, got, defaultDLQRetentionMs)
			}
		}
	})

	t.Run("explicit values win", func(t *testing.T) {
		k := k
		k.TopicRetentionMs = 3_600_000
		k.DLQRetentionMs = 86_400_000
		m := k.TopicRetention()
		if m["subtree-work"] != 3_600_000 {
			t.Errorf("subtree-work = %d, want 3600000", m["subtree-work"])
		}
		if m["subtree-dlq"] != 86_400_000 {
			t.Errorf("subtree-dlq = %d, want 86400000", m["subtree-dlq"])
		}
	})

	t.Run("non-positive falls back to defaults", func(t *testing.T) {
		k := k
		k.TopicRetentionMs = 0
		k.DLQRetentionMs = -1
		m := k.TopicRetention()
		if m["subtree"] != defaultTopicRetentionMs || m["subtree-dlq"] != defaultDLQRetentionMs {
			t.Errorf("non-positive should fall back to defaults: %+v", m)
		}
	})

	t.Run("empty topic names are skipped", func(t *testing.T) {
		m := KafkaConfig{SubtreeTopic: "subtree"}.TopicRetention()
		if len(m) != 1 {
			t.Errorf("expected only the configured topic in the map, got %+v", m)
		}
		if _, ok := m[""]; ok {
			t.Error("empty topic name must not appear in the retention map")
		}
	})
}

// TestLoad_TopicRetentionDefaults verifies the viper defaults for the new
// keys: 6h for work topics, 7d for DLQs — so a fresh cluster's aggressive
// broker default (dev-ovh-1: 15 minutes) can't silently apply to topics this
// service creates.
func TestLoad_TopicRetentionDefaults(t *testing.T) {
	t.Setenv("CONFIG_FILE", "/nonexistent/config.yaml")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Kafka.TopicRetentionMs != 21_600_000 {
		t.Errorf("kafka.topicRetentionMs default = %d, want 21600000 (6h)", cfg.Kafka.TopicRetentionMs)
	}
	if cfg.Kafka.DLQRetentionMs != 604_800_000 {
		t.Errorf("kafka.dlqRetentionMs default = %d, want 604800000 (7d)", cfg.Kafka.DLQRetentionMs)
	}
}

// TestLoad_TopicRetentionEnvOverride verifies the env var plumbing.
func TestLoad_TopicRetentionEnvOverride(t *testing.T) {
	t.Setenv("CONFIG_FILE", "/nonexistent/config.yaml")
	t.Setenv("KAFKA_TOPIC_RETENTION_MS", "3600000")
	t.Setenv("KAFKA_DLQ_RETENTION_MS", "86400000")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Kafka.TopicRetentionMs != 3_600_000 {
		t.Errorf("KAFKA_TOPIC_RETENTION_MS override = %d, want 3600000", cfg.Kafka.TopicRetentionMs)
	}
	if cfg.Kafka.DLQRetentionMs != 86_400_000 {
		t.Errorf("KAFKA_DLQ_RETENTION_MS override = %d, want 86400000", cfg.Kafka.DLQRetentionMs)
	}
}
