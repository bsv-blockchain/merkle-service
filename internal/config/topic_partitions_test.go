package config

import "testing"

func TestKafkaConfig_TopicPartitions(t *testing.T) {
	t.Run("defaults map only the two safe topics", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", BlockTopic: "block", CallbackTopic: "callback"}
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
		if _, ok := m["callback"]; ok {
			t.Error("callback must not appear in the partition map (it stays at 1)")
		}
	})

	t.Run("explicit values win", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", SubtreePartitions: 4, SubtreeWorkPartitions: 32}
		m := k.TopicPartitions()
		if m["subtree"] != 4 || m["subtree-work"] != 32 {
			t.Errorf("explicit counts not honoured: %+v", m)
		}
	})

	t.Run("non-positive falls back to default", func(t *testing.T) {
		k := KafkaConfig{SubtreeTopic: "subtree", SubtreeWorkTopic: "subtree-work", SubtreePartitions: 0, SubtreeWorkPartitions: -5}
		m := k.TopicPartitions()
		if m["subtree"] != defaultSubtreePartitions || m["subtree-work"] != defaultSubtreeWorkPartitions {
			t.Errorf("non-positive should fall back to defaults: %+v", m)
		}
	})
}
