package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// spyPublisher implements ONLY the 2-method Publisher interface (no batch
// capability), so PublishBatch must fall back to a per-entry Publish loop.
type spyPublisher struct {
	produced []BatchEntry
	failAt   int // 1-based call index to fail at; 0 = never
	calls    int
}

func (s *spyPublisher) Produce(_ context.Context, key string, value []byte) (int32, int64, error) {
	s.calls++
	if s.failAt > 0 && s.calls == s.failAt {
		return 0, 0, errors.New("injected produce failure")
	}
	s.produced = append(s.produced, BatchEntry{Key: key, Value: value})
	return 0, int64(len(s.produced)), nil
}

func (s *spyPublisher) Close() error { return nil }

// TestPublishBatch_FallbackLoop verifies that a Publisher without the batch
// capability gets each entry via Produce, in order — so every existing test
// fake keeps capturing batched publishes without implementing ProduceBatch.
func TestPublishBatch_FallbackLoop(t *testing.T) {
	spy := &spyPublisher{}
	p := NewTestProducer(spy, "fallback-test", discardLogger())

	entries := []BatchEntry{
		{Key: "k0", Value: []byte("v0")},
		{Key: "k1", Value: []byte("v1")},
		{Key: "k2", Value: []byte("v2")},
	}
	if err := p.PublishBatch(context.Background(), entries); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(spy.produced) != 3 {
		t.Fatalf("produced %d entries, want 3", len(spy.produced))
	}
	for i, e := range entries {
		if spy.produced[i].Key != e.Key || string(spy.produced[i].Value) != string(e.Value) {
			t.Errorf("produced[%d] = %+v, want %+v (in order)", i, spy.produced[i], e)
		}
	}
}

// TestPublishBatch_FallbackContinuesOnError verifies the fallback loop
// attempts EVERY entry and surfaces the first failure — matching kgo's
// ProduceSync, which tries all records and reports FirstErr. One bad entry
// must not suppress the rest on this attempt; the returned error makes the
// caller redeliver the idempotent batch.
func TestPublishBatch_FallbackContinuesOnError(t *testing.T) {
	spy := &spyPublisher{failAt: 2}
	p := NewTestProducer(spy, "fallback-err-test", discardLogger())

	err := p.PublishBatch(context.Background(), []BatchEntry{
		{Key: "k0", Value: []byte("v0")},
		{Key: "k1", Value: []byte("v1")},
		{Key: "k2", Value: []byte("v2")},
	})
	if err == nil {
		t.Fatal("expected error from injected produce failure")
	}
	if len(spy.produced) != 2 {
		t.Errorf("produced %d entries, want 2 (k0 and k2; only the injected k1 failure skipped)", len(spy.produced))
	}
}

// TestPublishBatch_KgoRoundTrip exercises the real kgoPublisher.ProduceBatch
// against an in-memory kfake cluster: every record must land and be
// consumable, and keyed entries must keep their keys.
func TestPublishBatch_KgoRoundTrip(t *testing.T) {
	const topic = "publish-batch-test"

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	prod, err := NewProducer(brokers, topic, discardLogger())
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer prod.Close() //nolint:errcheck // test teardown

	const n = 50
	entries := make([]BatchEntry, n)
	for i := range entries {
		entries[i] = HashBatchEntry(fmt.Sprintf("pk-%d", i), []byte(fmt.Sprintf("payload-%d", i)))
	}
	if pubErr := prod.PublishBatch(context.Background(), entries); pubErr != nil {
		t.Fatalf("PublishBatch: %v", pubErr)
	}

	// Consume everything back with a plain kgo client.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	got := make(map[string]string, n)
	for len(got) < n {
		fetches := consumer.PollFetches(ctx)
		if fetchErr := fetches.Err(); fetchErr != nil {
			t.Fatalf("poll: %v (got %d/%d)", fetchErr, len(got), n)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got[string(r.Value)] = string(r.Key)
		})
	}

	for i := 0; i < n; i++ {
		payload := fmt.Sprintf("payload-%d", i)
		wantKey := HashPartitionKey(fmt.Sprintf("pk-%d", i))
		key, ok := got[payload]
		if !ok {
			t.Errorf("payload %q never consumed", payload)
			continue
		}
		if key != wantKey {
			t.Errorf("payload %q has key %q, want %q", payload, key, wantKey)
		}
	}
}
