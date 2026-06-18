package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConsumer_RedeliversFailedRecordAndTail is the F-030/F-021 regression
// test for the franz migration, run against an in-memory kfake cluster (no
// external broker needed).
//
// kgo advances its in-memory fetch position as records are returned from
// PollFetches, independent of offset commits. A consumer that merely withholds
// the commit on a handler error (sarama semantics) therefore NEVER sees the
// failed record again in the same session: the next poll continues past the
// fetched batch, and a later successful commit advances the committed offset
// past the failure — silently and permanently losing the failed record and
// the skipped tail of its batch. The fix rewinds the partition's fetch
// position (PauseFetchPartitions + SetOffsets + ResumeFetchPartitions) to the
// failed record.
//
// Scenario: five records v0..v4 on one partition; the handler fails v2 exactly
// once. Without the rewind, v2/v3/v4 are never redelivered and the test times
// out. With it, all five values are eventually handled successfully.
func TestConsumer_RedeliversFailedRecordAndTail(t *testing.T) {
	const topic = "redeliver-test"

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, topic),
	)
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	// Produce v0..v4 onto the single partition.
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	for i := 0; i < 5; i++ {
		res := prod.ProduceSync(context.Background(), &kgo.Record{Value: []byte(fmt.Sprintf("v%d", i))})
		if prodErr := res.FirstErr(); prodErr != nil {
			prod.Close()
			t.Fatalf("producing v%d: %v", i, prodErr)
		}
	}
	prod.Close()

	// The F-053 crash-guard calls exitFunc on unexpected goroutine exit; stub
	// it so an unexpected exit fails the test instead of killing the runner.
	exitCh := make(chan int, 1)
	origExit := exitFunc
	exitFunc = func(code int) { exitCh <- code }
	defer func() { exitFunc = origExit }()

	// Handler: fail v2 exactly once, succeed otherwise.
	var (
		mu         sync.Mutex
		succeeded  = make(map[string]int)
		failedOnce bool
	)
	handler := func(_ context.Context, m *Message) error {
		mu.Lock()
		defer mu.Unlock()
		if string(m.Value) == "v2" && !failedOnce {
			failedOnce = true
			return errors.New("transient handler failure")
		}
		succeeded[string(m.Value)]++
		return nil
	}

	consumer, err := NewConsumer(brokers, "redeliver-group", []string{topic}, handler, nil, discardLogger())
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("starting consumer: %v", err)
	}
	defer consumer.Stop() //nolint:errcheck // test teardown

	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case code := <-exitCh:
			t.Fatalf("consumer crash-guard fired unexpectedly (exit %d)", code)
		case <-deadline:
			mu.Lock()
			defer mu.Unlock()
			t.Fatalf(
				"timed out waiting for redelivery: succeeded=%v failedOnce=%v — v2 and the skipped tail (v3, v4) were never redelivered (fetch position advanced past the failed batch)",
				succeeded, failedOnce,
			)
		case <-tick.C:
			mu.Lock()
			complete := len(succeeded) == 5
			redelivered := succeeded["v2"] >= 1 && failedOnce
			mu.Unlock()
			if complete && redelivered {
				return // every record, including the failed one and its tail, was handled
			}
		}
	}
}
