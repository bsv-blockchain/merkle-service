package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConsumer_PartitionsProcessConcurrently verifies the consumer runs one
// worker per partition — the concurrency model sarama's per-claim
// ConsumeClaim goroutines provided, which the initial franz migration
// regressed to a single goroutine per pod (throughput review F-1).
//
// Deterministic shape: two partitions on one topic. The handler BLOCKS
// indefinitely on partition 0's first record (released at the end) while
// partition 1's records must all complete. Under a single shared consume
// goroutine, the blocked p0 handler starves p1 forever and the test times
// out; with per-partition workers, p1 finishes while p0 is still blocked.
// After release, p0 must complete too, in order — per-partition ordering is
// the one guarantee the pipeline's per-URL serialization depends on.
func TestConsumer_PartitionsProcessConcurrently(t *testing.T) {
	const topic = "partition-concurrency-test"

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic),
	)
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	// Produce a0..a2 onto partition 0 and b0..b2 onto partition 1.
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	for i := 0; i < 3; i++ {
		for part, prefix := range map[int32]string{0: "a", 1: "b"} {
			res := prod.ProduceSync(context.Background(), &kgo.Record{
				Partition: part,
				Value:     []byte(fmt.Sprintf("%s%d", prefix, i)),
			})
			if prodErr := res.FirstErr(); prodErr != nil {
				prod.Close()
				t.Fatalf("producing %s%d: %v", prefix, i, prodErr)
			}
		}
	}
	prod.Close()

	// Stub the F-053 crash-guard so an unexpected goroutine exit fails the
	// test instead of killing the runner.
	exitCh := make(chan int, 1)
	origExit := exitFunc
	exitFunc = func(code int) { exitCh <- code }
	defer func() { exitFunc = origExit }()

	release := make(chan struct{}) // closing unblocks partition 0's handler
	var (
		mu        sync.Mutex
		p0Handled []string
		p1Handled []string
	)
	handler := func(ctx context.Context, m *Message) error {
		if m.Partition == 0 {
			mu.Lock()
			first := len(p0Handled) == 0
			mu.Unlock()
			if first {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			mu.Lock()
			p0Handled = append(p0Handled, string(m.Value))
			mu.Unlock()
			return nil
		}
		mu.Lock()
		p1Handled = append(p1Handled, string(m.Value))
		mu.Unlock()
		return nil
	}

	consumer, err := NewConsumer(brokers, "partition-concurrency-group", []string{topic}, handler, discardLogger())
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("starting consumer: %v", err)
	}
	defer consumer.Stop() //nolint:errcheck // test teardown

	waitFor := func(desc string, cond func() bool) {
		t.Helper()
		deadline := time.After(20 * time.Second)
		tick := time.NewTicker(25 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case code := <-exitCh:
				t.Fatalf("consumer crash-guard fired unexpectedly (exit %d) while waiting for %s", code, desc)
			case <-deadline:
				mu.Lock()
				defer mu.Unlock()
				t.Fatalf("timed out waiting for %s: p0=%v p1=%v", desc, p0Handled, p1Handled)
			case <-tick.C:
				mu.Lock()
				ok := cond()
				mu.Unlock()
				if ok {
					return
				}
			}
		}
	}

	// Partition 1 must fully complete WHILE partition 0's handler is still
	// blocked — this is the per-partition concurrency assertion. A single
	// shared consume goroutine can never satisfy it.
	waitFor("partition 1 to finish while partition 0 is blocked", func() bool {
		return len(p1Handled) == 3 && len(p0Handled) == 0
	})

	// Release partition 0 and verify it completes too.
	close(release)
	waitFor("partition 0 to finish after release", func() bool {
		return len(p0Handled) == 3
	})

	// Per-partition ordering must hold (the pipeline's per-URL serialization
	// depends on it).
	mu.Lock()
	defer mu.Unlock()
	for i, want := range []string{"a0", "a1", "a2"} {
		if p0Handled[i] != want {
			t.Errorf("p0Handled[%d] = %q, want %q (in-order processing within a partition)", i, p0Handled[i], want)
		}
	}
	for i, want := range []string{"b0", "b1", "b2"} {
		if p1Handled[i] != want {
			t.Errorf("p1Handled[%d] = %q, want %q (in-order processing within a partition)", i, p1Handled[i], want)
		}
	}
}
