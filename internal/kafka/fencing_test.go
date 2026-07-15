package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConsumerOpts_SurvivesSlowProcessing pins the group-liveness timeouts
// against the dev-ovh-1 fencing collapse: with SessionTimeout(10s) /
// RebalanceTimeout(60s), slow handlers (1-2s each, feeding depth-5 worker
// channels) blocked the poll loop long enough that rebalances could not be
// serviced within 60s — the coordinator fenced members one by one until a
// 24-partition group had collapsed 5 -> 1 while every pod stayed Running.
// The consumer must tolerate minutes of slow processing before being fenced.
// Introspected via kgo's OptValue since franz options are otherwise opaque.
func TestConsumerOpts_SurvivesSlowProcessing(t *testing.T) {
	client, err := kgo.NewClient(consumerOpts([]string{"localhost:9092"}, "test-group", []string{testTopic})...)
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	defer client.Close()

	session := client.OptValue(kgo.SessionTimeout).(time.Duration)
	heartbeat := client.OptValue(kgo.HeartbeatInterval).(time.Duration)
	rebalance := client.OptValue(kgo.RebalanceTimeout).(time.Duration)

	if session != 30*time.Second {
		t.Errorf("SessionTimeout = %v, want 30s", session)
	}
	if heartbeat != 3*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 3s", heartbeat)
	}
	if rebalance != 5*time.Minute {
		t.Errorf("RebalanceTimeout = %v, want 5m", rebalance)
	}
	// kgo/broker constraint the options must keep satisfying.
	if session < 3*heartbeat {
		t.Errorf("SessionTimeout %v must be >= 3x HeartbeatInterval %v", session, heartbeat)
	}
}

// TestConsumer_PartitionsLostCancelsInFlightHandler covers the fenced-pod
// half of the incident: when partitions are LOST (session expiry, fencing) —
// not gracefully revoked — the previous behavior let in-flight handlers run
// to completion on partitions the member no longer owned. Fenced dev-ovh-1
// pods kept burning 13-14 cores processing stolen partitions whose commits
// could never land. A lost partition must cancel its worker's context so the
// in-flight handler aborts promptly.
func TestConsumer_PartitionsLostCancelsInFlightHandler(t *testing.T) {
	started := make(chan struct{})
	handlerErr := make(chan error, 1)
	handler := func(ctx context.Context, _ *Message) error {
		close(started)
		<-ctx.Done() // simulates a slow DataHub fetch that honors ctx
		handlerErr <- ctx.Err()
		return ctx.Err()
	}
	cc := &commitCapture{}
	c := newCommitTestConsumer(handler, cc)

	tp := topicPartition{testTopic, 0}
	w := newPartitionWorker(c, tp, context.Background())
	c.workers[tp] = w
	go w.run()
	w.recs <- recsRange(1)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	lostDone := make(chan struct{})
	go func() {
		c.partitionsLost(context.Background(), nil, map[string][]int32{tp.topic: {tp.partition}})
		close(lostDone)
	}()

	select {
	case err := <-handlerErr:
		if err == nil {
			t.Error("handler ctx.Err() = nil, want cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight handler was not canceled on partitions-lost — a fenced pod would keep processing a partition it no longer owns")
	}
	select {
	case <-lostDone:
	case <-time.After(5 * time.Second):
		t.Fatal("partitionsLost did not return after the worker drained")
	}
	if _, ok := c.workers[tp]; ok {
		t.Error("lost partition's worker still registered")
	}
}

// TestConsumer_PartitionsRevokedDrainsWithoutCancel pins the ordinary-rebalance
// contract that must NOT change: a graceful revoke lets the in-flight chunk
// finish and commit before the partition moves — no context cancellation, no
// lost work.
func TestConsumer_PartitionsRevokedDrainsWithoutCancel(t *testing.T) {
	const total = 3
	started := make(chan struct{})
	var startOnce sync.Once
	var mu sync.Mutex
	var ctxErrs []error
	handler := func(ctx context.Context, _ *Message) error {
		startOnce.Do(func() { close(started) })
		time.Sleep(100 * time.Millisecond) // slow-ish handler, does not watch ctx
		mu.Lock()
		ctxErrs = append(ctxErrs, ctx.Err())
		mu.Unlock()
		return nil
	}
	cc := &commitCapture{}
	c := newCommitTestConsumer(handler, cc)

	tp := topicPartition{testTopic, 0}
	w := newPartitionWorker(c, tp, context.Background())
	c.workers[tp] = w
	go w.run()
	w.recs <- recsRange(total)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	done := make(chan struct{})
	go func() {
		c.partitionsRevoked(context.Background(), nil, map[string][]int32{tp.topic: {tp.partition}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("partitionsRevoked did not return")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ctxErrs) != total {
		t.Fatalf("handler completed %d records, want %d (graceful revoke drains the in-flight chunk)", len(ctxErrs), total)
	}
	for i, err := range ctxErrs {
		if err != nil {
			t.Errorf("record %d: handler ctx canceled during graceful revoke: %v", i, err)
		}
	}
	batches := cc.get()
	if len(batches) != 1 || len(batches[0]) != total {
		t.Fatalf("expected the drained chunk committed before the partition moved, got %v", batches)
	}
}
