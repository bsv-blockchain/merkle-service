package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// commitCapture records every commitRecords call a partition worker makes so
// tests can assert on commit cadence, ordering, and contents without a broker.
type commitCapture struct {
	mu      sync.Mutex
	batches [][]*kgo.Record
	err     error // returned from every capture call when non-nil
}

func (cc *commitCapture) commit(_ context.Context, recs ...*kgo.Record) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	batch := make([]*kgo.Record, len(recs))
	copy(batch, recs)
	cc.batches = append(cc.batches, batch)
	return cc.err
}

func (cc *commitCapture) get() [][]*kgo.Record {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	out := make([][]*kgo.Record, len(cc.batches))
	copy(out, cc.batches)
	return out
}

// newCommitTestConsumer builds a Consumer whose commit path is captured by cc
// instead of hitting a broker. Only the fields the partition-worker path
// touches are populated — the same struct-literal pattern the processBatch
// unit tests use.
func newCommitTestConsumer(handler MessageHandler, cc *commitCapture) *Consumer {
	return &Consumer{
		groupID:       "commit-progress-group",
		handler:       handler,
		logger:        discardLogger(),
		workers:       make(map[topicPartition]*partitionWorker),
		rewindReqs:    make(map[topicPartition]*kgo.Record),
		resumeAt:      make(map[topicPartition]time.Time),
		commitRecords: cc.commit,
	}
}

// recsRange builds n consecutive records starting at offset 0 on topic
// "test" partition 0 (see rec in consumer_test.go).
func recsRange(n int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		out[i] = rec(int64(i))
	}
	return out
}

// TestPartitionWorker_CommitsProgressMidBatch is the regression test for the
// dev-ovh-1 commit freeze (Jul 2026): with commits issued only after an entire
// fetched batch completed, a 50k-message backlog with 1-2s handlers meant the
// committed offset stayed frozen for the whole (hours-long) batch and every
// pod restart reprocessed from scratch. The worker must instead commit the
// successful prefix every commitEvery records, so progress is durable while a
// long batch is still in flight.
func TestPartitionWorker_CommitsProgressMidBatch(t *testing.T) {
	handler := func(_ context.Context, _ *Message) error { return nil }
	cc := &commitCapture{}
	c := newCommitTestConsumer(handler, cc)

	const total = 2*commitEvery + 20 // three chunks: full, full, partial
	w := newPartitionWorker(c, topicPartition{"test", 0}, context.Background())
	w.process(recsRange(total))

	batches := cc.get()
	if len(batches) != 3 {
		t.Fatalf("expected 3 incremental commits (%d/%d/20 records), got %d", commitEvery, commitEvery, len(batches))
	}
	wantSizes := []int{commitEvery, commitEvery, 20}
	next := int64(0)
	for i, b := range batches {
		if len(b) != wantSizes[i] {
			t.Errorf("commit %d: %d records, want %d", i, len(b), wantSizes[i])
		}
		for _, r := range b {
			if r.Offset != next {
				t.Fatalf("commit %d: offset %d out of order, want %d", i, r.Offset, next)
			}
			next++
		}
	}
	if next != total {
		t.Errorf("committed %d records total, want %d", next, total)
	}
}

// TestPartitionWorker_MidBatchFailureCommitsPrefixOnly pins F-030 semantics
// under incremental commits: a failure inside a chunk commits only that
// chunk's successful prefix, records the failed record as the rewind point,
// and processes nothing after it.
func TestPartitionWorker_MidBatchFailureCommitsPrefixOnly(t *testing.T) {
	const failAt = commitEvery + 10 // second chunk, 10 records in
	var calls int
	handler := func(_ context.Context, m *Message) error {
		calls++
		if m.Offset == failAt {
			return errors.New("boom")
		}
		return nil
	}
	cc := &commitCapture{}
	c := newCommitTestConsumer(handler, cc)

	tp := topicPartition{"test", 0}
	w := newPartitionWorker(c, tp, context.Background())
	w.process(recsRange(3 * commitEvery))

	batches := cc.get()
	if len(batches) != 2 {
		t.Fatalf("expected 2 commits (first chunk + prefix of second), got %d", len(batches))
	}
	if got := len(batches[0]); got != commitEvery {
		t.Errorf("first commit: %d records, want %d", got, commitEvery)
	}
	if got := len(batches[1]); got != 10 {
		t.Errorf("second commit: %d records, want 10 (prefix before the failure)", got)
	}
	if last := batches[1][len(batches[1])-1].Offset; last != failAt-1 {
		t.Errorf("last committed offset = %d, want %d (never past the failure)", last, failAt-1)
	}
	if calls != failAt+1 {
		t.Errorf("handler invoked %d times, want %d (nothing after the failed record)", calls, failAt+1)
	}

	c.rewindMu.Lock()
	req := c.rewindReqs[tp]
	c.rewindMu.Unlock()
	if req == nil || req.Offset != failAt {
		t.Fatalf("rewind request = %v, want offset %d", req, failAt)
	}
}

// TestConsumerOpts_BoundsFetchSizes verifies the consumer bounds how much
// data one poll may return (per partition and total) instead of inheriting
// kgo's 1 MiB / 50 MiB defaults — under a deep backlog those defaults hand a
// worker thousands of records per batch (hours of work at incident-observed
// handler latencies). Introspected via kgo's OptValue since franz options are
// otherwise opaque.
func TestConsumerOpts_BoundsFetchSizes(t *testing.T) {
	client, err := kgo.NewClient(consumerOpts([]string{"localhost:9092"}, "test-group", []string{"test"})...)
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	defer client.Close()

	if got := client.OptValue(kgo.FetchMaxPartitionBytes).(int32); got != fetchMaxPartitionBytes {
		t.Errorf("FetchMaxPartitionBytes = %d, want %d", got, fetchMaxPartitionBytes)
	}
	if got := client.OptValue(kgo.FetchMaxBytes).(int32); got != fetchMaxBytes {
		t.Errorf("FetchMaxBytes = %d, want %d", got, fetchMaxBytes)
	}
}

// TestPartitionWorker_StopSkipsRemainingChunks verifies that a worker told to
// stop (revoke, lost, shutdown) finishes and commits its current chunk but
// does not start the next one — the uncommitted tail is redelivered to the
// partition's next owner instead of holding the rebalance for the whole
// fetched batch.
func TestPartitionWorker_StopSkipsRemainingChunks(t *testing.T) {
	cc := &commitCapture{}
	var c *Consumer
	var w *partitionWorker
	var calls int
	handler := func(_ context.Context, m *Message) error {
		calls++
		if m.Offset == 10 {
			w.signalStop()
		}
		return nil
	}
	c = newCommitTestConsumer(handler, cc)
	w = newPartitionWorker(c, topicPartition{"test", 0}, context.Background())

	w.process(recsRange(3 * commitEvery))

	if calls != commitEvery {
		t.Errorf("handler invoked %d times, want %d (current chunk finishes, later chunks don't start)", calls, commitEvery)
	}
	batches := cc.get()
	if len(batches) != 1 || len(batches[0]) != commitEvery {
		t.Fatalf("expected exactly the current chunk committed, got %d commits: %v", len(batches), batches)
	}
}
