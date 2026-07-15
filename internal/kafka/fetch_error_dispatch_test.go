package kafka

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestProcessFetches_DispatchesRecordsDespiteFetchErrors pins the second
// lesson of the scale-ovh wedge of 15 Jul 2026: a poll whose Fetches carry
// BOTH records and a per-partition error must still dispatch the records.
//
// The old poll loop `continue`d past dispatch whenever any non-tick fetch
// error was present. That starved every healthy partition for as long as one
// partition kept erroring — and worse: kgo advances its in-memory fetch
// positions for records the moment PollFetches returns them, so records
// skipped this way are never re-fetched in-session; a later commit on the
// same partition then seals the loss permanently (F-030 violation).
//
// Driven through processFetches with hand-built Fetches because a real
// broker delivers the mixed records-plus-errors poll only under timing
// races; the shape itself is what must be handled.
func TestProcessFetches_DispatchesRecordsDespiteFetchErrors(t *testing.T) {
	const healthyTopic = "healthy-topic"
	const wedgedTopic = "wedged-topic"

	// A real (kfake-backed) client so processFetches' AllowRebalance and
	// rewind plumbing run against genuine kgo internals; the Fetches under
	// test are injected, not polled.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, healthyTopic))
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var handled atomic.Int64
	handler := func(context.Context, *Message) error {
		handled.Add(1)
		return nil
	}

	c, err := NewConsumer(cluster.ListenAddrs(), "dispatch-group", []string{healthyTopic}, handler, nil, nil, logger)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer func() { _ = c.closeClient() }()

	// Register a worker for the healthy partition by hand — the consumer is
	// deliberately not Started, so processFetches (normally poll-goroutine
	// code) can be driven synchronously here.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tp := topicPartition{healthyTopic, 0}
	w := newPartitionWorker(c, tp, ctx)
	c.workers[tp] = w
	go w.run()
	defer w.stop()

	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{
			{
				Topic: wedgedTopic,
				Partitions: []kgo.FetchPartition{
					{Partition: 0, Err: kerr.UnknownTopicID},
				},
			},
			{
				Topic: healthyTopic,
				Partitions: []kgo.FetchPartition{
					{Partition: 0, Records: []*kgo.Record{
						{Topic: healthyTopic, Partition: 0, Offset: 0, Value: []byte("must-not-be-dropped")},
					}},
				},
			},
		},
	}}

	if exit := c.processFetches(ctx, fetches); exit {
		t.Fatal("processFetches reported a fatal exit for a transient per-partition error")
	}

	waitFor(t, "record dispatched despite concurrent fetch error", 5*time.Second,
		func() bool { return handled.Load() == 1 })
}
