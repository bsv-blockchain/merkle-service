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
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestConsumer_CrashesOnPersistentUnknownTopicID reproduces the scale-ovh
// wedge of 15 Jul 2026: the redpanda operator (reconciling Topic CRs)
// deleted and recreated every merkle topic while the services were running.
// kgo pins a consumed topic's UUID at cursor creation and deliberately never
// adopts the recreated topic's new ID (kgo source.go: "a recreated topic
// stalls loudly (UNKNOWN_TOPIC_ID) ... and the user must purge+re-add"), so
// every consumer spun on UNKNOWN_TOPIC_ID fetch errors — ~10/s per partition,
// group Stable via background heartbeats, zero commits, for an hour — while
// the old poll loop classified the error as transient forever.
//
// The broker symptom is injected with a kfake fetch Control rather than a
// real out-of-band delete+recreate: a real recreation on kfake wedges the
// consumer just as hard (verified while writing this test) but SILENTLY —
// kgo's fetch session goes quiescent after a single stripped error, so the
// persistent UNKNOWN_TOPIC_ID stream redpanda produces never reaches
// PollFetches. The Control replays the observed redpanda behavior: every
// fetch response fails every requested partition with UNKNOWN_TOPIC_ID.
//
// The contract pinned here: when UNKNOWN_TOPIC_ID persists past
// unknownTopicIDCrashAfter, the consumer crashes the process (exitFunc, the
// F-053 zombie guard) so the orchestrator restarts it — a fresh client adopts
// the new topic ID and NewConsumer's EnsureTopics re-creates the topic if it
// is missing outright. Both wedge variants heal on restart.
func TestConsumer_CrashesOnPersistentUnknownTopicID(t *testing.T) {
	const topic = "recreated-topic"
	const partitions = 2

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(partitions, topic),
	)
	if err != nil {
		t.Fatalf("starting kfake cluster: %v", err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Shrink the escalation window so the test completes quickly; production
	// default is restored on cleanup.
	origCrashAfter := unknownTopicIDCrashAfter
	unknownTopicIDCrashAfter = 2 * time.Second
	defer func() { unknownTopicIDCrashAfter = origCrashAfter }()

	// F-053 crash-guard stub (see redelivery_test.go).
	exitCh := make(chan int, 1)
	origExit := exitFunc
	exitFunc = func(code int) {
		select {
		case exitCh <- code:
		default:
		}
	}
	defer func() { exitFunc = origExit }()

	var handled atomic.Int64
	handler := func(context.Context, *Message) error {
		handled.Add(1)
		return nil
	}

	c, err := NewConsumer(brokers, "recreate-group", []string{topic}, handler, nil, nil, logger)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = c.Stop() }()

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer prod.Close()

	// Healthy baseline: the consumer must be demonstrably consuming before
	// the wedge, so the crash below cannot be a startup artifact.
	if err := prod.ProduceSync(context.Background(), &kgo.Record{Value: []byte("pre")}).FirstErr(); err != nil {
		t.Fatalf("producing baseline record: %v", err)
	}
	waitFor(t, "baseline record consumed", 15*time.Second,
		func() bool { return handled.Load() > 0 })

	// Wedge the broker: from now on every fetch fails every requested
	// partition with UNKNOWN_TOPIC_ID, as redpanda does when the fetch
	// carries the UUID of a deleted-and-recreated topic.
	cluster.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		req := kreq.(*kmsg.FetchRequest)
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewFetchResponseTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.UnknownTopicID.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	// The consumer is now wedged on UNKNOWN_TOPIC_ID. It must escalate to a
	// process crash once the error persists past the (shrunken) threshold.
	select {
	case code := <-exitCh:
		if code != 1 {
			t.Fatalf("crash guard fired with exit code %d, want 1", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("consumer never crashed on persistent UNKNOWN_TOPIC_ID (handled=%d) — zombie state",
			handled.Load())
	}
}

// waitFor polls cond until it is true or the timeout elapses.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
