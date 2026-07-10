package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConsumer_RewindStormSurvivesRebalance is the liveness regression test
// for the dev-ovh-1 subtree-fetcher wedge of 8 Jul 2026. When every handler
// invocation fails (there: disk full AND the DLQ topic missing), every
// partition cycles through the rewind path concurrently. The old rewind
// implementation called kgo's SetOffsets from the partition-worker
// goroutines — usage the kgo docs warn is "prone to odd interactions" unless
// concurrent revokes are blocked — and under a simultaneous rebalance the
// consumer deadlocked: offsets frozen, zero CPU, group member kept alive by
// background heartbeats, forever.
//
// The pinned contract: under a permanent rewind storm across many
// partitions, with continuous production and group-membership churn, the
// consumer keeps redelivering (handler call count keeps rising) and Stop()
// completes promptly.
func TestConsumer_RewindStormSurvivesRebalance(t *testing.T) {
	const topic = "rewind-storm"
	const partitions = 12

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

	// Continuous production across all partitions for the whole test, so
	// fetch buffers and worker channels stay hot like the production storm.
	prodCtx, stopProd := context.WithCancel(context.Background())
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		stopProd()
		t.Fatalf("creating producer: %v", err)
	}
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		i := 0
		for prodCtx.Err() == nil {
			for p := int32(0); p < partitions; p++ {
				prod.Produce(prodCtx, &kgo.Record{
					Partition: p,
					Value:     []byte(fmt.Sprintf("m%d", i)),
				}, nil)
			}
			i++
			time.Sleep(5 * time.Millisecond)
		}
	}()
	// Teardown order matters: stop the producer goroutine and wait for it to
	// exit BEFORE closing the client, so no Produce call races the close.
	defer func() {
		stopProd()
		<-prodDone
		prod.Close()
	}()

	// F-053 crash-guard stub (see redelivery_test.go).
	exitCh := make(chan int, 1)
	origExit := exitFunc
	exitFunc = func(code int) { exitCh <- code }
	defer func() { exitFunc = origExit }()

	var calls atomic.Int64
	failingHandler := func(context.Context, *Message) error {
		calls.Add(1)
		return errors.New("permanent handler failure (storm)")
	}

	c1, err := NewConsumer(brokers, "storm-group", []string{topic}, failingHandler, nil, logger)
	if err != nil {
		t.Fatalf("NewConsumer(c1): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c1.Start(ctx); err != nil {
		t.Fatalf("Start(c1): %v", err)
	}

	waitFor := func(what string, timeout time.Duration, cond func() bool) {
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

	waitFor("first handler failures", 15*time.Second, func() bool { return calls.Load() > 0 })

	// Membership churn mid-storm: join and leave a second member twice,
	// forcing revokes/assigns to interleave with in-flight rewinds.
	for i := 0; i < 2; i++ {
		c2, cErr := NewConsumer(brokers, "storm-group", []string{topic}, failingHandler, nil, logger)
		if cErr != nil {
			t.Fatalf("NewConsumer(c2 #%d): %v", i, cErr)
		}
		c2Ctx, c2Cancel := context.WithCancel(context.Background())
		if sErr := c2.Start(c2Ctx); sErr != nil {
			t.Fatalf("Start(c2 #%d): %v", i, sErr)
		}
		time.Sleep(1500 * time.Millisecond)
		c2Cancel()
		if stopErr := c2.Stop(); stopErr != nil {
			t.Fatalf("Stop(c2 #%d): %v", i, stopErr)
		}
	}

	// Liveness: redelivery must still be happening after the churn.
	before := calls.Load()
	waitFor("handler calls to keep rising after rebalance churn", 15*time.Second,
		func() bool { return calls.Load() > before })

	// And shutdown must complete promptly — a deadlocked poll loop hangs Stop.
	cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- c1.Stop() }()
	select {
	case sErr := <-stopped:
		if sErr != nil {
			t.Fatalf("Stop(c1): %v", sErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Stop(c1) hung — consumer deadlocked")
	}

	select {
	case code := <-exitCh:
		t.Fatalf("consumer crash-guard fired unexpectedly (exit %d)", code)
	default:
	}
}
