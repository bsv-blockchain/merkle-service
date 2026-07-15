package block

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// enospcErr is what a full filesystem actually returns from a blob write: an
// *fs.PathError wrapping syscall.ENOSPC, further wrapped by the store layer.
// Mirrors internal/subtree/processor_backpressure_test.go.
func enospcErr() error {
	return fmt.Errorf("writing blob: %w", &fs.PathError{Op: "write", Path: "/data/stumps/ab/cd.stump", Err: syscall.ENOSPC})
}

// notFoundErr mimics the worker's real 404 chain: datahub wraps ErrNotFound
// with %w at the 404 site, and intermediate stages wrap again.
func notFoundErr() error {
	return fmt.Errorf("fetching subtree from datahub: %w",
		fmt.Errorf("%w: http://peer.example.test/subtree/aa (HTTP 404)", datahub.ErrNotFound))
}

// subtreeWorkCount returns the current value of the named outcome counter on
// merkle_subtree_work_messages_total. Tests assert on deltas against a
// pre-call snapshot since the underlying Prometheus counter is process-global
// and accumulates across tests.
func subtreeWorkCount(outcome string) int64 {
	return int64(testutil.ToFloat64(metrics.SubtreeWorkMessagesTotal.WithLabelValues(outcome)))
}

// newWorkerForTransientFailure builds the minimal SubtreeWorkerService needed
// to drive handleTransientFailure directly: block config, counter, and the
// (retry, dlq) producers. The callback producer stays nil — the DLQ branch's
// decrement can still run because countingSubtreeCounter is pre-seeded above
// zero, so no BLOCK_PROCESSED emission is triggered.
func newWorkerForTransientFailure(
	t *testing.T,
	retry, dlq kafka.Publisher,
	counter *countingSubtreeCounter,
	blockCfg config.BlockConfig,
) *SubtreeWorkerService {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &SubtreeWorkerService{
		blockCfg:       blockCfg,
		subtreeCounter: counter,
	}
	s.InitBase("subtree-worker-backpressure-test")
	s.Logger = logger
	s.retryProducer = kafka.NewTestProducer(retry, "subtree-work-test", logger)
	s.dlqProducer = kafka.NewTestProducer(dlq, "subtree-work-dlq-test", logger)
	return s
}

// TestWorkerHandleTransientFailure_CountsRetriedOutcome pins the visibility
// the 2026-07-15 incident lacked: every retry republish increments
// merkle_subtree_work_messages_total{outcome="retried"}.
func TestWorkerHandleTransientFailure_CountsRetriedOutcome(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 5})

	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-metric-retry", SubtreeHash: "aa11", AttemptCount: 0}
	before := subtreeWorkCount(metrics.OutcomeRetried)
	if err := svc.handleTransientFailure(context.Background(), msg, errors.New("blip")); err != nil {
		t.Fatalf("handleTransientFailure: %v", err)
	}
	if got := subtreeWorkCount(metrics.OutcomeRetried) - before; got != 1 {
		t.Errorf("retried outcome delta = %d, want 1", got)
	}
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected 1 retry publish, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected 0 DLQ publishes, got %d", got)
	}
}

// TestWorkerHandleTransientFailure_CountsDLQOutcome pins the terminal branch:
// a max-attempts DLQ hand-off increments outcome="dlq".
func TestWorkerHandleTransientFailure_CountsDLQOutcome(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	// Pre-seed above zero so the DLQ-branch decrement does not reach the
	// BLOCK_PROCESSED emit path (callback producer is nil in this harness).
	_ = counter.Init("blk-metric-dlq", 2, nil)
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 3})

	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-metric-dlq", SubtreeHash: "aa22", AttemptCount: 2}
	before := subtreeWorkCount(metrics.OutcomeDLQ)
	if err := svc.handleTransientFailure(context.Background(), msg, errors.New("blip")); err != nil {
		t.Fatalf("handleTransientFailure: %v", err)
	}
	if got := subtreeWorkCount(metrics.OutcomeDLQ) - before; got != 1 {
		t.Errorf("dlq outcome delta = %d, want 1", got)
	}
	if got := dlqMock.sentCount(); got != 1 {
		t.Errorf("expected 1 DLQ publish, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected 0 retry publishes, got %d", got)
	}
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected exactly 1 counter decrement on DLQ, got %d", got)
	}
}

// --- retry backoff schedule ---

// TestWorkerRetryBackoff_ExponentialWithCap pins the worker's backoff
// schedule: base doubling per attempt, capped at workerRetryBackoffCap (5s,
// NOT the fetcher's 30s — the worker's 10-attempt budget would live at a
// bigger cap and a failing 50-record consumer chunk would sleep past the
// group's 5m rebalance timeout and get the member fenced).
func TestWorkerRetryBackoff_ExponentialWithCap(t *testing.T) {
	s := &SubtreeWorkerService{blockCfg: config.BlockConfig{RetryBackoffBaseMs: 1000}}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 5 * time.Second},  // 8s capped at 5s
		{10, 5 * time.Second}, // deep in the budget, still 5s
		{50, 5 * time.Second}, // shift overflow guard
	}
	for _, tc := range cases {
		if got := s.retryBackoff(tc.attempt); got != tc.want {
			t.Errorf("retryBackoff(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}

	// 0 disables (struct-literal test configs keep existing tests sleep-free).
	s.blockCfg.RetryBackoffBaseMs = 0
	if got := s.retryBackoff(3); got != 0 {
		t.Errorf("retryBackoff with base 0 = %v, want 0 (disabled)", got)
	}
}

// --- backoff-before-retry ---

// TestWorkerHandleTransientFailure_BacksOffBeforeRetry verifies a transient
// failure waits its backoff before re-publishing, so the bounded retry budget
// spans real time instead of burning out in milliseconds (the 2026-07-15
// scale-ovh amplification).
func TestWorkerHandleTransientFailure_BacksOffBeforeRetry(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 5, RetryBackoffBaseMs: 60})

	// AttemptCount 1 -> next attempt 2 -> base*2 = 120ms.
	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-backoff", SubtreeHash: "aa33", AttemptCount: 1}
	begin := time.Now()
	if err := svc.handleTransientFailure(context.Background(), msg, errors.New("blip")); err != nil {
		t.Fatalf("handleTransientFailure: %v", err)
	}
	if elapsed := time.Since(begin); elapsed < 120*time.Millisecond {
		t.Errorf("retry republished after %v, want >= 120ms backoff (attempt 2, base 60ms)", elapsed)
	}
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected 1 retry publish after backoff, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected 0 DLQ publishes, got %d", got)
	}
	if got := counter.decrementCount(); got != 0 {
		t.Errorf("expected 0 counter decrements on retry, got %d", got)
	}
}

// TestWorkerHandleTransientFailure_BackoffAbortsOnContextCancel verifies a
// canceled consumer context (shutdown, lost partition) interrupts the backoff
// and surfaces an error WITHOUT re-publishing — the unacked original is
// redelivered instead, so nothing is lost or duplicated by the abort.
func TestWorkerHandleTransientFailure_BackoffAbortsOnContextCancel(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 5, RetryBackoffBaseMs: 60_000})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-backoff-cancel", SubtreeHash: "aa44"}
	begin := time.Now()
	err := svc.handleTransientFailure(ctx, msg, errors.New("blip"))
	if err == nil {
		t.Fatal("expected an error when the backoff is interrupted by context cancellation")
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Fatalf("backoff did not abort on cancel (took %v)", elapsed)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected no retry publish after interrupted backoff, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected no DLQ publish after interrupted backoff, got %d", got)
	}
}

// --- disk-full parking ---

// TestWorkerHandleTransientFailure_DiskFullNeverDLQs is the worker-side
// regression test for the ENOSPC data loss the fetcher fixed in v0.4.6: a
// full disk is an operational condition, not bad data, so it must never
// consume the retry budget, never decrement the per-block counter, and never
// route to subtree-work-dlq (from which there is no replay). The handler
// surfaces an error, the consumer parks the message by not advancing past it,
// and Kafka retention keeps it safe until the disk recovers.
func TestWorkerHandleTransientFailure_DiskFullNeverDLQs(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 3, RetryBackoffBaseMs: 1})

	// AttemptCount 2 with MaxAttempts 3: an ordinary transient failure would
	// DLQ right here. Disk-full must not.
	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-diskfull", SubtreeHash: "aa55", AttemptCount: 2}
	beforeParked := subtreeWorkCount(metrics.OutcomeParkedDiskFull)
	err := svc.handleTransientFailure(context.Background(), msg, enospcErr())
	if err == nil {
		t.Fatal("expected an error so the consumer parks the message (no ack, no advance)")
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("returned error should wrap the ENOSPC cause, got: %v", err)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("disk-full must never DLQ, got %d DLQ publishes", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("disk-full must not burn the retry budget, got %d retry publishes", got)
	}
	if got := counter.decrementCount(); got != 0 {
		t.Errorf("disk-full must not decrement the per-block counter, got %d decrements", got)
	}
	if msg.AttemptCount != 2 {
		t.Errorf("AttemptCount mutated to %d, want unchanged 2 (parked, not retried)", msg.AttemptCount)
	}
	if got := subtreeWorkCount(metrics.OutcomeParkedDiskFull) - beforeParked; got != 1 {
		t.Errorf("parked_disk_full outcome delta = %d, want 1", got)
	}
}

// TestWorkerHandleTransientFailure_DiskQuotaMessageNeverDLQs covers
// quota-style full-filesystem errors that arrive as text (e.g. from a store
// layer that did not preserve the errno chain).
func TestWorkerHandleTransientFailure_DiskQuotaMessageNeverDLQs(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 3, RetryBackoffBaseMs: 1})

	for _, cause := range []error{
		errors.New("mkdir /data/stumps/ab: disk quota exceeded"),
		errors.New("write /data/stumps/ab/cd.stump: no space left on device"),
	} {
		msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-quota", SubtreeHash: "aa66", AttemptCount: 2}
		if err := svc.handleTransientFailure(context.Background(), msg, cause); err == nil {
			t.Errorf("cause %q: expected parking error, got nil", cause)
		}
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("quota-style disk-full must never DLQ, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("quota-style disk-full must not retry-publish, got %d", got)
	}
	if got := counter.decrementCount(); got != 0 {
		t.Errorf("quota-style disk-full must not decrement, got %d", got)
	}
}

// TestHandleMessage_DiskFullOnStumpPut_ParksMessage drives the incident path
// end to end: subtree processing succeeds, the STUMP blob write fails with
// ENOSPC, and handleMessage must surface an error (parking the message)
// without touching the DLQ, the retry topic, or the counter. This pins that
// ENOSPC survives the publishSubtreeCallbacks error wrap.
func TestHandleMessage_DiskFullOnStumpPut_ParksMessage(t *testing.T) {
	cbMock := &callbackFailingProducer{}
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}

	counter := newCountingSubtreeCounter()
	stumpStore := &stubStumpStore{putErr: enospcErr()}

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	svc := newWorkerForHandleMessage(t, cbMock, retryMock, dlqMock, stumpStore, counter, 3)

	// AttemptCount = maxAttempts-1: an ordinary transient failure would DLQ
	// right here; disk-full must park instead.
	value := makeWorkMessageBytes(t, "block-diskfull-e2e", contentAddressOf(t, subtreePayload), server.URL, 2)
	if err := svc.handleMessage(context.Background(), &kafka.Message{Value: value}); err == nil {
		t.Fatal("expected handleMessage to return an error so the consumer does not advance past the message")
	}
	if stumpStore.puts == 0 {
		t.Fatal("stump store was never written — test did not reach the STUMP Put stage")
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected 0 DLQ publishes on disk-full, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected 0 retry publishes on disk-full, got %d", got)
	}
	if got := counter.decrementCount(); got != 0 {
		t.Errorf("expected 0 counter decrements on disk-full, got %d", got)
	}
}

// --- DataHub 404 reduced budget ---

// TestWorkerHandleTransientFailure_NotFound_ReducedBudgetDLQs verifies a
// datahub.ErrNotFound failure DLQs once AttemptCount+1 reaches
// notFoundMaxAttempts — well before block.maxAttempts. Teranode's asset cache
// prunes subtree data after ~2h, so a late 404 is effectively permanent;
// burning the full 10-attempt budget on it amplified the scale-ovh 404 storm.
func TestWorkerHandleTransientFailure_NotFound_ReducedBudgetDLQs(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	_ = counter.Init("blk-nf-dlq", 2, nil)
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 10, NotFoundMaxAttempts: 3, RetryBackoffBaseMs: 1})

	// AttemptCount 2 -> next attempt 3 >= notFoundMaxAttempts 3 -> DLQ,
	// even though maxAttempts (10) is nowhere near exhausted.
	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-nf-dlq", SubtreeHash: "aa77", AttemptCount: 2}
	if err := svc.handleTransientFailure(context.Background(), msg, notFoundErr()); err != nil {
		t.Fatalf("handleTransientFailure: expected nil after DLQ hand-off, got: %v", err)
	}
	if got := dlqMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 DLQ publish at not-found budget, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected 0 retry publishes at not-found budget, got %d", got)
	}
	// Terminal: BLOCK_PROCESSED must still be able to fire — counter MUST be
	// decremented exactly once.
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected exactly 1 counter decrement on not-found DLQ, got %d", got)
	}
}

// TestWorkerHandleTransientFailure_NotFound_BelowBudgetRetriesWithBackoff
// verifies a 404 below the reduced budget still retries (with its backoff):
// worker subtrees provably existed at announcement time, so a short retry can
// win — unlike the fetcher, which DLQs announcement-time 404s immediately.
func TestWorkerHandleTransientFailure_NotFound_BelowBudgetRetriesWithBackoff(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 10, NotFoundMaxAttempts: 3, RetryBackoffBaseMs: 60})

	// AttemptCount 0 -> next attempt 1 < notFoundMaxAttempts 3 -> retry after
	// base*1 = 60ms.
	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-nf-retry", SubtreeHash: "aa88", AttemptCount: 0}
	begin := time.Now()
	if err := svc.handleTransientFailure(context.Background(), msg, notFoundErr()); err != nil {
		t.Fatalf("handleTransientFailure: %v", err)
	}
	if elapsed := time.Since(begin); elapsed < 60*time.Millisecond {
		t.Errorf("not-found retry republished after %v, want >= 60ms backoff (attempt 1, base 60ms)", elapsed)
	}
	if got := retryMock.sentCount(); got != 1 {
		t.Errorf("expected 1 retry publish below not-found budget, got %d", got)
	}
	if got := dlqMock.sentCount(); got != 0 {
		t.Errorf("expected 0 DLQ publishes below not-found budget, got %d", got)
	}
	if got := counter.decrementCount(); got != 0 {
		t.Errorf("expected 0 counter decrements on retry, got %d", got)
	}
}

// TestWorkerHandleTransientFailure_NotFoundBudgetCappedByMaxAttempts pins the
// hard ceiling: maxAttempts stays an independent DLQ clause, so a
// misconfigured notFoundMaxAttempts > maxAttempts cannot extend the budget.
func TestWorkerHandleTransientFailure_NotFoundBudgetCappedByMaxAttempts(t *testing.T) {
	retryMock := &callbackFailingProducer{}
	dlqMock := &callbackFailingProducer{}
	counter := newCountingSubtreeCounter()
	_ = counter.Init("blk-nf-cap", 2, nil)
	svc := newWorkerForTransientFailure(t, retryMock, dlqMock, counter,
		config.BlockConfig{MaxAttempts: 3, NotFoundMaxAttempts: 20, RetryBackoffBaseMs: 1})

	// AttemptCount 2 -> next attempt 3 >= maxAttempts 3 -> DLQ under the
	// maxAttempts clause despite the (misconfigured) not-found budget of 20.
	msg := &kafka.SubtreeWorkMessage{BlockHash: "blk-nf-cap", SubtreeHash: "aa99", AttemptCount: 2}
	if err := svc.handleTransientFailure(context.Background(), msg, notFoundErr()); err != nil {
		t.Fatalf("handleTransientFailure: expected nil after DLQ hand-off, got: %v", err)
	}
	if got := dlqMock.sentCount(); got != 1 {
		t.Errorf("expected exactly 1 DLQ publish at the maxAttempts ceiling, got %d", got)
	}
	if got := retryMock.sentCount(); got != 0 {
		t.Errorf("expected 0 retry publishes at the maxAttempts ceiling, got %d", got)
	}
	if got := counter.decrementCount(); got != 1 {
		t.Errorf("expected exactly 1 counter decrement, got %d", got)
	}
}
