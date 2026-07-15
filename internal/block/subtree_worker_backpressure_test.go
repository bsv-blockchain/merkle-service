package block

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

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
