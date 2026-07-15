package subtree

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

	"github.com/bsv-blockchain/merkle-service/internal/cache"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// failingSubtreeStore implements store.SubtreeStore, failing every write with
// storeErr. Used to drive the "storing subtree" transient-failure stage the
// way a full blob-store volume did on dev-ovh-1.
type failingSubtreeStore struct {
	storeErr error
	calls    int
}

func (f *failingSubtreeStore) StoreSubtree(string, []byte, uint64) error {
	f.calls++
	return f.storeErr
}

func (f *failingSubtreeStore) StoreSubtreeFromReader(string, io.Reader, int64, uint64) error {
	f.calls++
	return f.storeErr
}
func (f *failingSubtreeStore) GetSubtree(string) ([]byte, error) { return nil, f.storeErr }
func (f *failingSubtreeStore) GetSubtreeReader(string) (io.ReadCloser, error) {
	return nil, f.storeErr
}
func (f *failingSubtreeStore) DeleteSubtree(string) error          { return nil }
func (f *failingSubtreeStore) ScheduleDelete(string, uint64) error { return nil }
func (f *failingSubtreeStore) SetCurrentBlockHeight(uint64)        {}

// enospcErr is what a full filesystem actually returns from a blob write: an
// *fs.PathError wrapping syscall.ENOSPC, further wrapped by the store layer.
func enospcErr() error {
	return fmt.Errorf("writing blob: %w", &fs.PathError{Op: "write", Path: "/data/subtrees/ab/cd.subtree", Err: syscall.ENOSPC})
}

// TestRetryBackoff_ExponentialWithCap pins the backoff schedule: base doubles
// per attempt and is capped at subtreeRetryBackoffCap. On dev-ovh-1 the three
// retry attempts of a blob-store write burned in ~300ms, so a 15-minute disk
// incident converted 1,406 subtrees into dead letters.
func TestRetryBackoff_ExponentialWithCap(t *testing.T) {
	p := &Processor{cfg: &config.Config{Subtree: config.SubtreeConfig{RetryBackoffBaseMs: 1000}}}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{6, 30 * time.Second},  // 32s capped
		{50, 30 * time.Second}, // shift overflow guard
	}
	for _, tc := range cases {
		if got := p.retryBackoff(tc.attempt); got != tc.want {
			t.Errorf("retryBackoff(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}

	// 0 disables (struct-literal test configs); nil cfg likewise.
	p.cfg.Subtree.RetryBackoffBaseMs = 0
	if got := p.retryBackoff(3); got != 0 {
		t.Errorf("retryBackoff with base 0 = %v, want 0 (disabled)", got)
	}
	if got := (&Processor{}).retryBackoff(3); got != 0 {
		t.Errorf("retryBackoff with nil cfg = %v, want 0", got)
	}
}

// TestHandleTransientFailure_BacksOffBeforeRetry verifies a transient failure
// waits its backoff before re-publishing, so the bounded retry budget spans
// real time instead of ~100ms per attempt.
func TestHandleTransientFailure_BacksOffBeforeRetry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{MaxAttempts: 3, RetryBackoffBaseMs: 60},
		},
		retryProducer: kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:   kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
	}
	p.InitBase("subtree-backoff-test")
	p.Logger = logger

	msg := &kafka.SubtreeMessage{Hash: "subtree-backoff", AttemptCount: 1} // next attempt 2 -> base*2
	begin := time.Now()
	if err := p.handleTransientFailure(context.Background(), msg, "storing subtree", errors.New("blip"), time.Now()); err != nil {
		t.Fatalf("handleTransientFailure: %v", err)
	}
	if elapsed := time.Since(begin); elapsed < 120*time.Millisecond {
		t.Errorf("retry republished after %v, want >= 120ms backoff (attempt 2, base 60ms)", elapsed)
	}
	if got := len(retryMock.getMessages()); got != 1 {
		t.Errorf("expected 1 retry publish after backoff, got %d", got)
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected 0 DLQ publishes, got %d", got)
	}
}

// TestHandleTransientFailure_BackoffAbortsOnContextCancel verifies a canceled
// consumer context (shutdown, lost partition) interrupts the backoff and
// surfaces an error WITHOUT re-publishing — the unacked original is
// redelivered instead, so nothing is lost or duplicated by the abort.
func TestHandleTransientFailure_BackoffAbortsOnContextCancel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{MaxAttempts: 3, RetryBackoffBaseMs: 60_000},
		},
		retryProducer: kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:   kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
	}
	p.InitBase("subtree-backoff-cancel-test")
	p.Logger = logger

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	msg := &kafka.SubtreeMessage{Hash: "subtree-backoff-cancel"}
	begin := time.Now()
	err := p.handleTransientFailure(ctx, msg, "storing subtree", errors.New("blip"), time.Now())
	if err == nil {
		t.Fatal("expected an error when the backoff is interrupted by context cancellation")
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Fatalf("backoff did not abort on cancel (took %v)", elapsed)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected no retry publish after interrupted backoff, got %d", got)
	}
}

// TestHandleTransientFailure_DiskFullNeverDLQs is the regression test for the
// ENOSPC data loss: a full disk is an operational condition, not bad data, so
// it must never consume the retry budget nor route to subtree-dlq (from which
// there is no replay — every registered tx in the subtree loses its callbacks
// permanently). Instead the handler surfaces an error, the consumer parks the
// message by not advancing past it, and Kafka retention keeps it safe until
// the disk recovers.
func TestHandleTransientFailure_DiskFullNeverDLQs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{MaxAttempts: 3, RetryBackoffBaseMs: 1},
		},
		retryProducer: kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:   kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
	}
	p.InitBase("subtree-diskfull-test")
	p.Logger = logger

	// AttemptCount 2 with MaxAttempts 3: an ordinary transient failure would
	// DLQ right here. Disk-full must not.
	msg := &kafka.SubtreeMessage{Hash: "subtree-diskfull", AttemptCount: 2}
	beforeParked := subtreeCount(metrics.OutcomeParkedDiskFull)
	err := p.handleTransientFailure(context.Background(), msg, "storing subtree", enospcErr(), time.Now())
	if err == nil {
		t.Fatal("expected an error so the consumer parks the message (no ack, no advance)")
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("returned error should wrap the ENOSPC cause, got: %v", err)
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("disk-full must never DLQ, got %d DLQ publishes", got)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("disk-full must not burn the retry budget, got %d retry publishes", got)
	}
	if msg.AttemptCount != 2 {
		t.Errorf("AttemptCount mutated to %d, want unchanged 2 (parked, not retried)", msg.AttemptCount)
	}
	if got := subtreeCount(metrics.OutcomeParkedDiskFull) - beforeParked; got != 1 {
		t.Errorf("parked_disk_full outcome delta = %d, want 1", got)
	}
}

// TestHandleTransientFailure_DiskQuotaMessageNeverDLQs covers quota-style
// full-filesystem errors that arrive as text (e.g. from a store layer that
// did not preserve the errno chain).
func TestHandleTransientFailure_DiskQuotaMessageNeverDLQs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{MaxAttempts: 3, RetryBackoffBaseMs: 1},
		},
		retryProducer: kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:   kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
	}
	p.InitBase("subtree-quota-test")
	p.Logger = logger

	for _, cause := range []error{
		errors.New("mkdir /data/subtrees/ab: disk quota exceeded"),
		errors.New("write /data/subtrees/ab/cd.subtree: no space left on device"),
	} {
		msg := &kafka.SubtreeMessage{Hash: "subtree-quota", AttemptCount: 2}
		if err := p.handleTransientFailure(context.Background(), msg, "storing subtree", cause, time.Now()); err == nil {
			t.Errorf("cause %q: expected parking error, got nil", cause)
		}
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("quota-style disk-full must never DLQ, got %d", got)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("quota-style disk-full must not retry-publish, got %d", got)
	}
}

// TestHandleMessage_DiskFullOnStore_ParksMessage drives the incident path end
// to end: DataHub fetch succeeds, the blob-store write fails with ENOSPC, and
// handleMessage must surface an error (parking the message) without touching
// the DLQ, the retry topic, or the dedup cache.
func TestHandleMessage_DiskFullOnStore_ParksMessage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registeredTxid := "9602604163d73e2ab424bad28b1363694c397512dfa883ec1ee90cc92f847359"
	dataHubServer := startRawSubtreeServer(hashFromHex(t, registeredTxid))
	defer dataHubServer.Close()

	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	blobStore := &failingSubtreeStore{storeErr: enospcErr()}
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{
				MaxAttempts:        3,
				StorageMode:        "realtime",
				RetryBackoffBaseMs: 1,
			},
		},
		registrationStore: &mockRegStore{registrations: map[string][]string{}},
		seenCounterStore:  &mockSeenCounter{},
		subtreeStore:      blobStore,
		dedupCache:        cache.NewDedupCache(16),
		retryProducer:     kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:       kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
		dataHubClient:     datahub.NewClient(5, 0, logger),
	}
	p.InitBase("subtree-diskfull-e2e-test")
	p.Logger = logger

	msg := &kafka.SubtreeMessage{Hash: "subtree-diskfull-e2e", DataHubURL: dataHubServer.URL}
	value, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode subtree msg: %v", err)
	}

	if err := p.handleMessage(t.Context(), &kafka.Message{Value: value}); err == nil {
		t.Fatal("expected handleMessage to return an error so the consumer does not advance past the message")
	}
	if blobStore.calls == 0 {
		t.Fatal("blob store was never written — test did not reach the storing-subtree stage")
	}
	if got := len(dlqMock.getMessages()); got != 0 {
		t.Errorf("expected 0 DLQ publishes on disk-full, got %d", got)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry publishes on disk-full, got %d", got)
	}
	if p.dedupCache.Contains(msg.Hash) {
		t.Error("dedup cache must not mark a parked subtree as processed — redelivery would be skipped")
	}
}
