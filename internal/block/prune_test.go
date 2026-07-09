package block

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// These tests pin the block-time half of blob-store pruning. The
// subtree-fetcher stores subtree blobs at announcement time, when the block
// height is unknown, so those writes carry no delete-at-height. The worker is
// the first component that sees the blob WITH its height — it must therefore
// (a) schedule deletion for blobs that are already present (the healthy path
// where no re-store happens), and (b) advance the store's prune height so
// schedules actually fire in the worker process. Without both, blobs written
// by the fetcher are never deleted and the shared volume fills until the
// pipeline wedges.

// TestProcessBlockSubtree_SchedulesPruneForPreStoredBlob covers (a): a blob
// present in the store (announcement-time write, no DAH) must come out of
// ProcessBlockSubtree with a delete scheduled at blockHeight + dahOffset —
// exactly the schedule the DataHub-refetch path gets via StoreSubtree.
func TestProcessBlockSubtree_SchedulesPruneForPreStoredBlob(t *testing.T) {
	rawBytes := buildRawSubtreeBytes(t, 2)

	blob := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blob, 1, testLogger()) // dahOffset = 1

	// Announcement-time write: height unknown, no DAH.
	if err := subtreeStore.StoreSubtree("st-hit", rawBytes, 0); err != nil {
		t.Fatalf("StoreSubtree: %v", err)
	}

	// Block time: the blob is found in the store, so no DataHub fetch happens
	// (the URL is unroutable to prove it).
	_, err := ProcessBlockSubtree(
		context.Background(),
		"st-hit", 10, "blk-prune", "http://unroutable.invalid",
		datahub.NewClient(1, 0, testLogger()),
		subtreeStore,
		&reprocessRegStore{},
		nil, nil,
		0,
		"", "",
		nil,
		testLogger(),
	)
	if err != nil {
		t.Fatalf("ProcessBlockSubtree: %v", err)
	}

	subtreeStore.SetCurrentBlockHeight(10)
	if _, err := subtreeStore.GetSubtree("st-hit"); err != nil {
		t.Fatalf("subtree must survive prune below blockHeight+dahOffset: %v", err)
	}

	subtreeStore.SetCurrentBlockHeight(11) // 10 + dahOffset 1
	if _, err := subtreeStore.GetSubtree("st-hit"); err == nil {
		t.Error("pre-stored subtree must be scheduled for pruning at block time (blockHeight+dahOffset)")
	}
}

// TestHandleMessage_AdvancesPruneHeight covers (b): handling a work item for
// a block at height H must advance the worker's subtree store to H, firing
// any schedules at or below it. (The work message height is 200 — see
// makeWorkMessageBytes.)
func TestHandleMessage_AdvancesPruneHeight(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	blob := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blob, 1, logger)

	// A blob from an earlier block, scheduled to die at height 150.
	if err := blob.Set("old-blob", []byte("stale")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := blob.ScheduleDelete("old-blob", 150); err != nil {
		t.Fatalf("ScheduleDelete: %v", err)
	}

	counter := newCountingSubtreeCounter()
	_ = counter.Init("block-prune", 1, nil)
	counter.initCalls = 0

	subtreePayload := buildRawSubtreeBytes(t, 2)
	server := rawSubtreeServer(subtreePayload)
	defer server.Close()

	s := &SubtreeWorkerService{
		blockCfg: config.BlockConfig{
			MaxAttempts:    5,
			PostMineTTLSec: 0,
		},
		regStore:       &staticRegStore{urls: []string{"http://cb.example.test/hook"}},
		subtreeStore:   subtreeStore,
		stumpStore:     &stubStumpStore{},
		subtreeCounter: counter,
	}
	s.InitBase("subtree-worker-test")
	s.Logger = logger
	s.dataHubClient = datahub.NewClient(5, 0, logger)
	s.callbackProducer = kafka.NewTestProducer(&callbackFailingProducer{}, "callback-test", logger)
	s.retryProducer = kafka.NewTestProducer(&callbackFailingProducer{}, "subtree-work-test", logger)
	s.dlqProducer = kafka.NewTestProducer(&callbackFailingProducer{}, "subtree-work-dlq-test", logger)

	value := makeWorkMessageBytes(t, "block-prune", "subtree-prune", server.URL, 0)
	if err := s.handleMessage(context.Background(), &kafka.Message{Value: value}); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	if _, err := blob.Get("old-blob"); !errors.Is(err, store.ErrBlobNotFound) {
		t.Errorf("blob scheduled at 150 must be pruned after handling a height-200 work item; Get err = %v", err)
	}
}
