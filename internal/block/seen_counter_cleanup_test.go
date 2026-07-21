package block

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// fakeSeenCounter records BatchDelete calls. The other SeenCounterStore
// methods are unused on the block path.
type fakeSeenCounter struct {
	deleted [][]string
}

func (f *fakeSeenCounter) Increment(string, string) (*store.IncrementResult, error) {
	return &store.IncrementResult{}, nil
}

func (f *fakeSeenCounter) Threshold() int { return 3 }

func (f *fakeSeenCounter) AddPeer(txid, peerID string, weight int) (*store.IncrementResult, error) {
	return &store.IncrementResult{}, nil
}

func (f *fakeSeenCounter) BatchAddPeer(txids []string, peerID string, weight int) (map[string]*store.IncrementResult, error) {
	return map[string]*store.IncrementResult{}, nil
}

func (f *fakeSeenCounter) BatchDelete(txids []string) error {
	cp := make([]string, len(txids))
	copy(cp, txids)
	f.deleted = append(f.deleted, cp)
	return nil
}

// TestProcessBlockSubtree_DeletesSeenCountersForMinedTxids pins the
// event-driven half of seen-counter cleanup: a seen counter exists to track
// pre-mine propagation of a registered txid, so the moment the txid is mined
// the counter is dead weight — ProcessBlockSubtree must batch-delete the
// counters for the subtree's registered txids. (Counters only ever exist for
// registered txids: the fetcher increments after a registration match.)
func TestProcessBlockSubtree_DeletesSeenCountersForMinedTxids(t *testing.T) {
	rawBytes := buildRawSubtreeBytes(t, 2)
	txids, err := datahub.ParseRawTxids(rawBytes)
	if err != nil {
		t.Fatalf("ParseRawTxids: %v", err)
	}

	blob := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blob, 1, testLogger())
	if storeErr := subtreeStore.StoreSubtree("st-seen", rawBytes, 0); storeErr != nil {
		t.Fatalf("StoreSubtree: %v", storeErr)
	}

	regStore := &reprocessRegStore{byTxID: map[string][]store.CallbackEntry{
		txids[0]: {{URL: "https://cb.example/hook"}},
		txids[1]: {{URL: "https://cb.example/hook"}},
	}}
	seen := &fakeSeenCounter{}

	_, err = ProcessBlockSubtree(
		context.Background(),
		"st-seen", 10, "blk-seen", "http://unroutable.invalid",
		datahub.NewClient(1, 0, testLogger()),
		subtreeStore,
		regStore,
		nil, nil,
		0,
		"", "",
		seen,
		testLogger(),
	)
	if err != nil {
		t.Fatalf("ProcessBlockSubtree: %v", err)
	}

	if len(seen.deleted) != 1 {
		t.Fatalf("expected exactly one BatchDelete call, got %d", len(seen.deleted))
	}
	got := map[string]bool{}
	for _, txid := range seen.deleted[0] {
		got[txid] = true
	}
	if !got[txids[0]] || !got[txids[1]] || len(got) != 2 {
		t.Errorf("BatchDelete txids = %v, want exactly the registered mined txids %v", seen.deleted[0], txids)
	}
}
