//go:build integration

package store_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// TestSeenCounter_BatchDelete_Aerospike exercises the mine-time cleanup
// against a real cluster: deleted counters restart from a clean slate,
// untouched counters keep their history, and deleting absent txids is not an
// error (idempotent — safe on work-item redelivery).
func TestSeenCounter_BatchDelete_Aerospike(t *testing.T) {
	client := newAerospikeClient(t)
	setName := uniqueSet(t, "seen_del")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s := store.NewSeenCounterStore(client, setName, 3, 2, 10, logger)

	for _, txid := range []string{"tx-del-a", "tx-del-b", "tx-del-keep"} {
		for _, st := range []string{"st1", "st2"} {
			if _, err := s.Increment(txid, st); err != nil {
				t.Fatalf("Increment(%s, %s): %v", txid, st, err)
			}
		}
	}

	if err := s.BatchDelete([]string{"tx-del-a", "tx-del-b", "tx-del-absent"}); err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}

	res, err := s.Increment("tx-del-a", "st-new")
	if err != nil {
		t.Fatalf("Increment after delete: %v", err)
	}
	if res.NewCount != 1 {
		t.Errorf("count after delete+increment = %d, want 1 (fresh counter)", res.NewCount)
	}

	res, err = s.Increment("tx-del-keep", "st3")
	if err != nil {
		t.Fatalf("Increment(tx-del-keep): %v", err)
	}
	if res.NewCount != 3 {
		t.Errorf("tx-del-keep count = %d, want 3 (2 prior + 1 new)", res.NewCount)
	}
}
