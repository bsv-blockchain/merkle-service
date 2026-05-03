//go:build scale

package scale

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// preloadRegistrations loads all txid→callbackURL registrations into Aerospike.
// Uses parallel workers for large datasets.
func preloadRegistrations(manifest *Manifest, txids [][]byte, regStore *store.RegistrationStore, logger *slog.Logger) error {
	workers := 10
	if len(manifest.ArcadeInstances) < workers {
		workers = len(manifest.ArcadeInstances)
	}

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	var completed int

	ch := make(chan ArcadeInstance, len(manifest.ArcadeInstances))
	for _, arcade := range manifest.ArcadeInstances {
		ch <- arcade
	}
	close(ch)

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for arcade := range ch {
				for j := arcade.TxidStart; j < arcade.TxidEnd; j++ {
					var h chainhash.Hash
					copy(h[:], txids[j])
					txidStr := h.String()

					if err := regStore.Add(txidStr, arcade.CallbackURL, ""); err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("adding registration for txid index %d: %w", j, err)
						}
						mu.Unlock()
						return
					}
				}

				mu.Lock()
				completed++
				if completed%10 == 0 {
					txidsPerInstance := arcade.TxidEnd - arcade.TxidStart
					logger.Info("pre-loaded registrations", "arcadeInstances", completed, "txids", completed*txidsPerInstance)
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// preloadCallbackURLRegistry adds all callback URLs to the broadcast registry.
func preloadCallbackURLRegistry(manifest *Manifest, urlRegistry *store.CallbackURLRegistry) error {
	for _, arcade := range manifest.ArcadeInstances {
		if err := urlRegistry.Add(arcade.CallbackURL, ""); err != nil {
			return fmt.Errorf("adding callback URL for arcade %d: %w", arcade.Index, err)
		}
	}
	return nil
}

// preloadSubtrees loads all subtree binary data into the subtree store.
func preloadSubtrees(manifest *Manifest, subtreeData map[string][]byte, subtreeStore *store.SubtreeStore) error {
	for _, st := range manifest.Subtrees {
		data, ok := subtreeData[st.Hash]
		if !ok {
			return fmt.Errorf("missing subtree data for hash %s", st.Hash)
		}
		if err := subtreeStore.StoreSubtree(st.Hash, data, uint64(manifest.BlockHeight)); err != nil {
			return fmt.Errorf("storing subtree %s: %w", st.Hash, err)
		}
	}
	return nil
}
