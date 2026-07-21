package nodes

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// weightView is an immutable snapshot published for lock-free hot-path reads.
// Weight/Ready are on the multi-million-tx scoring path; they must not take
// mutexes or rebuild maps.
type weightView struct {
	ready   bool
	weights map[string]int // peerID → count; never mutated after publish
}

// Registry is the shared-store-backed node weight map used by subtree scoring.
//
// Hot path (Weight/Ready): atomic.Value load only — no I/O, no locks.
// Cold path (RecordBlock): shared store first-seen insert + local chain update
// + publish new weightView. Block rate is ~O(1/min), so cost is irrelevant.
//
// Multi-replica: block-processor mutates the store; subtree-fetchers refresh
// from the store on a ticker so their weightView stays current without
// consuming the block topic on every fetcher.
type Registry struct {
	store  store.BlockAttributionStore
	window int
	logger *slog.Logger

	mu    sync.Mutex // protects chain mutations only
	chain *Chain

	view atomic.Value // *weightView

	// Optional background refresh for replicas that do not see RecordBlock.
	refreshStop chan struct{}
	refreshOnce sync.Once
}

// NewRegistry loads existing attributions from store and builds the chain.
// store may be nil only for pure unit tests that never call RecordBlock.
func NewRegistry(attrStore store.BlockAttributionStore, window int, logger *slog.Logger) (*Registry, error) {
	if window <= 0 {
		window = 100
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Registry{
		store:  attrStore,
		window: window,
		logger: logger,
		chain:  NewChain(window),
	}
	r.publishView() // empty view
	if attrStore != nil {
		attrs, err := attrStore.ListAll()
		if err != nil {
			return nil, fmt.Errorf("load block attributions: %w", err)
		}
		r.chain.Load(fromStoreAttrs(attrs))
		r.publishView()
		r.logger.Info("node registry loaded",
			"blocks", r.chain.Len(),
			"tip", r.chain.TipHash(),
			"ready", r.chain.Ready(),
			"window", window,
		)
	}
	return r, nil
}

// NewMemoryRegistry returns a registry backed by an in-process store (tests / single process).
func NewMemoryRegistry(window int, logger *slog.Logger) (*Registry, error) {
	return NewRegistry(NewMemoryBlockAttributionStore(), window, logger)
}

// StartBackgroundRefresh periodically reloads attributions from the shared
// store. Call from subtree-fetcher replicas that do not process block messages
// so Weight/Ready track tip changes. interval <= 0 defaults to 5s.
// Block rate is low; a multi-second lag on weight updates is acceptable.
func (r *Registry) StartBackgroundRefresh(interval time.Duration) {
	if r.store == nil {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	r.refreshOnce.Do(func() {
		r.refreshStop = make(chan struct{})
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-r.refreshStop:
					return
				case <-t.C:
					if err := r.Reload(); err != nil {
						r.logger.Warn("node registry refresh failed", "error", err)
					}
				}
			}
		}()
	})
}

// StopBackgroundRefresh stops the refresh goroutine if running.
func (r *Registry) StopBackgroundRefresh() {
	if r.refreshStop != nil {
		select {
		case <-r.refreshStop:
		default:
			close(r.refreshStop)
		}
	}
}

// RecordBlock attributes the first peer to announce this block hash, updates
// the tip path using header prevHash (approach C), and returns whether the
// registry is ready for scoring after this update.
func (r *Registry) RecordBlock(hash string, height uint32, headerHex, peerID string) (ready bool, err error) {
	if hash == "" || peerID == "" {
		return r.Ready(), nil
	}
	if r.store == nil {
		return false, fmt.Errorf("node registry has no attribution store")
	}

	prevHash, err := ParsePrevHash(headerHex)
	if err != nil {
		r.logger.Warn("failed to parse block header for node registry; recording without prevHash",
			"hash", hash,
			"error", err,
		)
		prevHash = ""
	}

	_, inserted, err := r.store.TryAttribute(hash, prevHash, peerID, height)
	if err != nil {
		return r.Ready(), fmt.Errorf("attribute block: %w", err)
	}
	if !inserted {
		return r.Ready(), nil
	}

	attr := BlockAttr{
		Hash:     hash,
		PrevHash: prevHash,
		Height:   height,
		PeerID:   peerID,
	}

	r.mu.Lock()
	first, orphaned := r.chain.Record(attr)
	if first {
		if len(orphaned) > 0 {
			r.logger.Info("node registry tip path changed (reorg or extension)",
				"tip", r.chain.TipHash(),
				"orphanedFromWindow", len(orphaned),
			)
		}
		if removed := r.chain.PruneBelow(0); removed > 0 {
			r.logger.Debug("pruned off-path block attributions from memory", "count", removed)
		}
		r.publishViewLocked()
	}
	ready = r.chain.Ready()
	r.mu.Unlock()
	return ready, nil
}

// Weight returns the number of tip-window blocks first-announced by peerID.
// Lock-free; safe on the per-txid scoring hot path.
func (r *Registry) Weight(peerID string) int {
	v := r.loadView()
	if v == nil || peerID == "" {
		return 0
	}
	return v.weights[peerID]
}

// Ready reports whether the tip path has at least W known blocks. Lock-free.
func (r *Registry) Ready() bool {
	v := r.loadView()
	return v != nil && v.ready
}

// Snapshot returns a copy of peerID → weight for the current window.
func (r *Registry) Snapshot() map[string]int {
	v := r.loadView()
	if v == nil {
		return map[string]int{}
	}
	out := make(map[string]int, len(v.weights))
	for k, n := range v.weights {
		out[k] = n
	}
	return out
}

// Reload refreshes chain state from the store (background refresh / startup).
func (r *Registry) Reload() error {
	if r.store == nil {
		return nil
	}
	attrs, err := r.store.ListAll()
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.chain.Load(fromStoreAttrs(attrs))
	r.publishViewLocked()
	r.mu.Unlock()
	return nil
}

func (r *Registry) loadView() *weightView {
	v, _ := r.view.Load().(*weightView)
	return v
}

func (r *Registry) publishView() {
	r.mu.Lock()
	r.publishViewLocked()
	r.mu.Unlock()
}

// publishViewLocked copies chain caches into an immutable weightView.
// Caller must hold r.mu.
func (r *Registry) publishViewLocked() {
	src := r.chain.Weights()
	cp := make(map[string]int, len(src))
	for k, n := range src {
		cp[k] = n
	}
	r.view.Store(&weightView{
		ready:   r.chain.Ready(),
		weights: cp,
	})
}

func fromStoreAttrs(attrs []store.BlockAttribution) []BlockAttr {
	out := make([]BlockAttr, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, BlockAttr{
			Hash:     a.Hash,
			PrevHash: a.PrevHash,
			Height:   a.Height,
			PeerID:   a.PeerID,
		})
	}
	return out
}
