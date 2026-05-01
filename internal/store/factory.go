package store

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// SQLBackendFactory is installed by the SQL backend package (via an init-time
// registration, below) to avoid a hard import dependency from the core store
// package into the SQL driver code. Keeping the SQL driver out of the root
// package means consumers that only use Aerospike don't compile SQL support in.
type SQLBackendFactory func(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Registry, error)

var sqlBackend SQLBackendFactory

// RegisterSQLBackend is called by the SQL backend package via an import side
// effect. Not safe for concurrent use — should only be called from package init.
func RegisterSQLBackend(fn SQLBackendFactory) {
	sqlBackend = fn
}

// NewFromConfig dispatches on cfg.Store.Backend and returns a populated
// Registry. The caller is responsible for Close() on shutdown.
//
// For the Aerospike backend, the Aerospike client, all Aerospike-backed stores,
// and the BlobStore-backed stump/subtree stores are constructed and bundled.
// For the SQL backend, the SQL driver is opened, migrations applied, and all
// stores implemented against SQL tables are bundled. Stump/subtree storage
// continues to delegate to the BlobStore configured via cfg.BlobStore.URL.
func NewFromConfig(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Registry, error) {
	switch cfg.Store.Backend {
	case config.BackendAerospike, "":
		return newAerospikeRegistry(ctx, cfg, logger)
	case config.BackendSQL:
		if sqlBackend == nil {
			return nil, fmt.Errorf("sql backend selected but not compiled in; import _ \"github.com/bsv-blockchain/merkle-service/internal/store/sql\"")
		}
		return sqlBackend(ctx, cfg, logger)
	default:
		return nil, fmt.Errorf("unknown store backend %q", cfg.Store.Backend)
	}
}

// newAerospikeRegistry constructs every Aerospike-backed store plus the
// BlobStore-backed stump/subtree stores using cfg, wires them into a Registry,
// and registers a closer for the Aerospike client.
func newAerospikeRegistry(_ context.Context, cfg *config.Config, logger *slog.Logger) (*Registry, error) {
	asClient, err := NewAerospikeClientFromConfig(cfg.Aerospike, logger)
	if err != nil {
		return nil, fmt.Errorf("aerospike client: %w", err)
	}

	blob, err := NewBlobStoreFromURL(cfg.BlobStore.URL)
	if err != nil {
		asClient.Close()
		return nil, fmt.Errorf("blob store: %w", err)
	}

	r := &Registry{
		Registration: NewRegistrationStore(
			asClient, cfg.Aerospike.SetName,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		Subtree: NewSubtreeStore(blob, uint64(cfg.Subtree.DAHOffset), logger),
		Stump:   NewStumpStore(blob, uint64(cfg.Subtree.StumpDAHOffset), logger),
		CallbackDedup: NewCallbackDedupStore(
			asClient, cfg.Aerospike.CallbackDedupSet,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		CallbackURLRegistry: NewCallbackURLRegistry(
			asClient, cfg.Aerospike.CallbackURLRegistry,
			cfg.Aerospike.CallbackURLRegistryTTLSec,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		CallbackAccumulator: NewCallbackAccumulatorStore(
			asClient, cfg.Aerospike.CallbackAccumulatorSet, cfg.Aerospike.CallbackAccumulatorTTLSec,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		SeenCounter: NewSeenCounterStore(
			asClient, cfg.Aerospike.SeenSet, cfg.Callback.SeenThreshold,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		SubtreeCounter: NewSubtreeCounterStore(
			asClient, cfg.Aerospike.SubtreeCounterSet, cfg.Aerospike.SubtreeCounterTTLSec,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		Health: asClient,
	}
	r.AddCloser(func() error { asClient.Close(); return nil })
	return r, nil
}
