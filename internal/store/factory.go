package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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

// Startup retry policy for NewFromConfigWithRetry: 5 attempts with the
// backoff doubling from 8s — 8+16+32+64 = 120s of waiting, so a binary rides
// out ~2 minutes of backend unavailability before giving up.
const (
	registryRetryAttempts  = 5
	registryRetryBaseDelay = 8 * time.Second
)

// NewFromConfigWithRetry is NewFromConfig with a bounded startup retry. Every
// binary builds its store registry first thing at startup, and a transient
// backend blip there used to exit the process immediately: on dev-ovh-1 the
// api-server pods crash-looped 5-7 times each on a single Aerospike
// "command execution timed out" while the cluster recovered from the disk
// incident. A slow backend at boot is an operational condition to wait out,
// not a configuration error — but a persistent failure must still fail
// startup so the orchestrator surfaces it.
func NewFromConfigWithRetry(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Registry, error) {
	return newRegistryWithRetry(ctx, registryRetryAttempts, registryRetryBaseDelay, logger, func() (*Registry, error) {
		return NewFromConfig(ctx, cfg, logger)
	})
}

// newRegistryWithRetry runs build up to attempts times, sleeping baseDelay
// (doubling per attempt) between failures. Every failed attempt is logged so
// a crash-looping pod's history is visible in the log stream, not just the
// restart counter. The context aborts a pending wait (SIGTERM mid-backoff).
func newRegistryWithRetry(ctx context.Context, attempts int, baseDelay time.Duration, logger *slog.Logger, build func() (*Registry, error)) (*Registry, error) {
	var lastErr error
	delay := baseDelay
	for attempt := 1; attempt <= attempts; attempt++ {
		r, err := build()
		if err == nil {
			if attempt > 1 {
				logger.Info("store registry built after retry", "attempt", attempt)
			}
			return r, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		logger.Warn("failed to build store registry; backing off before retry",
			"attempt", attempt,
			"maxAttempts", attempts,
			"retryIn", delay.String(),
			"error", err,
		)
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, fmt.Errorf("store registry build aborted while backing off: %w", ctx.Err())
		}
		delay *= 2
	}
	return nil, fmt.Errorf("store registry build failed after %d attempts: %w", attempts, lastErr)
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
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs,
			cfg.Registry.MaxCallbacksPerTxID, logger,
		),
		Subtree: NewSubtreeStore(blob, uint64(cfg.Subtree.DAHOffset), logger),    //nolint:gosec // config-validated int
		Stump:   NewStumpStore(blob, uint64(cfg.Subtree.StumpDAHOffset), logger), //nolint:gosec // config-validated int
		CallbackDedup: NewCallbackDedupStore(
			asClient, cfg.Aerospike.CallbackDedupSet,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		CallbackURLRegistry: NewCallbackURLRegistry(
			asClient, cfg.Aerospike.CallbackURLRegistry,
			cfg.Aerospike.CallbackURLRegistryTTLSec,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		DataHubRegistry: NewDataHubRegistry(
			asClient, cfg.Aerospike.DataHubRegistry,
			cfg.Aerospike.DataHubRegistryTTLSec,
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
		ExpectedStump: NewExpectedStumpStore(
			asClient, cfg.Aerospike.ExpectedStumpSet, cfg.Aerospike.SubtreeCounterTTLSec,
			cfg.Aerospike.MaxRetries, cfg.Aerospike.RetryBaseMs, logger,
		),
		Health: asClient,
	}
	r.AddCloser(func() error { asClient.Close(); return nil })

	// Backstop for blobs whose height is never learned (announced but never
	// mined) — see StartBlobSweeperFromConfig. No-op for memory stores.
	stopSweeper := StartBlobSweeperFromConfig(blob, cfg.BlobStore, logger)
	r.AddCloser(func() error { stopSweeper(); return nil })

	return r, nil
}
