package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // postgres driver
	_ "modernc.org/sqlite"             // pure-go sqlite driver

	"github.com/bsv-blockchain/merkle-service/internal/config"
	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

// init registers the SQL backend with the core store package so that
// store.NewFromConfig can dispatch to it without importing this package.
func init() {
	storepkg.RegisterSQLBackend(New)
}

// New opens the configured SQL database, applies migrations, starts the TTL
// sweeper, and returns a Registry whose stores are all SQL-backed. The caller
// must invoke Registry.Close() on shutdown to stop the sweeper and close the
// database.
func New(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*storepkg.Registry, error) {
	d, driverName, err := resolveDriver(cfg.Store.SQL.Driver)
	if err != nil {
		return nil, err
	}
	dsn := cfg.Store.SQL.DSN
	if dsn == "" {
		return nil, fmt.Errorf("store.sql.dsn is required when store.backend=sql")
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", driverName, err)
	}

	if n := cfg.Store.SQL.MaxOpenConns; n > 0 {
		db.SetMaxOpenConns(n)
	}
	if n := cfg.Store.SQL.MaxIdleConns; n > 0 {
		db.SetMaxIdleConns(n)
	}

	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping %s: %w", driverName, err)
	}

	if err = runMigrations(ctx, db, d, logger); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	// BlobStore stays file/memory — SQL backend only owns metadata/index tables.
	blob, err := storepkg.NewBlobStoreFromURL(cfg.BlobStore.URL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("blob store: %w", err)
	}

	sweepInterval := parseDuration(cfg.Store.SQL.SweeperInterval, 60*time.Second)
	urlRetention := parseDuration(cfg.Store.SQL.CallbackURLRegistryRetention, defaultCallbackURLRetention)
	sweeperCtx, cancelSweeper := context.WithCancel(context.Background())
	sw := newSweeper(db, d, sweepInterval, urlRetention, logger)
	go sw.run(sweeperCtx)

	r := &storepkg.Registry{
		Registration:        newRegistrationStore(db, d, cfg.Registry.MaxCallbacksPerTxID),
		Subtree:             storepkg.NewSubtreeStore(blob, uint64(cfg.Subtree.DAHOffset), logger),    //nolint:gosec // config-validated int
		Stump:               storepkg.NewStumpStore(blob, uint64(cfg.Subtree.StumpDAHOffset), logger), //nolint:gosec // config-validated int
		CallbackDedup:       newCallbackDedup(db, d),
		CallbackURLRegistry: newCallbackURLRegistry(db, d, urlRetention),
		CallbackAccumulator: newCallbackAccumulator(db, d, cfg.Aerospike.CallbackAccumulatorTTLSec),
		SeenCounter:         newSeenCounter(db, d, cfg.Callback.SeenThreshold),
		SubtreeCounter:      newSubtreeCounter(db, d, cfg.Aerospike.SubtreeCounterTTLSec),
		Health:              &pingHealth{db: db},
	}
	r.AddCloser(func() error {
		cancelSweeper()
		sw.waitStopped()
		return db.Close()
	})
	return r, nil
}

func resolveDriver(name string) (*dialect, string, error) {
	switch name {
	case "postgres", "pgx", "":
		return postgresDialect(), "pgx", nil
	case "sqlite", "sqlite3":
		return sqliteDialect(), "sqlite", nil
	default:
		return nil, "", fmt.Errorf("unknown store.sql.driver %q (supported: postgres, sqlite)", name)
	}
}

func parseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// pingHealth adapts a *sql.DB to store.BackendHealth.
type pingHealth struct{ db *sql.DB }

func (h *pingHealth) Healthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return h.db.PingContext(ctx) == nil
}
