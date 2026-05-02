//go:build e2e_postgres

package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	_ "github.com/bsv-blockchain/merkle-service/internal/store/sql" // register SQL backend
)

// startPostgres boots a disposable PostgreSQL container and returns a DSN the
// store package can use. Tests invoking this require Docker on the host.
func startPostgres(ctx context.Context, t *testing.T) (dsn string) {
	t.Helper()
	container, err := tcpostgres.Run(
		ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("merkle"),
		tcpostgres.WithUsername("merkle"),
		tcpostgres.WithPassword("merkle"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		if strings.Contains(err.Error(), "get provider") || strings.Contains(err.Error(), "Docker") {
			t.Skipf("Docker not available; skipping: %v", err)
		}
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres: %v", err)
		}
	})

	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

// newSQLConfig returns a config.Config pointing at the given DSN with the SQL
// backend selected. BlobStore is in-memory (file:/// on a temp dir) so stump
// and subtree payload storage is isolated per test.
func newSQLConfig(t *testing.T, dsn string) *config.Config {
	t.Helper()
	return &config.Config{
		Store: config.StoreConfig{
			Backend: config.BackendSQL,
			SQL: config.StoreSQLConfig{
				Driver:          "postgres",
				DSN:             dsn,
				SweeperInterval: "200ms",
				MaxOpenConns:    10,
				MaxIdleConns:    2,
			},
		},
		BlobStore: config.BlobStoreConfig{
			URL: "file://" + t.TempDir(),
		},
		Subtree: config.SubtreeConfig{
			DAHOffset:      1,
			StumpDAHOffset: 6,
		},
		Callback: config.CallbackConfig{
			SeenThreshold: 3,
			DedupTTLSec:   3600,
		},
		Aerospike: config.AerospikeConfig{
			SubtreeCounterTTLSec:      600,
			CallbackAccumulatorTTLSec: 600,
		},
	}
}

func TestPostgres_RegistrySmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQL e2e in short mode")
	}
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	cfg := newSQLConfig(t, dsn)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	registry, err := store.NewFromConfig(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	if registry.Health == nil || !registry.Health.Healthy() {
		t.Fatal("backend should be healthy right after migrations")
	}
}

func TestPostgres_RegistrationRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQL e2e in short mode")
	}
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	cfg := newSQLConfig(t, dsn)

	reg1, err := store.NewFromConfig(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("NewFromConfig #1: %v", err)
	}

	// Register 100 txids, each with 2 URLs. Duplicates on every Add test idempotency.
	txids := make([]string, 100)
	for i := range txids {
		txids[i] = fmt.Sprintf("tx%064d", i)
		if err := reg1.Registration.Add(txids[i], "http://cb-one"); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := reg1.Registration.Add(txids[i], "http://cb-two"); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := reg1.Registration.Add(txids[i], "http://cb-one"); err != nil { // duplicate
			t.Fatalf("Add dup: %v", err)
		}
	}

	got, err := reg1.Registration.BatchGet(txids)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(got) != len(txids) {
		t.Fatalf("BatchGet returned %d txids, want %d", len(got), len(txids))
	}
	for _, txid := range txids {
		urls := got[txid]
		if len(urls) != 2 {
			t.Fatalf("%s: urls=%v, want 2", txid, urls)
		}
	}

	// Close and reopen — durable state survives restart.
	if err := reg1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reg2, err := store.NewFromConfig(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("NewFromConfig #2 (reopen): %v", err)
	}
	t.Cleanup(func() { _ = reg2.Close() })

	got2, err := reg2.Registration.BatchGet(txids)
	if err != nil {
		t.Fatalf("BatchGet after reopen: %v", err)
	}
	for _, txid := range txids {
		urls := got2[txid]
		if len(urls) != 2 {
			t.Fatalf("after reopen, %s: urls=%v, want 2", txid, urls)
		}
	}
}

func TestPostgres_SeenCounterThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQL e2e in short mode")
	}
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	cfg := newSQLConfig(t, dsn)

	registry, err := store.NewFromConfig(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	txid := "tx" + fmt.Sprintf("%064d", 1)

	firedCount := 0
	for i := 0; i < 6; i++ {
		res, err := registry.SeenCounter.Increment(txid, fmt.Sprintf("st-%d", i))
		if err != nil {
			t.Fatalf("Increment: %v", err)
		}
		if res.ThresholdReached {
			firedCount++
		}
	}
	if firedCount != 1 {
		t.Fatalf("ThresholdReached fired %d times, want exactly 1", firedCount)
	}
}

func TestPostgres_SubtreeCounterConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQL e2e in short mode")
	}
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	cfg := newSQLConfig(t, dsn)

	registry, err := store.NewFromConfig(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	const initial = 100
	blockHash := "block-001"
	if err := registry.SubtreeCounter.Init(blockHash, initial); err != nil {
		t.Fatalf("Init: %v", err)
	}

	type outcome struct {
		val int
		err error
	}
	results := make(chan outcome, initial)
	for i := 0; i < initial; i++ {
		go func() {
			v, err := registry.SubtreeCounter.Decrement(blockHash)
			results <- outcome{v, err}
		}()
	}

	seen := map[int]bool{}
	for i := 0; i < initial; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("Decrement: %v", r.err)
		}
		if seen[r.val] {
			t.Fatalf("duplicate decrement result %d", r.val)
		}
		seen[r.val] = true
	}
	for want := 0; want < initial; want++ {
		if !seen[want] {
			t.Fatalf("missing result %d", want)
		}
	}
}

func TestPostgres_CallbackDedupTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQL e2e in short mode")
	}
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	cfg := newSQLConfig(t, dsn)

	registry, err := store.NewFromConfig(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	// 1s TTL. The expiry filter inside Exists should observe it as gone after 2s.
	if err := registry.CallbackDedup.Record("tx-x", "u", "MINED", 1*time.Second); err != nil {
		t.Fatalf("Record: %v", err)
	}
	ok, err := registry.CallbackDedup.Exists("tx-x", "u", "MINED")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Exists=false immediately after Record")
	}
	time.Sleep(2 * time.Second)
	ok, err = registry.CallbackDedup.Exists("tx-x", "u", "MINED")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Exists=true after TTL elapsed")
	}

	// Sweeper runs every 200ms per our cfg — give it a window to physically delete.
	time.Sleep(500 * time.Millisecond)
	// No direct assertion on row count here (the Registry doesn't expose raw
	// db access); the sweeper test in internal/store/sql verifies physical
	// deletion at the row level.
}

func TestPostgres_AccumulatorReadAndDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQL e2e in short mode")
	}
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	cfg := newSQLConfig(t, dsn)

	registry, err := store.NewFromConfig(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	blockHash := "blk-accum"
	if err := registry.CallbackAccumulator.Append(blockHash, "http://u1", []string{"a", "b"}, 0, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := registry.CallbackAccumulator.Append(blockHash, "http://u1", []string{"c"}, 1, []byte{0x02}); err != nil {
		t.Fatal(err)
	}
	if err := registry.CallbackAccumulator.Append(blockHash, "http://u2", []string{"d"}, 0, []byte{0x03}); err != nil {
		t.Fatal(err)
	}

	got, err := registry.CallbackAccumulator.ReadAndDelete(blockHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d URLs, want 2", len(got))
	}
	if len(got["http://u1"].Entries) != 2 {
		t.Fatalf("u1 entries = %d, want 2", len(got["http://u1"].Entries))
	}

	// Second call returns nil — entries were deleted.
	got2, err := registry.CallbackAccumulator.ReadAndDelete(blockHash)
	if err != nil {
		t.Fatal(err)
	}
	if got2 != nil {
		t.Fatalf("second ReadAndDelete should be nil, got %v", got2)
	}
}
