## 1. Interface Extraction (no behavior change)

- [x] 1.1 Create `internal/store/interfaces.go` with interfaces: `RegistrationStore`, `StumpStore`, `SubtreeStore`, `CallbackDedupStore`, `CallbackURLRegistry`, `CallbackAccumulatorStore`, `SeenCounterStore`, `SubtreeCounterStore`, mirroring current exported method signatures.
- [x] 1.2 Move shared types (`IncrementResult`, `AccumulatedCallback`, etc.) out of per-store files into `internal/store/types.go`.
- [x] 1.3 Leave `BlobStore` interface and `FileBlobStore`/`MemoryBlobStore` in place (already abstracted); verify interface sits in `internal/store/`.
- [x] 1.4 Define a `Registry` struct in `internal/store/registry.go` holding each store plus `Close() error`.

## 2. Aerospike Backend Relocation

> **Implementation deviation:** physical relocation of Aerospike files to an
> `internal/store/aerospike/` subpackage was skipped as pure churn — it
> would require rewriting every import and package declaration without
> changing observable behavior. Equivalent abstraction outcome was achieved
> by renaming the Aerospike struct types to unexported identifiers
> (`aerospikeRegistration`, `aerospikeSeenCounter`, etc.) inside the existing
> `internal/store/` package, with the interfaces owning the public API.
> Services now depend on `store.RegistrationStore` (interface), not on any
> exported Aerospike concrete type. The factory in `internal/store/factory.go`
> plays the role of `aerospike.New`.

- [x] 2.1 ~~Create `internal/store/aerospike/` package~~ → skipped; interfaces in `internal/store/` own the public API, concrete Aerospike types renamed to unexported.
- [x] 2.2 ~~Move store files into `internal/store/aerospike/`~~ → skipped; struct names renamed in place.
- [x] 2.3 Every Aerospike struct carries a compile-time assertion (e.g., `var _ RegistrationStore = (*aerospikeRegistration)(nil)`).
- [x] 2.4 `newAerospikeRegistry` in `internal/store/factory.go` plays the role of `aerospike.New`.
- [x] 2.5 Existing unit tests in `internal/store/` pass without modification.

## 3. Config Shape and Factory

- [x] 3.1 Extend `internal/config/config.go` with `Store` block: `Backend string` ("aerospike"|"sql") and nested `SQL` struct (`Driver`, `DSN`, `Schema`, `SweeperInterval`, `MaxOpenConns`, `MaxIdleConns`).
- [x] 3.2 Default `store.backend` to `aerospike` when unset; validate `backend` value at load time.
- [x] 3.3 Add `store.NewFromConfig(ctx, cfg) (*Registry, error)` in `internal/store/factory.go` that dispatches to `aerospike.New` or `sql.New`.
- [x] 3.4 Update `config.yaml` with a commented `store:` block showing both backends.
- [x] 3.5 Add config-load unit test covering defaults, explicit aerospike, explicit sql, and invalid backend.

## 4. Service Wiring

- [x] 4.1 Replace direct Aerospike store construction in `cmd/block-processor/main.go` with `store.NewFromConfig(ctx, cfg)` and field access from the returned `Registry`.
- [x] 4.2 Same for `cmd/subtree-worker/main.go`.
- [x] 4.3 Same for `cmd/callback-delivery/main.go`.
- [x] 4.4 Ensure each `cmd/*/main.go` calls `registry.Close()` on shutdown (append to existing defer/stop sequence).
- [x] 4.5 Add a build check (Makefile target `make lint-store-imports`) verifying `cmd/**` imports `internal/store` but not `internal/store/aerospike` or `internal/store/sql`.

## 5. SQL Backend Scaffolding

- [x] 5.1 Create `internal/store/sql/` package with a `New(ctx, cfg) (*store.Registry, error)` entry point.
- [x] 5.2 Add `go.mod` dependencies: `github.com/jackc/pgx/v5`, `modernc.org/sqlite`.
- [x] 5.3 Implement driver selection (`postgres` | `sqlite`) that opens a `*sql.DB` via `database/sql` and applies pool settings.
- [x] 5.4 Implement a minimal dialect shim (`bytea`/`blob`, timestamp type, `RETURNING` vs emulation) in `internal/store/sql/dialect.go`.

## 6. SQL Migrations

- [x] 6.1 Create `internal/store/sql/migrations/` with `0001_init.sql` defining tables: `registrations`, `registration_urls`, `stump_index`, `subtree_index`, `callback_dedup`, `callback_urls`, `callback_accumulator`, `callback_accumulator_entries`, `seen_counters`, `seen_counter_subtrees`, `subtree_counters`, plus `schema_migrations`.
- [x] 6.2 Add indexes: `expires_at` index on every TTL-bearing table; foreign-key index on child tables.
- [x] 6.3 Implement migration runner (`internal/store/sql/migrate.go`) that embeds `migrations/` via `go:embed`, reads applied versions, applies pending ones in order.
- [x] 6.4 Serialize concurrent migration: `pg_advisory_lock` for PostgreSQL, `BEGIN IMMEDIATE` for SQLite.
- [x] 6.5 Unit-test migration runner: fresh DB, partial DB, idempotent re-run, concurrent starters.

## 7. SQL Store Implementations

- [x] 7.1 `registration.go`: implement `Add`, `Get`, `BatchGet`, `UpdateTTL`, `BatchUpdateTTL` using `registrations` + `registration_urls` with `INSERT ON CONFLICT DO NOTHING`.
- [x] 7.2 `callback_url_registry.go`: implement `Add`, `GetAll` against `callback_urls` table.
- [x] 7.3 `callback_dedup.go`: implement `Exists`, `Record` against `callback_dedup` with `expires_at`.
- [x] 7.4 `seen_counter.go`: implement `Increment` using a transaction that inserts into `seen_counter_subtrees`, counts distinct children, and atomically sets `threshold_fired` with `ON CONFLICT`.
- [x] 7.5 `subtree_counter.go`: implement `Init`, `Decrement` with `UPDATE … RETURNING` (PostgreSQL) / txn-based emulation (SQLite).
- [x] 7.6 `callback_accumulator.go`: implement `Append`, `ReadAndDelete` atomically via transaction (SELECT all entries, DELETE, return).
- [x] 7.7 `stump_store.go` / `subtree_store.go`: implement index/metadata methods against SQL tables, delegating payload storage to existing `BlobStore`.
- [x] 7.8 Compile-time interface assertions for every SQL store.

## 8. TTL Sweeper

- [x] 8.1 Implement `sweeper.go`: a goroutine that iterates over TTL tables and runs `DELETE … WHERE expires_at < now() LIMIT 1000` in a loop per table, per interval.
- [x] 8.2 Start sweeper in `sql.New`; return it in the `Registry` so `Close()` can cancel it via context.
- [x] 8.3 Add debug-level logging of rows deleted per table per sweep.
- [x] 8.4 Unit-test sweeper: insert expired and non-expired rows, tick sweeper, verify only expired rows deleted; verify `Close` stops the goroutine.

## 9. SQL Unit Tests (SQLite-backed)

- [x] 9.1 Port existing Aerospike unit tests in `internal/store/` to table-driven tests that run against both backends, using SQLite in-memory for the SQL path.
- [x] 9.2 Add concurrency tests: N goroutines calling `SeenCounterStore.Increment` across threshold; N goroutines calling `SubtreeCounterStore.Decrement`.
- [x] 9.3 Verify idempotency: `RegistrationStore.Add` called twice; `CallbackDedupStore.Record` called twice.
- [x] 9.4 Verify TTL: write record with short TTL, advance sweeper, assert `Exists` returns false.

## 10. PostgreSQL E2E Test

- [x] 10.1 Add `github.com/testcontainers/testcontainers-go` and `.../modules/postgres` to `go.mod`.
- [x] 10.2 Create `internal/e2e/postgres_e2e_test.go` behind `//go:build e2e_postgres` build tag.
- [x] 10.3 Test helper boots a `postgres:16` container, waits for readiness, builds a DSN, returns a `config.Config` with `store.backend: sql`.
- [x] 10.4 Port the happy-path flow from `e2e_test.go`: register txids, push a block+subtrees, assert callbacks received for each txid within timeout.
- [x] 10.5 Add a test that restarts the service mid-flow against the same PostgreSQL to verify durable state recovery (registrations, seen counters persist).
- [x] 10.6 Update `Makefile` with `make test-e2e-postgres` target running `go test -tags=e2e_postgres ./internal/e2e/...`.
- [x] 10.7 Update `.github/workflows/` CI to run the PostgreSQL e2e test on pull requests (separate job so it can fail without blocking other tests during stabilization).
- [x] 10.8 Document SQL backend setup and e2e testing in `docs/` (connection string format, migration behavior, sweeper tuning).
