## Why

The store layer is tightly coupled to Aerospike: every concrete store in `internal/store/` holds an `*AerospikeClient` and uses Aerospike-specific features (CDT list ops, TTLs, atomic counters) directly. This blocks operators who cannot run Aerospike (compliance, ops familiarity, smaller deployments) from adopting the Merkle Service, and makes the service harder to test since the e2e suite requires a live Aerospike instance. Abstracting the store layer behind interfaces and adding a SQL implementation unlocks PostgreSQL/MySQL/SQLite deployments while keeping Aerospike as the recommended production backend.

## What Changes

- Introduce Go interfaces in `internal/store/` for each currently-Aerospike-backed store: `RegistrationStore`, `StumpStore`, `SubtreeStore`, `CallbackDedupStore`, `CallbackURLRegistry`, `CallbackAccumulatorStore`, `SeenCounterStore`, `SubtreeCounterStore`.
- Move existing Aerospike implementations into `internal/store/aerospike/` behind those interfaces; keep Aerospike as the default backend with no behavioral change.
- Add a SQL implementation under `internal/store/sql/` using `database/sql` with a driver-agnostic schema (PostgreSQL as the first-class target, SQLite for tests). Preserve atomic/idempotent semantics (INSERT … ON CONFLICT, SELECT … FOR UPDATE, transactional counters) and implement a TTL sweeper for expiry.
- Add a `store.backend` config key (`aerospike` | `sql`) plus a `store.sql` block (`driver`, `dsn`, `schema`, `sweeper_interval`). Wiring in `cmd/*/main.go` selects the factory based on config.
- Add schema migrations for the SQL backend (embedded via `go:embed`) and run them on Init.
- Add an end-to-end test under `internal/e2e/` that boots PostgreSQL via testcontainers and exercises block/subtree/callback flows through the SQL backend.
- **BREAKING**: none. Aerospike remains default; SQL is opt-in.

## Capabilities

### New Capabilities
- `store-backend`: Defines the store-layer contract (interfaces for each store), backend selection, lifecycle, and correctness requirements (atomicity, idempotency, TTL semantics) that every implementation must satisfy.
- `sql-store`: Defines the SQL backend implementation requirements: supported drivers, schema migrations, TTL sweeper, connection pooling, and transactional guarantees.

### Modified Capabilities
<!-- None. No requirement-level behavior is changing for existing capabilities; this is an implementation-layer refactor with an additive new backend. -->

## Impact

- **Code**:
  - `internal/store/` — new interfaces; existing `.go` files move under `internal/store/aerospike/` behind a factory.
  - `internal/store/sql/` — new package with SQL implementations, migrations, and sweeper.
  - `cmd/block-processor/main.go`, `cmd/subtree-worker/main.go`, `cmd/callback-delivery/main.go` — swap direct constructor calls for `store.NewFromConfig(cfg)` factory.
  - `internal/config/` — new `store` config block.
  - `internal/e2e/` — new PostgreSQL-backed e2e test.
- **APIs**: None external. Go method signatures on stores are preserved (now satisfied by interfaces).
- **Dependencies**: add `github.com/jackc/pgx/v5` (PostgreSQL driver), `modernc.org/sqlite` (CGO-free SQLite for tests), `github.com/testcontainers/testcontainers-go/modules/postgres`.
- **Config/Deployment**: `config.yaml` gains a `store` block; existing Aerospike keys stay under `aerospike.*` and apply only when `store.backend: aerospike`. Default remains Aerospike — existing deployments work unchanged.
- **Operational**: SQL backend requires operators to provision a database, run migrations (auto-run on start), and monitor the TTL sweeper.
