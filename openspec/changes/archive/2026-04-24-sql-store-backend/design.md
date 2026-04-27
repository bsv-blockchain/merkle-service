## Context

All eight Aerospike-backed stores in `internal/store/` (registration, stump, subtree, callback-dedup, callback-url-registry, callback-accumulator, seen-counter, subtree-counter) are concrete structs that hold an `*AerospikeClient` directly. They depend on Aerospike-specific primitives that have no direct SQL equivalent: CDT ordered-list operations with `UNIQUE|NOFAIL` flags for set semantics, compound `Operate` calls that append+size+get in one round-trip, record-level TTLs for automatic cleanup, and atomic `AddOp`/`GetOp` counters. The `BlobStore` interface (for stump/subtree payloads) is the only existing abstraction boundary, and it's limited to file/memory.

Services in `cmd/*/main.go` wire stores by calling `store.NewXxxStore(aeroClient, namespace, set)` directly. The e2e test in `internal/e2e/e2e_test.go` assumes a running Aerospike at `localhost:3000` and auto-detects a usable namespace — there is no way to run the suite against a different backend.

Teranode-style daemon lifecycle (Init/Start/Stop/Health) is already observed throughout the codebase; adding a backend selector fits naturally into the existing `Init` phase of each service.

## Goals / Non-Goals

**Goals:**
- Define a stable Go interface for every store that captures the semantic contract (idempotent add, atomic counter decrement, set-membership enumeration, TTL-expiring marker, etc.) without leaking Aerospike-isms.
- Add a PostgreSQL-primary SQL backend that satisfies those contracts with equivalent semantics, including atomicity and expiry.
- Keep Aerospike as the default; preserve all current performance characteristics for the default path.
- Make backend selection a runtime config switch: `store.backend: aerospike | sql` — no code changes to flip.
- Provide an e2e test that boots PostgreSQL via testcontainers and runs the same flow the Aerospike e2e test runs.

**Non-Goals:**
- Supporting data migration between backends. Deployments pick one backend and stay on it; cross-backend migration is a future change.
- Multi-backend at runtime (e.g., stumps in SQL, registrations in Aerospike).
- Sharding or read-replica topology for SQL. Single primary is sufficient for the initial scope.
- Changing the external behavior of any service (HTTP API, Kafka topics, libp2p messages) — this is an implementation-layer refactor.
- Replacing `BlobStore` (file/memory). Stump and subtree payload storage continues to delegate to `BlobStore`; only their Aerospike-backed index/metadata paths move behind interfaces.

## Decisions

### 1. One interface per store, not a single mega-interface

Each store becomes its own Go interface (`RegistrationStore`, `SubtreeCounterStore`, etc.) with methods matching today's concrete signatures. Services depend on the interfaces they use, nothing more.

*Alternative considered*: A single `Store` interface aggregating every method. Rejected — it would force every backend to implement every method even if a service doesn't use it, and it would be a large surface for mocking in tests. Separate interfaces follow Go's interface-segregation norm and match how services are wired today (each service constructs only the stores it needs).

### 2. Package layout: `internal/store/` (interfaces) + `internal/store/aerospike/` + `internal/store/sql/`

Interfaces, shared types (e.g., `IncrementResult`), and a `NewFromConfig(cfg) (*Registry, error)` factory live in `internal/store/`. Concrete backends live in subpackages. The `BlobStore` interface and its file/memory implementations stay in `internal/store/` unchanged.

*Alternative considered*: Flat package with `aerospike_*.go` and `sql_*.go` prefixes. Rejected — each backend has a dozen files, and subpackages give us a clean boundary for vendor-specific imports (the Aerospike Go client is heavyweight; SQL drivers pull build tags for CGO-free SQLite).

### 3. SQL semantic mapping

Each Aerospike primitive maps to a SQL pattern:

| Aerospike primitive | SQL pattern |
|---|---|
| CDT ordered list with `UNIQUE\|NOFAIL` | `INSERT … ON CONFLICT DO NOTHING` into a child table with `(parent_key, value)` UNIQUE |
| Compound append+size+get | Single transaction: `INSERT ON CONFLICT`, then `SELECT COUNT(*)` and `SELECT fired_flag FOR UPDATE` |
| Atomic `AddOp(-1)` + `GetOp` | `UPDATE … SET n = n - 1 RETURNING n` (PostgreSQL `RETURNING`; emulated via `UPDATE` + `SELECT` in a transaction on drivers without RETURNING) |
| Record TTL + nsup | `expires_at TIMESTAMPTZ` column + background sweeper goroutine running `DELETE … WHERE expires_at < now()` every `store.sql.sweeper_interval` (default 60s) |
| Single-record CDT list broadcast (`CallbackURLRegistry`) | `SELECT url FROM callback_urls ORDER BY url` — one row per URL; atomicity via `INSERT ON CONFLICT` |

*Alternative considered*: pg-specific features like `INTERVAL` + `pg_cron` for TTL. Rejected — we want SQLite to work for unit tests without extensions, so TTL must live in Go.

### 4. PostgreSQL as primary SQL target; SQLite for tests

Use `github.com/jackc/pgx/v5/stdlib` as the `database/sql` driver for PostgreSQL (production) and `modernc.org/sqlite` (CGO-free) for unit tests of the SQL layer. The schema uses a conservative subset: `BIGINT`, `BYTEA`/`BLOB`, `TEXT`, `TIMESTAMPTZ`/`DATETIME`. Migrations are parameterized with a small driver dialect (`bytea` vs `blob`, `TIMESTAMPTZ` vs `DATETIME`).

*Alternative considered*: `pgx` native (no `database/sql`). Rejected — `database/sql` lets SQLite share the same implementation for tests, avoiding two codepaths.

*Alternative considered*: MySQL. Deferred — not in initial scope, but the dialect abstraction leaves room to add it without reshaping the store package.

### 5. Migrations embedded via `go:embed`, run on Init

A `migrations/` directory under `internal/store/sql/` holds `0001_init.sql`, `0002_…`, etc. The SQL backend runs migrations idempotently on `Init` using a simple `schema_migrations(version INT PRIMARY KEY, applied_at)` table. No external migration tool is required.

*Alternative considered*: `golang-migrate/migrate`. Rejected for the initial change — adds a dependency and CLI surface; a 60-line in-repo runner covers our needs. We can swap later if migrations get complex.

### 6. TTL sweeper is per-process, not per-store

A single `sweeper` goroutine, started in `store.NewFromConfig` for the SQL backend, runs one `DELETE WHERE expires_at < now()` per TTL-bearing table on a ticker. This avoids N goroutines and matches Aerospike's single nsup thread model. The sweeper logs deletion counts at debug level for observability.

*Alternative considered*: Lazy expiry on read (check `expires_at` in every query). Rejected — leaves tombstones indefinitely for keys that are never read again (e.g., `callback_dedup` entries for URLs that stop firing), which grows unboundedly.

### 7. Config shape

```yaml
store:
  backend: aerospike   # or: sql
  sql:
    driver: postgres   # or: sqlite
    dsn: "postgres://user:pass@host:5432/merkle?sslmode=disable"
    schema: merkle     # optional; postgres only
    sweeper_interval: 60s
    max_open_conns: 25
    max_idle_conns: 5
```

Existing `aerospike:` block is unchanged. When `store.backend: sql`, the `aerospike:` block is ignored; when `store.backend: aerospike` (default), the `store.sql` block is ignored. `NewFromConfig` validates that exactly one backend's config block is well-formed.

### 8. E2E test boots PostgreSQL via testcontainers

A new `internal/e2e/postgres_e2e_test.go` uses `github.com/testcontainers/testcontainers-go/modules/postgres` to start a transient PostgreSQL container, point the service at it via `store.backend: sql`, and exercise the same block/subtree/callback flow as the Aerospike e2e test. Test is gated behind a build tag `e2e_postgres` so `go test ./...` on a laptop without Docker doesn't pay the cost; CI runs with `-tags=e2e_postgres`.

*Alternative considered*: Spin up PG with `docker-compose.yml` like Aerospike. Rejected — testcontainers gives per-test isolation and cleans up on failure, which is a nicer developer loop.

## Risks / Trade-offs

- **Risk**: SQL `INSERT ON CONFLICT` + transactional counters are measurably slower than Aerospike's single-RTT CDT ops for write-heavy paths (seen-counter, registration). → **Mitigation**: Keep Aerospike as default, document SQL as "works, scales lower". Add benchmarks in `internal/store/sql/` to track per-op latency against a baseline. Connection pooling + prepared statements mitigate the worst of the overhead.

- **Risk**: TTL sweeper deletes rows that a concurrent transaction is about to touch, causing subtle races. → **Mitigation**: All TTL-table writes include `ON CONFLICT (…) DO UPDATE SET expires_at = EXCLUDED.expires_at`, so a sweeper deletion followed by a re-insert is harmless. Sweeper batches deletes in `LIMIT 1000` chunks to keep locks short.

- **Risk**: Interface extraction is mechanical but touches every store + every service. Missing one site silently keeps the concrete Aerospike dependency. → **Mitigation**: In tasks.md, land the interface + Aerospike-implementation-moved PR first (no behavior change), then the SQL PR on top. Add a lint rule (`grep`) ensuring `cmd/*/main.go` imports only `internal/store`, not `internal/store/aerospike`.

- **Risk**: Migrations run on every service start; if block-processor and subtree-worker start simultaneously against an empty database, both attempt migration. → **Mitigation**: Wrap migration in `SELECT pg_advisory_lock(hashtext('merkle_migrate'))` on PostgreSQL; on SQLite, use `BEGIN IMMEDIATE` which serializes writers. Already-applied migrations are no-ops.

- **Risk**: Dialect abstraction is a mini-ORM in disguise; easy to grow. → **Mitigation**: Limit dialect to data-type substitution (`bytea`↔`blob`, timestamp type). All query strings stay hand-written. If we need more, it's a signal to adopt `sqlc` rather than expand the hand-rolled layer.

- **Trade-off**: We are adding ~1500 lines of SQL code plus migrations for a feature most users won't use. The payoff is removing a hard Aerospike dependency for new adopters and making our test suite runnable without a running Aerospike cluster.
