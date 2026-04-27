## ADDED Requirements

### Requirement: Supported SQL Drivers

The SQL backend SHALL support PostgreSQL as the primary production driver and SQLite as a secondary driver for tests and lightweight deployments. Driver selection SHALL be controlled by `store.sql.driver` (`postgres` | `sqlite`). The implementation SHALL use `database/sql` so that additional drivers can be added without reshaping the store package.

#### Scenario: PostgreSQL driver initializes successfully
- **WHEN** `store.sql.driver: postgres` is configured with a valid DSN
- **THEN** the factory SHALL open a connection using `github.com/jackc/pgx/v5/stdlib`, verify connectivity with a ping, and apply all pending migrations before returning

#### Scenario: SQLite driver initializes successfully
- **WHEN** `store.sql.driver: sqlite` is configured with a file-path or `:memory:` DSN
- **THEN** the factory SHALL open a connection using `modernc.org/sqlite` (no CGO required), apply migrations, and return a fully-initialized registry

#### Scenario: Unknown driver fails fast
- **WHEN** `store.sql.driver` is set to a value other than `postgres` or `sqlite`
- **THEN** the factory SHALL return an error naming the unknown driver and listing supported drivers

### Requirement: Embedded Schema Migrations

The SQL backend SHALL bundle its schema migrations via `go:embed` under `internal/store/sql/migrations/` as numbered `.sql` files (e.g., `0001_init.sql`). On factory initialization, it SHALL apply any unapplied migrations in ascending order, recording applied versions in a `schema_migrations` table. Migration application SHALL be idempotent and safe to run from multiple processes starting concurrently.

#### Scenario: Fresh database receives all migrations
- **WHEN** the factory initializes against a database with no `schema_migrations` table
- **THEN** it SHALL create `schema_migrations` and apply every migration in the embedded set in version order

#### Scenario: Partially migrated database applies only pending migrations
- **WHEN** the factory initializes against a database where `schema_migrations` contains versions 1 and 2, and migrations 1, 2, and 3 are embedded
- **THEN** only migration 3 SHALL be executed, and `schema_migrations` SHALL contain versions 1, 2, and 3 after initialization

#### Scenario: Concurrent startup is safe
- **WHEN** two service processes start simultaneously against an un-migrated database
- **THEN** exactly one process SHALL apply each migration (serialized via `pg_advisory_lock` on PostgreSQL or `BEGIN IMMEDIATE` on SQLite), and neither process SHALL observe a partially applied schema

### Requirement: Atomic Semantics via Transactions

The SQL backend SHALL implement every compound store operation (append + size + flag-check, decrement + read, read-and-delete) inside a single transaction with isolation level sufficient to prevent lost updates. For PostgreSQL, this SHALL be `READ COMMITTED` with `SELECT … FOR UPDATE` on contended rows, or `REPEATABLE READ` where required. The backend SHALL use `INSERT … ON CONFLICT DO NOTHING` / `DO UPDATE` for idempotent writes.

#### Scenario: Seen-counter threshold fires exactly once under concurrency
- **WHEN** multiple goroutines concurrently call `SeenCounterStore.Increment(txid, distinctSubtreeID)` crossing the threshold
- **THEN** exactly one call SHALL observe `ThresholdReached: true`, and subsequent calls SHALL observe `ThresholdReached: false`

#### Scenario: Subtree-counter decrement is atomic
- **WHEN** `SubtreeCounterStore.Decrement(blockHash)` is invoked from N concurrent goroutines
- **THEN** the N returned values SHALL form a strictly decreasing sequence with no duplicates or skipped integers

#### Scenario: Duplicate registration is silently deduplicated
- **WHEN** `RegistrationStore.Add(txid, url)` is called twice with identical arguments
- **THEN** the second call SHALL not return an error, and `Get(txid)` SHALL return `[url]` (length 1)

### Requirement: TTL Sweeper for Expiry

The SQL backend SHALL emulate Aerospike record TTL using an `expires_at TIMESTAMPTZ` (or SQLite `DATETIME`) column on every TTL-bearing table, plus a single background goroutine that periodically deletes expired rows. The sweeper interval SHALL be configurable via `store.sql.sweeper_interval` (default 60s). The sweeper SHALL run `DELETE … WHERE expires_at < now()` in bounded-size batches and SHALL log deletion counts at debug level. The sweeper SHALL stop cleanly when `Registry.Close()` is called.

#### Scenario: Expired rows are deleted on the next sweep
- **WHEN** a row has `expires_at` set to a time earlier than now, and one sweeper interval elapses
- **THEN** that row SHALL no longer be returned by any store-level read (e.g., `CallbackDedupStore.Exists` returns false)

#### Scenario: Sweeper stops on Close
- **WHEN** `Registry.Close()` is invoked
- **THEN** the sweeper goroutine SHALL exit within one sweeper interval, and no further `DELETE` statements SHALL be issued after `Close` returns

#### Scenario: Sweeper uses bounded batches
- **WHEN** 10,000 rows are expired at the same time
- **THEN** the sweeper SHALL delete them in multiple statements each limited to at most 1,000 rows per statement, avoiding long-held table locks

### Requirement: Connection Pooling

The SQL backend SHALL honor `store.sql.max_open_conns` and `store.sql.max_idle_conns` by calling `db.SetMaxOpenConns` and `db.SetMaxIdleConns` on the underlying `*sql.DB`. Defaults SHALL be 25 open / 5 idle, suitable for a small-to-medium deployment.

#### Scenario: Pool limits apply
- **WHEN** `store.sql.max_open_conns: 10` and `store.sql.max_idle_conns: 2` are set
- **THEN** the underlying `*sql.DB` SHALL be configured with those limits after factory return

### Requirement: PostgreSQL-backed E2E Test

The repository SHALL include an end-to-end test that boots PostgreSQL via testcontainers, configures the service with `store.backend: sql` and `store.sql.driver: postgres`, and exercises the core block/subtree/callback flow. The test SHALL be gated behind the `e2e_postgres` Go build tag so `go test ./...` without the tag does not require Docker. CI SHALL run this test with `-tags=e2e_postgres`.

#### Scenario: PostgreSQL e2e test covers the happy path
- **WHEN** `go test -tags=e2e_postgres ./internal/e2e/...` is run on a host with Docker available
- **THEN** the test SHALL start a PostgreSQL container, apply migrations, push a block and its subtrees through the pipeline, receive a callback for every registered txid, and tear down the container on exit

#### Scenario: Test is skipped without the build tag
- **WHEN** `go test ./...` is run without `-tags=e2e_postgres`
- **THEN** the PostgreSQL e2e test SHALL NOT be compiled into the test binary, and `go test` SHALL NOT require Docker to pass
