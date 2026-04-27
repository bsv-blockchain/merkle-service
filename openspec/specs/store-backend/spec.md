# store-backend Specification

## Purpose

Defines the pluggable persistent-store layer that lets the merkle-service select between backend implementations (Aerospike, SQL) at startup without changing service code. Establishes the interface contracts, configuration-driven backend selection, factory/lifecycle, and behavioral equivalence guarantees that every backend must satisfy.

## Requirements

### Requirement: Store Interface Contracts

The system SHALL expose each persistent store as a Go interface in `internal/store/` so that services depend on behavior, not on a specific backend. The interfaces SHALL cover at minimum: `RegistrationStore`, `StumpStore`, `SubtreeStore`, `CallbackDedupStore`, `CallbackURLRegistry`, `CallbackAccumulatorStore`, `SeenCounterStore`, and `SubtreeCounterStore`. Method signatures SHALL preserve the current exported behavior (argument types, return types, error semantics) of the existing Aerospike implementations.

#### Scenario: Services wire stores through interfaces
- **WHEN** `cmd/block-processor/main.go`, `cmd/subtree-worker/main.go`, or `cmd/callback-delivery/main.go` constructs its dependencies
- **THEN** the imports SHALL reference `internal/store` (interfaces) and SHALL NOT reference `internal/store/aerospike` or `internal/store/sql` directly

#### Scenario: Existing method signatures are preserved
- **WHEN** a caller invokes `SeenCounterStore.Increment(txid, subtreeID)` against the new interface
- **THEN** it SHALL return the same `IncrementResult{NewCount, ThresholdReached}` shape as the current concrete Aerospike implementation

### Requirement: Backend Selection via Config

The system SHALL select the store backend at startup based on a `store.backend` configuration value. Supported values SHALL be `aerospike` (default, existing behavior) and `sql` (new backend). Unknown values SHALL cause startup to fail with a clear error. The default value when the `store.backend` key is absent SHALL be `aerospike`, so existing deployments work unchanged.

#### Scenario: Default backend is Aerospike
- **WHEN** `config.yaml` omits the `store.backend` key
- **THEN** the service SHALL initialize Aerospike-backed stores and read the existing `aerospike:` config block

#### Scenario: SQL backend is selected
- **WHEN** `config.yaml` sets `store.backend: sql`
- **THEN** the service SHALL initialize SQL-backed stores and read the `store.sql` config block, and the `aerospike:` block SHALL be ignored

#### Scenario: Unknown backend fails fast
- **WHEN** `config.yaml` sets `store.backend: cassandra`
- **THEN** the service SHALL fail to start with an error naming the invalid value and listing the supported values

### Requirement: Factory and Lifecycle

The system SHALL expose a `store.NewFromConfig(ctx, cfg)` factory that returns a `*Registry` holding every constructed store along with a `Close()` method. The factory SHALL perform any backend-specific initialization (e.g., Aerospike client handshake, SQL schema migrations, SQL TTL sweeper startup) before returning. `Close()` SHALL release all backend resources (connections, goroutines).

#### Scenario: Factory completes initialization synchronously
- **WHEN** `store.NewFromConfig(ctx, cfg)` returns without error
- **THEN** every store in the returned `*Registry` SHALL be usable immediately, and backend-level setup (migrations for SQL, client connection for Aerospike) SHALL have completed

#### Scenario: Close releases resources
- **WHEN** the service invokes `Registry.Close()` during shutdown
- **THEN** open connections SHALL be closed and background goroutines (SQL TTL sweeper) SHALL be stopped before `Close` returns

### Requirement: Behavioral Equivalence Across Backends

Every backend implementation of a store interface SHALL satisfy the same behavioral contract with respect to: idempotency of add/record operations, atomic decrement-and-read semantics, set-membership uniqueness in enumeration, exactly-once threshold firing (for `SeenCounterStore`), and eventual expiry for TTL-bearing records. Backends MAY differ in latency and durability characteristics but SHALL NOT differ in externally observable semantics.

#### Scenario: Idempotent registration
- **WHEN** `RegistrationStore.Add(txid, url)` is called twice with the same arguments against any backend
- **THEN** `RegistrationStore.Get(txid)` SHALL return a list containing `url` exactly once

#### Scenario: Exactly-once seen-threshold firing
- **WHEN** `SeenCounterStore.Increment(txid, subtreeID)` is called enough times to cross the configured threshold against any backend
- **THEN** exactly one of those calls SHALL return `ThresholdReached: true`, regardless of which backend is in use

#### Scenario: Atomic counter decrement
- **WHEN** `SubtreeCounterStore.Decrement(blockHash)` is called concurrently from multiple goroutines against any backend
- **THEN** the set of returned counts SHALL be the exact descending sequence from the initial value, with no duplicates or gaps

#### Scenario: TTL-bearing records expire
- **WHEN** a `CallbackDedupStore` record is written with TTL `t` against any backend, and time `t + sweeper_interval` has elapsed
- **THEN** `CallbackDedupStore.Exists(...)` SHALL return `false` for that record

### Requirement: Aerospike Remains the Primary Backend

The Aerospike backend SHALL remain the default and recommended backend, with no change to its performance characteristics, operational behavior, or required Aerospike version compared to the pre-refactor state. The refactor MUST NOT regress Aerospike-path latency or correctness.

#### Scenario: No Aerospike-path regressions
- **WHEN** the existing Aerospike e2e test suite runs after this change
- **THEN** every pre-existing test SHALL pass without modification to test logic (only imports and store construction may change)
