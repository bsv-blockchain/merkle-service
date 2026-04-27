## ADDED Requirements

### Requirement: Aerospike-backed STUMP cache
The system SHALL provide an Aerospike-backed STUMP cache implementation that stores encoded STUMP binary data in a dedicated Aerospike set with TTL-based eviction. The cache SHALL be accessible from any pod in the Kubernetes cluster.

#### Scenario: Subtree worker writes STUMP to Aerospike
- **WHEN** a subtree worker builds a STUMP for a subtree in a block
- **THEN** it stores the STUMP binary in the Aerospike `stump_cache` set with key `subtreeHash:blockHash` and the configured TTL

#### Scenario: Delivery service reads STUMP from Aerospike
- **WHEN** a delivery service in a different pod receives a StumpsMessage with `StumpRef`
- **THEN** it reads the STUMP binary from Aerospike using the reference key

#### Scenario: TTL-based eviction
- **WHEN** a STUMP cache entry exceeds the configured TTL (default 300 seconds)
- **THEN** Aerospike automatically evicts the entry

#### Scenario: Cache miss on expired entry
- **WHEN** a delivery service reads a StumpRef whose Aerospike entry has expired
- **THEN** the message is re-enqueued for retry

### Requirement: Local LRU read-through cache
The Aerospike STUMP cache SHALL include a local in-process LRU cache to reduce Aerospike read amplification. Multiple delivery workers resolving the same StumpRef SHALL read from Aerospike at most once per pod.

#### Scenario: First read populates local LRU
- **WHEN** the first delivery worker in a pod resolves a StumpRef not in the local LRU
- **THEN** the STUMP is fetched from Aerospike and stored in the local LRU cache

#### Scenario: Subsequent reads served from LRU
- **WHEN** additional delivery workers in the same pod resolve the same StumpRef
- **THEN** the STUMP is served from the local LRU without an Aerospike read

#### Scenario: LRU eviction by capacity
- **WHEN** the local LRU cache reaches its configured capacity (default 1024 entries)
- **THEN** the least recently used entry is evicted to make room for new entries

### Requirement: StumpCache interface with pluggable backends
The system SHALL define a `StumpCache` interface with `Put`, `Get`, and `Close` methods. Two implementations SHALL be provided: `MemoryStumpCache` for all-in-one/dev mode and `AerospikeStumpCache` for Kubernetes/production mode. The implementation SHALL be selected by configuration.

#### Scenario: Memory backend selected in all-in-one mode
- **WHEN** `stumpCacheMode` is set to `memory`
- **THEN** the system uses the in-process `sync.Map`-backed cache

#### Scenario: Aerospike backend selected in Kubernetes mode
- **WHEN** `stumpCacheMode` is set to `aerospike`
- **THEN** the system uses the Aerospike-backed cache with local LRU

#### Scenario: Default cache mode
- **WHEN** no `stumpCacheMode` is configured
- **THEN** the system defaults to `memory` mode
