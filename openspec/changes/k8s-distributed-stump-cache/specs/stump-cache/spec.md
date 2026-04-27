## MODIFIED Requirements

### Requirement: In-process STUMP cache with TTL eviction
The system SHALL provide a STUMP cache conforming to the `StumpCache` interface for encoded STUMP binary data, keyed by `subtreeHash:blockHash`. In `memory` mode, the cache SHALL use an in-process `sync.Map` with background TTL sweep. In `aerospike` mode, the cache SHALL use the `AerospikeStumpCache` implementation. The cache mode SHALL be selected by the `stumpCacheMode` configuration field.

#### Scenario: STUMP stored in cache after building
- **WHEN** a subtree worker builds a STUMP for a subtree in a block
- **THEN** the encoded STUMP binary is stored in the cache via the `StumpCache.Put` interface with the configured TTL

#### Scenario: STUMP retrieved from cache by delivery service
- **WHEN** the delivery service receives a StumpsMessage with a `StumpRef` field
- **THEN** it resolves the STUMP binary from the cache via the `StumpCache.Get` interface

#### Scenario: Cache miss triggers retry
- **WHEN** the delivery service receives a StumpsMessage with a `StumpRef` but the cache returns no entry
- **THEN** the message is re-enqueued for retry using the existing retry mechanism

#### Scenario: Cache entries evicted after TTL
- **WHEN** a STUMP cache entry exceeds the configured TTL (default 5 minutes)
- **THEN** the entry is removed (by background sweep in memory mode, by Aerospike TTL in aerospike mode)

#### Scenario: Concurrent access safety
- **WHEN** multiple delivery workers and a subtree worker access the cache simultaneously
- **THEN** all reads and writes complete without data races or corruption
