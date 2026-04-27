## ADDED Requirements

### Requirement: In-process STUMP cache with TTL eviction
The system SHALL provide an in-process cache for encoded STUMP binary data, keyed by `subtreeHash:blockHash`. The cache SHALL support concurrent read/write access and automatically evict entries after a configurable TTL.

#### Scenario: STUMP stored in cache after building
- **WHEN** a subtree worker builds a STUMP for a subtree in a block
- **THEN** the encoded STUMP binary is stored in the cache with key `subtreeHash:blockHash` and the configured TTL

#### Scenario: STUMP retrieved from cache by delivery service
- **WHEN** the delivery service receives a StumpsMessage with a `StumpRef` field
- **THEN** it resolves the STUMP binary from the cache using the reference key

#### Scenario: Cache miss triggers retry
- **WHEN** the delivery service receives a StumpsMessage with a `StumpRef` but the cache entry has expired or is missing
- **THEN** the message is re-enqueued for retry using the existing retry mechanism

#### Scenario: Cache entries evicted after TTL
- **WHEN** a STUMP cache entry exceeds the configured TTL (default 5 minutes)
- **THEN** the entry is removed from the cache by the background sweep goroutine

#### Scenario: Concurrent access safety
- **WHEN** multiple delivery workers and a subtree worker access the cache simultaneously
- **THEN** all reads and writes complete without data races or corruption

### Requirement: StumpsMessage reference field
The StumpsMessage struct SHALL support a `StumpRef` field as an alternative to inline `StumpData`. When `StumpRef` is set, the delivery service SHALL resolve the STUMP from the cache. When `StumpData` is set, it SHALL be used directly for backward compatibility.

#### Scenario: Message with StumpRef (new format)
- **WHEN** a StumpsMessage has `StumpRef` set and `StumpData` empty
- **THEN** the delivery service resolves the STUMP from the cache before HTTP delivery

#### Scenario: Message with inline StumpData (backward compat)
- **WHEN** a StumpsMessage has `StumpData` set (regardless of `StumpRef`)
- **THEN** the delivery service uses `StumpData` directly without cache lookup

#### Scenario: Kafka message size reduction
- **WHEN** the block processor publishes MINED messages using StumpRef instead of inline StumpData
- **THEN** each Kafka message is approximately 100× smaller (~3.6 KB vs ~365 KB)
