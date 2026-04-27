## ADDED Requirements

### Requirement: Consume subtree messages from Kafka
The system SHALL consume messages from the Kafka `subtree` topic using a consumer group. Each subtree message SHALL be processed exactly once within the consumer group.

> Traces to: requirements.md — "subtreeprocessor: subscribes to kafka subtree topic"

#### Scenario: Consume subtree message
- **WHEN** a subtree message is available on the Kafka `subtree` topic
- **THEN** the system consumes the message and extracts the subtree ID, transaction list, and merkle data

### Requirement: Store subtrees in blob.Store
The system SHALL store received subtrees using Teranode's `stores/blob` package (`blob.Store` interface), reusing the same pattern as Teranode's `services/subtreevalidation`. Subtrees SHALL be stored with delete-at-height (DAH) for blockchain-height-based expiry. Subtree storage SHALL be configurable to occur in the subtree processor (real-time) or deferred to the block processor.

> Traces to: requirements.md — "subtreeprocessor: storing subtrees to subtreestore with 1 block TTL" and "make behavior of subtree storing configurable within subtreeprocessor vs blockprocessor"

#### Scenario: Store subtree with DAH
- **WHEN** a subtree message is consumed
- **THEN** the system stores the subtree data in blob.Store via `Set` or `SetFromReader` with a delete-at-height set to current block height + configured offset
- **THEN** the subtree is retrievable by subtree ID for subsequent block processing

#### Scenario: Configurable storage timing
- **WHEN** the subtree storage mode is configured as "realtime"
- **THEN** subtrees are stored immediately upon consumption in the subtree processor
- **WHEN** the subtree storage mode is configured as "deferred"
- **THEN** subtrees are stored during block processing instead

### Requirement: Check transaction registrations
The system SHALL check all transaction IDs in a subtree against the registration store using batched Aerospike lookups. Only registered transactions trigger further processing.

> Traces to: requirements.md — "subtreeprocessor: checking if transaction is registered"

#### Scenario: Registered transaction found in subtree
- **WHEN** a subtree contains a transaction ID that has registered callback URLs
- **THEN** the system identifies the transaction as registered and proceeds to emit a SEEN_ON_NETWORK notification

#### Scenario: No registered transactions in subtree
- **WHEN** a subtree contains no transaction IDs that have registered callback URLs
- **THEN** the system completes processing of the subtree without emitting any callbacks

#### Scenario: Batched registration checks
- **WHEN** a subtree contains thousands of transaction IDs
- **THEN** the system checks registrations using batched Aerospike read operations (not individual lookups)

### Requirement: Emit SEEN_ON_NETWORK callback
The system SHALL emit a SEEN_ON_NETWORK notification to the Kafka `stumps` topic for each registered transaction found in a subtree. The notification SHALL include the txid and callback URLs.

> Traces to: requirements.md — "subtreeprocessor: if so hit callback with SEEN_ON_NETWORK"

#### Scenario: SEEN_ON_NETWORK for registered transaction
- **WHEN** a registered transaction is found in a subtree for the first time
- **THEN** the system publishes a SEEN_ON_NETWORK message to Kafka with the txid, callback URLs, and subtree reference

### Requirement: Track seen-count per transaction
The system SHALL maintain an atomic counter per txid in Aerospike that increments each time the transaction is seen in a subtree. When the counter reaches a configurable threshold, the system SHALL emit a SEEN_MULTIPLE_NODES status.

> Traces to: requirements.md — "txmetacache is useful for this (could use counter with threshold with new status SEEN_MULTIPLE_NODES etc)"

#### Scenario: First time seen
- **WHEN** a registered transaction is first seen in a subtree
- **THEN** the system increments the seen counter for that txid to 1
- **THEN** the system emits SEEN_ON_NETWORK

#### Scenario: Seen-count reaches threshold
- **WHEN** a registered transaction's seen-count reaches the configured threshold (e.g., 3)
- **THEN** the system emits a SEEN_MULTIPLE_NODES notification to the Kafka `stumps` topic

#### Scenario: Seen-count exceeds threshold
- **WHEN** a registered transaction's seen-count exceeds the configured threshold
- **THEN** the system does not emit additional SEEN_MULTIPLE_NODES notifications (deduplication)

### Requirement: Deduplicate registration checks with Teranode txmetacache
The system SHALL use Teranode's `stores/txmetacache` package (`github.com/bsv-blockchain/teranode/stores/txmetacache`) to avoid redundant Aerospike lookups for transaction IDs that have already been checked recently. The txmetacache provides off-heap mmap'd ring buffer storage with xxhash-based sharding, generation-based expiration, and batch operations for high-throughput deduplication. This reduces Aerospike load during periods of high subtree throughput.

> Traces to: requirements.md — "implements some mechanism to deduplicate requests to aerospike in future" and "txmetacache is useful for this"

#### Scenario: Cache hit avoids Aerospike lookup
- **WHEN** a transaction ID was recently checked and the result is cached in txmetacache via `GetMetaCached`
- **THEN** the system uses the cached registration result without querying Aerospike

#### Scenario: Cache miss triggers Aerospike lookup
- **WHEN** a transaction ID is not found in the txmetacache
- **THEN** the system queries Aerospike for the registration and populates the txmetacache with the result via `SetCache` or `SetCacheMulti`

#### Scenario: Batch deduplication check
- **WHEN** a subtree contains thousands of transaction IDs
- **THEN** the system uses txmetacache batch operations to check all txids against the cache before falling back to batched Aerospike lookups for cache misses
