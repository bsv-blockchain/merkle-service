## ADDED Requirements

### Requirement: Transaction registration store
The system SHALL store transaction registrations in Aerospike as a mapping from txid (key) to a set of callback URLs (CDT list/set bin). The store SHALL support adding, reading, and TTL-updating registrations.

> Traces to: requirements.md — "database: txid: callback_url, basic key-value store that scales really well" and "api-server: stores in aerospike"

#### Scenario: Store registration with CDT set
- **WHEN** a new registration is stored for a txid
- **THEN** the system writes to Aerospike using CDT set operations to maintain a unique set of callback URLs per txid

#### Scenario: Read registrations for txid
- **WHEN** the system queries registrations for a txid
- **THEN** the system returns the full set of callback URLs associated with that txid

#### Scenario: Batch read registrations
- **WHEN** the system needs to check registrations for thousands of txids (e.g., all txids in a subtree)
- **THEN** the system uses Aerospike batch read operations to fetch all registrations in a single round-trip

### Requirement: Seen-counter store
The system SHALL maintain an atomic counter per txid in Aerospike that tracks how many times the transaction has been seen across subtrees. The counter SHALL be incremented atomically using Aerospike's operate command.

> Traces to: requirements.md — "txmetacache is useful for this (could use counter with threshold with new status SEEN_MULTIPLE_NODES etc)"

#### Scenario: Increment seen counter
- **WHEN** a registered transaction is seen in a subtree
- **THEN** the system atomically increments the seen counter for that txid and returns the new value

#### Scenario: Counter supports configurable threshold
- **WHEN** the seen counter reaches the configured threshold value
- **THEN** the system detects this from the returned counter value and triggers SEEN_MULTIPLE_NODES logic

### Requirement: Subtree store via Teranode blob.Store with delete-at-height
The system SHALL store subtree data using Teranode's `stores/blob` package (`blob.Store` interface), reusing the same pattern as Teranode's `services/subtreevalidation`. Subtrees SHALL be stored by subtree hash key with `SUBTREESTORE` file type. Expiry SHALL use delete-at-height (DAH) — automatic deletion at a configured block height offset — rather than wall-clock TTL.

> Traces to: requirements.md — "subtreeprocessor: storing subtrees to subtreestore with 1 block TTL" and Teranode's subtreevalidation service pattern

#### Scenario: Store subtree with DAH
- **WHEN** a subtree is received and stored
- **THEN** the system writes the subtree to blob.Store via `Set` or `SetFromReader` with a delete-at-height option set to current block height + configured offset

#### Scenario: Retrieve subtree for block processing
- **WHEN** the block processor requests a subtree by ID
- **THEN** the system retrieves the subtree data via `blob.Store.Get` or `GetIoReader` if the DAH has not been reached

#### Scenario: Subtree expires after block height
- **WHEN** the chain advances past the subtree's delete-at-height
- **THEN** the blob.Store automatically prunes the subtree record via its DAH mechanism

#### Scenario: Streaming I/O for large subtrees
- **WHEN** a subtree is large (thousands of transactions)
- **THEN** the system uses `SetFromReader` and `GetIoReader` to avoid loading the entire blob into memory

#### Scenario: Concurrent subtree access
- **WHEN** multiple goroutines request the same subtree simultaneously (during block processing)
- **THEN** the `ConcurrentBlob` wrapper ensures only one fetch occurs and others wait for the result

### Requirement: Batched Aerospike operations
All Aerospike operations that process multiple records (registration checks, TTL updates, subtree lookups) SHALL use Aerospike batch operations to minimize round-trips and maximize throughput.

> Traces to: requirements.md — "all aerospike calls need to be batched" and reference to Teranode blockassembler batching pattern

#### Scenario: Batch registration check
- **WHEN** the subtree processor checks registrations for all txids in a subtree
- **THEN** the system uses a single Aerospike batch read call, not individual get operations

#### Scenario: Batch TTL update
- **WHEN** the block processor updates TTLs for all registered txids in a block
- **THEN** the system uses Aerospike batch operate to update all TTLs in a single call

### Requirement: Aerospike retry policy
The system SHALL configure Aerospike client retry policies for transient failures. Retries SHALL use backoff to avoid overwhelming the cluster during degraded conditions.

> Traces to: requirements.md — "Support scale where blocks contain millions of transactions"

#### Scenario: Transient Aerospike failure
- **WHEN** an Aerospike operation fails with a transient error (timeout, cluster rebalancing)
- **THEN** the system retries the operation with configurable backoff

#### Scenario: Permanent Aerospike failure
- **WHEN** an Aerospike operation fails with a non-transient error
- **THEN** the system logs the error and propagates the failure to the caller

### Requirement: Post-mine TTL update
The system SHALL update the TTL of registered transactions to 30 minutes after their block has been processed. This allows for fork/orphan handling while ensuring records are eventually cleaned up.

> Traces to: requirements.md — "update TTL of all transactions to 30 minutes to allow for forks/orphans but not keep it forever"

#### Scenario: TTL updated to 30 minutes after mining
- **WHEN** a block containing a registered transaction has been fully processed
- **THEN** the system updates the txid registration record's TTL to 30 minutes

#### Scenario: Registration expires after TTL
- **WHEN** 30 minutes have elapsed since the block was processed
- **THEN** Aerospike automatically evicts the registration record
