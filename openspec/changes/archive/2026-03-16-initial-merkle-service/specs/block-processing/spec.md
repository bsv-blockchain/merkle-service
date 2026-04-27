## ADDED Requirements

### Requirement: Consume block messages from Kafka
The system SHALL consume messages from the Kafka `block` topic using a consumer group. Each block message contains a block hash, block header, and references to all subtrees in the block.

> Traces to: requirements.md — "blockprocessor: fire off multiple blocksubtreeprocessors in parallel to process each subtree"

#### Scenario: Consume block message
- **WHEN** a block message is available on the Kafka `block` topic
- **THEN** the system consumes the message and extracts the block hash, header, and subtree reference list

### Requirement: Parallel subtree processing per block
The system SHALL process all subtrees within a block in parallel using a bounded goroutine worker pool. Each subtree is processed by a block-subtree processor.

> Traces to: requirements.md — "blockprocessor: fire off multiple blocksubtreeprocessors in parallel to process each subtree" and "blocksubtreeprocessor: partitions/parallelizes subtree processing"

#### Scenario: Parallel processing of block subtrees
- **WHEN** a block message references N subtrees
- **THEN** the system dispatches N block-subtree processing tasks to the worker pool
- **THEN** the system waits for all N tasks to complete before finalizing the block

#### Scenario: Worker pool bounds concurrency
- **WHEN** a block contains more subtrees than the configured worker pool size
- **THEN** the system processes subtrees up to the pool limit concurrently, queuing the rest
- **THEN** all subtrees are eventually processed

### Requirement: Retrieve subtrees from blob.Store
The block-subtree processor SHALL retrieve the full subtree data from the blob.Store (Teranode `stores/blob` package) for STUMP construction. The `ConcurrentBlob` wrapper SHALL be used to prevent duplicate fetches when multiple goroutines request the same subtree.

> Traces to: requirements.md — "blocksubtreeprocessor: get all txids from subtree"

#### Scenario: Subtree found in store
- **WHEN** the block-subtree processor requests a subtree by ID via `blob.Store.Get` or `GetIoReader`
- **THEN** the system retrieves the subtree data including all transaction IDs and merkle tree structure

#### Scenario: Subtree not found in store
- **WHEN** the block-subtree processor requests a subtree that has been pruned by DAH or is missing
- **THEN** the system logs an error and skips STUMP construction for that subtree

#### Scenario: Concurrent subtree retrieval during block processing
- **WHEN** multiple block-subtree processor goroutines request the same subtree simultaneously
- **THEN** the `ConcurrentBlob` wrapper ensures only one actual fetch occurs and other goroutines wait for the result

### Requirement: Check registrations for block subtree transactions
The block-subtree processor SHALL check all transaction IDs in the subtree against the registration store using batched Aerospike operations to identify transactions with registered callback URLs.

> Traces to: requirements.md — "blocksubtreeprocessor: check callback_urls for all txs that have been registered"

#### Scenario: Registered transactions found
- **WHEN** the subtree contains registered transactions
- **THEN** the system collects the set of callback URLs for each registered transaction

### Requirement: Construct STUMP per callback URL
The system SHALL construct a STUMP (Subtree Unified Merkle Path) in BRC-0074 BUMP format for each unique callback URL. The STUMP SHALL contain the merkle paths for all of that callback's registered transactions within the subtree.

> Traces to: requirements.md — "blocksubtreeprocessor: Build stump per callback URL" and terms section defining STUMP as "same format as BUMP but is a merkle path for a transaction in a given subtree"

#### Scenario: Build STUMP for single callback
- **WHEN** a callback URL has one registered transaction in the subtree
- **THEN** the system constructs a STUMP containing the merkle path from that transaction to the subtree root in BRC-0074 format

#### Scenario: Build STUMP for multiple transactions
- **WHEN** a callback URL has multiple registered transactions in the same subtree
- **THEN** the system constructs a single STUMP containing the merged merkle paths for all those transactions

### Requirement: Publish STUMPs to Kafka
The system SHALL publish constructed STUMPs to the Kafka `stumps` topic, partitioned by callback URL hash for ordered delivery per callback recipient.

> Traces to: requirements.md — "blocksubtreeprocessor: Push stump to kafka"

#### Scenario: Publish STUMP message
- **WHEN** a STUMP is constructed for a callback URL
- **THEN** the system publishes a message to the Kafka `stumps` topic containing the STUMP data, callback URL, block hash, and subtree ID
- **THEN** the message is partitioned by a hash of the callback URL

### Requirement: Update registration TTL after mining
The system SHALL update the TTL of all registered transactions in a processed block to 30 minutes, allowing time for fork/orphan handling without retaining data indefinitely.

> Traces to: requirements.md — "blocksubtreeprocessor: update TTL of all transactions to 30 minutes to allow for forks/orphans but not keep it forever"

#### Scenario: TTL update after block processing
- **WHEN** a block has been fully processed and all STUMPs published
- **THEN** the system updates the Aerospike TTL for all registered txids in the block to 30 minutes using batched Aerospike operations

### Requirement: Clean up subtree store after block processing
The system SHALL remove subtree data from the blob.Store after a block has been fully processed, complementing the DAH-based automatic expiry with explicit cleanup.

> Traces to: requirements.md — "blockprocessor: after all blocksubtreeprocessors are complete, clear out subtreestore"

#### Scenario: Subtree cleanup after block completion
- **WHEN** all block-subtree processors for a block have completed
- **THEN** the system calls `blob.Store.Del` for all subtrees referenced by that block for immediate cleanup
- **THEN** if explicit deletion fails, DAH-based pruning serves as a fallback to prevent stale data accumulation

### Requirement: Handle coinbase placeholder
The block-subtree processor SHALL handle subtree 0 which contains the coinbase placeholder. It SHALL NOT perform coinbase transaction replacement; this is Arcade's responsibility.

> Traces to: requirements.md — "blocksubtreeprocessor: this subtree processor receives subtree 0 with the coinbase placeholder; does not do coinbase transaction replacement"

#### Scenario: Process subtree 0 with coinbase placeholder
- **WHEN** the block-subtree processor processes subtree 0
- **THEN** the system treats the coinbase placeholder as an opaque entry
- **THEN** the system constructs STUMPs for registered transactions in subtree 0 without modifying the coinbase entry
