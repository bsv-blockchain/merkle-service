## ADDED Requirements

### Requirement: Fetch block metadata on announcement
The block processor SHALL fetch block metadata from the DataHub URL in the announcement message to obtain the list of subtree hashes for the block.

#### Scenario: Block announced and metadata fetched
- **WHEN** a block announcement is received with a DataHubURL
- **THEN** the processor fetches block metadata including the ordered list of subtree hashes

#### Scenario: Block metadata fetch fails
- **WHEN** a block announcement is received but the DataHub fetch fails after retries
- **THEN** the processor logs an error and skips processing the block

### Requirement: Process each subtree in block with worker pool
The block processor SHALL spawn a bounded worker pool to process each subtree in the block. Each worker retrieves the subtree data, finds registered txids, builds a STUMP, and emits `STUMP` callbacks. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `STUMP`, inline STUMP data, the block hash, and the subtree's index in the block.

#### Scenario: Block with registered txids in subtrees
- **WHEN** a block is processed and subtrees contain registered txids
- **THEN** the processor SHALL publish one `CallbackTopicMessage` per registered txid to the `callback` topic with `type` set to `STUMP`, `txid` set to the transaction hash, `blockHash` set to the block hash, `subtreeIndex` set to the subtree's zero-based index in the block, and `stump` set to the serialized STUMP binary

#### Scenario: Block with no registered txids
- **WHEN** a block is processed and no subtrees contain registered txids
- **THEN** the processor completes without emitting STUMP callbacks

#### Scenario: Worker pool concurrency
- **WHEN** a block contains more subtrees than the configured workerPoolSize
- **THEN** the processor processes subtrees with bounded concurrency matching workerPoolSize

### Requirement: Build STUMP for registered txids in block subtree
The block subtree processor SHALL build a STUMP (Subtree Unified Merkle Path) for all registered txids found in a subtree. The STUMP MUST be built using the full merkle tree from `BuildMerkleTreeStoreFromBytes` and encoded in BRC-0074 BUMP format.

#### Scenario: STUMP built for multiple registered txids
- **WHEN** a subtree contains multiple registered txids with different callback URLs
- **THEN** the processor groups txids by callback URL, builds one STUMP per callback URL containing all that URL's txids, and emits one StumpsMessage per callback URL

#### Scenario: Subtree data retrieved from blob store
- **WHEN** the subtree was previously stored in the blob store during announcement processing
- **THEN** the block subtree processor retrieves it from the blob store without re-fetching from DataHub

#### Scenario: Subtree data not in blob store
- **WHEN** the subtree is not found in the blob store
- **THEN** the block subtree processor fetches it from the DataHub as a fallback

### Requirement: Update registration TTL after block processing
The block processor SHALL update the TTL of registration records for txids found in the block to allow time for fork handling before Aerospike eviction.

#### Scenario: Registration TTL updated
- **WHEN** registered txids are found in a block's subtrees
- **THEN** the processor updates their Aerospike TTL to the configured postMineTTLSec value

### Requirement: Emit BLOCK_PROCESSED after all subtrees complete
When all subtrees in a block have been processed, the system SHALL publish a `BLOCK_PROCESSED` callback to all registered callback URLs. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `BLOCK_PROCESSED` and `blockHash` set to the block hash.

#### Scenario: All subtrees processed
- **WHEN** the subtree counter for a block reaches zero
- **THEN** the system SHALL publish one `CallbackTopicMessage` per registered callback URL to the `callback` topic with `type` set to `BLOCK_PROCESSED` and `blockHash` set to the block hash

#### Scenario: Subtree index included in STUMP callbacks
- **WHEN** a subtree worker processes the Nth subtree (zero-indexed) in a block
- **THEN** the `subtreeIndex` field in the `CallbackTopicMessage` SHALL be set to N
