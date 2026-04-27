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
The block processor SHALL spawn a bounded worker pool to process each subtree in the block. Each worker retrieves the subtree data (from blob store or DataHub fallback), finds registered txids, builds a STUMP, and emits MINED callbacks.

#### Scenario: Block with registered txids in subtrees
- **WHEN** a block is processed and subtrees contain registered txids
- **THEN** the processor builds STUMPs for registered txids and emits StumpsMessages with StatusType MINED, including encoded STUMP data and blockHash

#### Scenario: Block with no registered txids
- **WHEN** a block is processed and no subtrees contain registered txids
- **THEN** the processor completes without emitting MINED callbacks

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
