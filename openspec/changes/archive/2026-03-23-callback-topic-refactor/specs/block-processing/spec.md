## MODIFIED Requirements

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

### Requirement: Emit BLOCK_PROCESSED after all subtrees complete
When all subtrees in a block have been processed, the system SHALL publish a `BLOCK_PROCESSED` callback to all registered callback URLs. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `BLOCK_PROCESSED` and `blockHash` set to the block hash.

#### Scenario: All subtrees processed
- **WHEN** the subtree counter for a block reaches zero
- **THEN** the system SHALL publish one `CallbackTopicMessage` per registered callback URL to the `callback` topic with `type` set to `BLOCK_PROCESSED` and `blockHash` set to the block hash

#### Scenario: Subtree index included in STUMP callbacks
- **WHEN** a subtree worker processes the Nth subtree (zero-indexed) in a block
- **THEN** the `subtreeIndex` field in the `CallbackTopicMessage` SHALL be set to N
