## MODIFIED Requirements

### Requirement: Process each subtree in block with worker pool
The block processor SHALL spawn a bounded worker pool to process each subtree in the block. Each worker retrieves the subtree data (from blob store or DataHub fallback), finds registered txids, builds a STUMP, and emits MINED callbacks. After all subtrees are processed, if any contained registered txids, the processor SHALL emit BLOCK_PROCESSED callbacks to all registered callback URLs.

#### Scenario: Block with registered txids in subtrees
- **WHEN** a block is processed and subtrees contain registered txids
- **THEN** the processor builds STUMPs for registered txids, emits StumpsMessages with StatusType MINED, and after all subtrees complete, emits BLOCK_PROCESSED to all callback URLs in the registry

#### Scenario: Block with no registered txids
- **WHEN** a block is processed and no subtrees contain registered txids
- **THEN** the processor completes without emitting MINED or BLOCK_PROCESSED callbacks

#### Scenario: Worker pool concurrency
- **WHEN** a block contains more subtrees than the configured workerPoolSize
- **THEN** the processor processes subtrees with bounded concurrency matching workerPoolSize

#### Scenario: Subtree processing errors do not prevent BLOCK_PROCESSED
- **WHEN** some subtrees fail to process but others succeed with registered txids
- **THEN** the processor still emits BLOCK_PROCESSED after all workers finish (successful subtrees triggered MINED callbacks)
