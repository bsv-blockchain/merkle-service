## MODIFIED Requirements

### Requirement: Process each subtree in block with worker pool
The block processor SHALL dispatch subtree processing to the subtree-work Kafka topic instead of processing subtrees in-process. For each subtree in the block, the processor publishes a SubtreeWorkMessage and initializes an Aerospike atomic counter tracking pending subtrees. The actual subtree processing (registration lookup, STUMP building, MINED message publishing) is performed by the SubtreeWorkerService consuming from the subtree-work topic.

#### Scenario: Block with registered txids in subtrees
- **WHEN** a block is processed and subtrees contain registered txids
- **THEN** the block processor publishes SubtreeWorkMessages to the subtree-work topic, and the subtree workers build STUMPs and emit StumpsMessages with StatusType MINED containing StumpRef references

#### Scenario: Block with no registered txids
- **WHEN** a block is processed and no subtrees contain registered txids
- **THEN** the subtree workers complete without emitting MINED callbacks, and the last worker emits BLOCK_PROCESSED

#### Scenario: Worker pool concurrency via Kafka partitions
- **WHEN** a block contains more subtrees than available subtree worker consumers
- **THEN** excess work items queue in Kafka partitions and are processed as workers become available

#### Scenario: Block processor publishes work items quickly
- **WHEN** a block with N subtrees is announced
- **THEN** the block processor publishes N SubtreeWorkMessages and returns, without waiting for subtree processing to complete
