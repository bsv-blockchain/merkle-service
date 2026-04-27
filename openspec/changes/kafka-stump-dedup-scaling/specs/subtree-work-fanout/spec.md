## ADDED Requirements

### Requirement: SubtreeWorkMessage for Kafka fan-out
The system SHALL define a SubtreeWorkMessage type containing the block hash, block height, subtree hash, and DataHub URL. The block processor SHALL publish one SubtreeWorkMessage per subtree to the `subtree-work` Kafka topic.

#### Scenario: Block processor dispatches subtree work
- **WHEN** a block announcement is received with N subtrees
- **THEN** the block processor publishes N SubtreeWorkMessages to the subtree-work topic, one per subtree, keyed by subtreeHash

#### Scenario: SubtreeWorkMessage contains required fields
- **WHEN** a SubtreeWorkMessage is published
- **THEN** it contains blockHash, blockHeight, subtreeHash, and dataHubURL fields sufficient for a subtree worker to independently process the subtree

### Requirement: Subtree worker consumer service
The system SHALL provide a SubtreeWorkerService that consumes SubtreeWorkMessages from the subtree-work topic. Each worker SHALL retrieve subtree data, check registrations, build a STUMP, store it in the STUMP cache, and publish slim MINED messages (with StumpRef) to the stumps topic.

#### Scenario: Subtree worker processes work item
- **WHEN** a subtree worker receives a SubtreeWorkMessage
- **THEN** it retrieves the subtree data, batch-checks registrations, builds the STUMP, stores it in the STUMP cache, and publishes one StumpsMessage per callback URL with StumpRef instead of inline StumpData

#### Scenario: Subtree with no registered txids
- **WHEN** a subtree worker processes a subtree with no registered txids
- **THEN** it completes without publishing any MINED messages

#### Scenario: Horizontal scaling via consumer group
- **WHEN** multiple subtree worker instances join the same consumer group
- **THEN** Kafka rebalances partitions of the subtree-work topic across instances, enabling parallel processing of different subtrees

#### Scenario: Subtree worker failure and rebalance
- **WHEN** a subtree worker instance crashes or disconnects
- **THEN** Kafka rebalances its partitions to remaining instances, and unacknowledged work items are reprocessed

### Requirement: Subtree work topic configuration
The system SHALL support configuration of the subtree-work Kafka topic name and partition count.

#### Scenario: Default configuration
- **WHEN** no subtree-work topic configuration is provided
- **THEN** the system uses topic name `subtree-work` with 256 partitions as defaults

#### Scenario: Custom topic configuration
- **WHEN** custom subtree-work topic settings are configured
- **THEN** the system uses the configured topic name and partition count

### Requirement: BLOCK_PROCESSED coordination with atomic counter
The system SHALL coordinate BLOCK_PROCESSED callback emission across distributed subtree workers using an Aerospike atomic counter. The block processor SHALL initialize the counter to the subtree count, and each subtree worker SHALL atomically decrement it upon completion.

#### Scenario: Block processor initializes subtree counter
- **WHEN** the block processor dispatches N subtree work items for a block
- **THEN** it writes an Aerospike record `{blockHash: N}` as the pending subtree count

#### Scenario: Subtree worker decrements counter
- **WHEN** a subtree worker completes processing a subtree for a block
- **THEN** it atomically decrements the block's pending subtree counter by 1

#### Scenario: Last subtree worker emits BLOCK_PROCESSED
- **WHEN** a subtree worker decrements the counter to zero
- **THEN** it emits BLOCK_PROCESSED StumpsMessages to all registered callback URLs

#### Scenario: Counter not zero after subtree completion
- **WHEN** a subtree worker decrements the counter and the result is greater than zero
- **THEN** it does NOT emit BLOCK_PROCESSED callbacks (other subtrees still pending)

#### Scenario: Counter TTL for cleanup
- **WHEN** a block's subtree counter is initialized
- **THEN** it is stored with a TTL (default 10 minutes) to automatically clean up counters for blocks that fail to fully process
