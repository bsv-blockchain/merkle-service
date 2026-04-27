## ADDED Requirements

### Requirement: Subtree message deduplication
The subtree processor SHALL maintain an in-memory set of successfully-processed subtree hashes. When a subtree message arrives with a hash already in the set, the processor SHALL skip processing and mark the Kafka offset as consumed.

#### Scenario: Duplicate subtree message after successful processing
- **WHEN** a subtree message with hash H is received AND hash H has been successfully processed previously in this process lifetime
- **THEN** the processor SHALL skip all processing (no DataHub fetch, no registration check, no callback emission) AND mark the Kafka offset as consumed

#### Scenario: Subtree message after failed processing
- **WHEN** a subtree message with hash H is received AND a previous attempt to process hash H returned an error
- **THEN** the processor SHALL retry full processing (the hash is NOT in the processed set)

#### Scenario: Subtree message after process restart
- **WHEN** the processor restarts AND a subtree message with hash H arrives that was successfully processed before the restart
- **THEN** the processor SHALL process the message (in-memory set is empty after restart) AND callback-level dedup SHALL prevent duplicate deliveries

### Requirement: Block message deduplication
The block processor SHALL maintain an in-memory set of successfully-processed block hashes. When a block message arrives with a hash already in the set, the processor SHALL skip processing and mark the Kafka offset as consumed.

#### Scenario: Duplicate block message after successful processing
- **WHEN** a block message with hash H is received AND hash H has been successfully processed previously in this process lifetime
- **THEN** the processor SHALL skip all processing (no DataHub fetch, no subtree processing, no MINED callbacks) AND mark the Kafka offset as consumed

#### Scenario: Block message after failed processing
- **WHEN** a block message with hash H is received AND a previous attempt to process hash H returned an error (e.g., bad DataHub URL)
- **THEN** the processor SHALL retry full processing

### Requirement: Processed set bounded size
The processed-hash sets for both subtree and block processors SHALL be bounded by a configurable maximum size. When the set reaches maximum capacity, the oldest entries SHALL be evicted.

#### Scenario: Set reaches capacity
- **WHEN** the processed set contains max_size entries AND a new hash is successfully processed
- **THEN** the oldest entry SHALL be evicted AND the new hash SHALL be added

#### Scenario: Configurable size
- **WHEN** the configuration specifies `subtree.dedupCacheSize` or `block.dedupCacheSize`
- **THEN** the processor SHALL use that value as the maximum set size

### Requirement: Idempotent seen counter
The seen counter SHALL track which unique subtree IDs have contributed to a txid's count. Duplicate subtree announcements for the same txid SHALL NOT increment the count.

#### Scenario: First subtree for a txid
- **WHEN** `Increment(txid, subtreeID)` is called AND subtreeID has NOT been seen for this txid before
- **THEN** the subtreeID SHALL be added to the txid's set AND the unique count SHALL increase by 1

#### Scenario: Duplicate subtree for a txid
- **WHEN** `Increment(txid, subtreeID)` is called AND subtreeID has already been seen for this txid
- **THEN** the count SHALL NOT change AND ThresholdReached SHALL be false

#### Scenario: Threshold reached on unique subtree
- **WHEN** `Increment(txid, subtreeID)` is called with a new subtreeID AND the resulting unique count equals the threshold
- **THEN** ThresholdReached SHALL be true

#### Scenario: Threshold already passed
- **WHEN** `Increment(txid, subtreeID)` is called AND the unique count is already >= threshold
- **THEN** ThresholdReached SHALL be false (threshold fires exactly once)
