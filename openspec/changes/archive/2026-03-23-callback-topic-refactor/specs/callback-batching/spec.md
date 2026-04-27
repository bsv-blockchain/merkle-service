## MODIFIED Requirements

### Requirement: Cross-subtree callback batching
The system SHALL accumulate STUMP callback data across subtrees for the same callback URL within a block. When all subtrees for the block have been processed, the system SHALL publish individual `CallbackTopicMessage` messages (one per txid) to the `callback` topic rather than a single batched message.

#### Scenario: Block with multiple subtrees containing registered txids
- **WHEN** a block with subtrees S1, S2, S3 is processed AND callback URL U has registered txids in S1 and S3
- **THEN** the system SHALL publish one `CallbackTopicMessage` per registered txid to the `callback` topic, each with `type` set to `STUMP`, the respective `txid`, `blockHash`, `subtreeIndex`, and inline `stump` data

#### Scenario: Block with single subtree
- **WHEN** a block has only one subtree
- **THEN** the system SHALL publish one `CallbackTopicMessage` per registered txid (same as multi-subtree behavior)

#### Scenario: All subtrees processed signal
- **WHEN** the Aerospike subtree counter for a block reaches zero
- **THEN** the last subtree worker SHALL read the accumulated callback data, publish individual `CallbackTopicMessage` messages per txid, and clean up the accumulation record

### Requirement: Callback accumulation buffer stores inline STUMP data
The subtree worker SHALL store per-block callback accumulation data in Aerospike, including the serialized STUMP binary for each txid rather than cache references.

#### Scenario: Subtree worker appends to accumulation buffer
- **WHEN** a subtree worker processes subtree S for block B and finds registered txids for callback URL U
- **THEN** the worker SHALL atomically append each txid, its subtree index, and its serialized STUMP to the accumulation map keyed by block hash B

#### Scenario: Accumulation buffer cleanup
- **WHEN** individual callback messages have been published for a block
- **THEN** the accumulation buffer record SHALL be deleted from Aerospike
