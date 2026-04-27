## ADDED Requirements

### Requirement: Cross-subtree callback batching
The system SHALL accumulate MINED callback data across subtrees for the same callback URL within a block, and publish a single batched StumpsMessage per callback URL once all subtrees for the block have been processed.

#### Scenario: Block with multiple subtrees containing registered txids
- **WHEN** a block with subtrees S1, S2, S3 is processed AND callback URL U has registered txids in S1 and S3
- **THEN** the system SHALL publish one batched StumpsMessage to the stumps topic containing all matched txids from S1 and S3, with StumpRefs for both subtrees

#### Scenario: Block with single subtree
- **WHEN** a block has only one subtree
- **THEN** the system SHALL publish a single StumpsMessage per callback URL (same as current behavior)

#### Scenario: All subtrees processed signal
- **WHEN** the Aerospike subtree counter for a block reaches zero
- **THEN** the last subtree worker SHALL read the accumulated callback data, publish batched messages, and clean up the accumulation record

### Requirement: Callback accumulation buffer in Aerospike
The subtree worker SHALL store per-block callback accumulation data in Aerospike using CDT map operations, enabling cross-pod accumulation in K8s deployments.

#### Scenario: Subtree worker appends to accumulation buffer
- **WHEN** a subtree worker processes subtree S for block B and finds registered txids for callback URL U
- **THEN** the worker SHALL atomically append the txid list and StumpRef to the accumulation map keyed by block hash B

#### Scenario: Accumulation buffer TTL
- **WHEN** an accumulation buffer record is created in Aerospike
- **THEN** it SHALL have a configurable TTL (default matching subtree counter TTL) to ensure cleanup on partial failures

#### Scenario: Accumulation buffer cleanup
- **WHEN** batched messages have been published for a block
- **THEN** the accumulation buffer record SHALL be deleted from Aerospike

### Requirement: Batched StumpsMessage format
StumpsMessage SHALL support a `StumpRefs` field (string slice) to carry multiple STUMP references when txids span multiple subtrees within a block.

#### Scenario: Batched message with multiple StumpRefs
- **WHEN** a batched StumpsMessage carries txids from subtrees S1 and S3
- **THEN** the message SHALL include `StumpRefs: ["S1", "S3"]` and the delivery service SHALL resolve all referenced STUMPs from cache

#### Scenario: Backward compatibility
- **WHEN** a StumpsMessage has the singular `StumpRef` field set (not `StumpRefs`)
- **THEN** the delivery service SHALL resolve it as before (single STUMP lookup)

### Requirement: Delivery service resolves multiple StumpRefs
The delivery service SHALL resolve all StumpRefs in a batched message and include all STUMP data in the HTTP callback payload.

#### Scenario: All StumpRefs resolve from cache
- **WHEN** a batched message has `StumpRefs: ["S1", "S3"]` AND both are present in the STUMP cache
- **THEN** the delivery service SHALL encode all STUMP data and deliver a single HTTP POST with all txids and all STUMP data

#### Scenario: Partial StumpRef cache miss
- **WHEN** a batched message has `StumpRefs: ["S1", "S3"]` AND S3 is missing from cache
- **THEN** the delivery service SHALL re-enqueue the message for retry (same as current single StumpRef miss behavior)
