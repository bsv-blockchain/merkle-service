## ADDED Requirements

### Requirement: Callback delivery deduplication
The callback delivery system SHALL check whether a specific txid/callbackURL/type combination has already been successfully delivered before attempting HTTP delivery. The dedup key SHALL use the `CallbackType` values (`SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, `STUMP`, `BLOCK_PROCESSED`) from the `CallbackTopicMessage.Type` field.

#### Scenario: First delivery of a callback
- **WHEN** a callback message arrives for txid T, callbackURL U, and type S AND no prior successful delivery exists for (T, U, S)
- **THEN** the system SHALL deliver the HTTP POST to the callback URL AND record the successful delivery in Aerospike

#### Scenario: Duplicate callback message
- **WHEN** a callback message arrives for txid T, callbackURL U, and type S AND a prior successful delivery exists for (T, U, S)
- **THEN** the system SHALL skip the HTTP POST AND acknowledge the Kafka message

#### Scenario: BLOCK_PROCESSED dedup uses blockHash
- **WHEN** a `BLOCK_PROCESSED` callback message arrives with no txid
- **THEN** the dedup key SHALL be computed from `blockHash:callbackURL:BLOCK_PROCESSED`

#### Scenario: Multi-instance dedup correctness
- **WHEN** two delivery service instances consume the same callback message due to Kafka rebalance AND instance A successfully delivers and records dedup
- **THEN** instance B SHALL detect the existing dedup record and skip delivery

### Requirement: Callback dedup record TTL
Callback dedup records in Aerospike SHALL have a configurable TTL. After the TTL expires, the record is removed and a very late duplicate could be redelivered.

#### Scenario: Dedup record expires
- **WHEN** a dedup record for (T, U, S) was written more than TTL seconds ago AND a duplicate stumps message arrives
- **THEN** the system SHALL treat it as a first delivery and attempt HTTP POST

#### Scenario: Configurable TTL
- **WHEN** the configuration specifies `callback.dedupTTLSec`
- **THEN** dedup records SHALL be written with that TTL value

### Requirement: Idempotency key in callback HTTP requests
Each callback HTTP POST SHALL include an `X-Idempotency-Key` header that uniquely identifies the callback type for a given txid.

#### Scenario: SEEN_ON_NETWORK callback
- **WHEN** a SEEN_ON_NETWORK callback is delivered for txid T
- **THEN** the HTTP request SHALL include header `X-Idempotency-Key: {txid}:SEEN_ON_NETWORK`

#### Scenario: MINED callback
- **WHEN** a MINED callback is delivered for txid T
- **THEN** the HTTP request SHALL include header `X-Idempotency-Key: {txid}:MINED`

#### Scenario: Retried delivery uses same key
- **WHEN** a callback delivery fails and is retried
- **THEN** the retry SHALL use the same `X-Idempotency-Key` value as the original attempt

### Requirement: Callback dedup Aerospike set
Callback dedup records SHALL be stored in a dedicated Aerospike set, separate from registrations and seen counters.

#### Scenario: Dedup record key format
- **WHEN** a successful delivery is recorded for txid T, callbackURL U, statusType S
- **THEN** the record key SHALL be a hash of `{txid}:{callbackURL}:{statusType}` to keep key size bounded

#### Scenario: Aerospike set configuration
- **WHEN** the configuration specifies `aerospike.callbackDedupSet`
- **THEN** the system SHALL use that set name for callback dedup records
