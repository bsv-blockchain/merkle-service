## MODIFIED Requirements

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
