## MODIFIED Requirements

### Requirement: Callback delivery deduplication
The callback delivery system SHALL check whether a specific txid/callbackURL/statusType combination has already been successfully delivered before attempting HTTP delivery. If already delivered, the message SHALL be acknowledged without making an HTTP request. This deduplication SHALL work correctly across multiple delivery service instances sharing the same Aerospike dedup store.

#### Scenario: First delivery of a callback
- **WHEN** a stumps message arrives for txid T, callbackURL U, and statusType S AND no prior successful delivery exists for (T, U, S)
- **THEN** the system SHALL deliver the HTTP POST to the callback URL AND record the successful delivery in Aerospike

#### Scenario: Duplicate callback message
- **WHEN** a stumps message arrives for txid T, callbackURL U, and statusType S AND a prior successful delivery exists for (T, U, S)
- **THEN** the system SHALL skip the HTTP POST AND acknowledge the Kafka message

#### Scenario: Failed delivery followed by retry
- **WHEN** a stumps message arrives for txid T, callbackURL U, statusType S AND the previous delivery attempt failed (no success record exists)
- **THEN** the system SHALL attempt delivery again

#### Scenario: Multi-instance dedup correctness
- **WHEN** two delivery service instances consume the same stumps message due to Kafka rebalance AND instance A successfully delivers and records dedup
- **THEN** instance B SHALL detect the existing dedup record and skip delivery
