## ADDED Requirements

### Requirement: Callback URL registry
The system SHALL maintain a registry of all unique callback URLs that have been registered via the registration endpoint. The registry MUST be stored in Aerospike and support atomic, deduplicated additions and efficient enumeration of all URLs.

#### Scenario: Callback URL added to registry on registration
- **WHEN** a txid is registered with a callback URL
- **THEN** the callback URL is added to the registry if not already present

#### Scenario: Duplicate callback URL is not duplicated
- **WHEN** a callback URL that already exists in the registry is registered again
- **THEN** the registry retains exactly one entry for that URL

#### Scenario: All callback URLs can be enumerated
- **WHEN** the system needs to broadcast a message to all registered callback URLs
- **THEN** the registry returns the complete list of unique callback URLs

### Requirement: BLOCK_PROCESSED callback emission
The block processor SHALL emit a `BLOCK_PROCESSED` callback to every callback URL in the registry after all subtrees for a block have been successfully processed. The callback MUST include the block hash.

#### Scenario: Block with registered txids triggers BLOCK_PROCESSED
- **WHEN** all subtrees for a block are processed and at least one subtree contained registered txids
- **THEN** the processor emits a `BLOCK_PROCESSED` StumpsMessage for each callback URL in the registry, containing the block hash and StatusType BLOCK_PROCESSED

#### Scenario: Block with no registered txids does not trigger BLOCK_PROCESSED
- **WHEN** all subtrees for a block are processed but no subtree contained registered txids
- **THEN** the processor does NOT emit any BLOCK_PROCESSED callbacks

#### Scenario: BLOCK_PROCESSED message format
- **WHEN** a BLOCK_PROCESSED StumpsMessage is created
- **THEN** it SHALL contain the callbackUrl, statusType "BLOCK_PROCESSED", and blockHash fields; stumpData and txids fields SHALL be empty

### Requirement: BLOCK_PROCESSED callback delivery
The callback delivery service SHALL deliver `BLOCK_PROCESSED` messages via the same HTTP POST mechanism used for other status types. The payload MUST include the status and block hash.

#### Scenario: BLOCK_PROCESSED delivered via HTTP POST
- **WHEN** a BLOCK_PROCESSED message is consumed from the stumps topic
- **THEN** the delivery service sends an HTTP POST to the callback URL with a JSON payload containing status "BLOCK_PROCESSED" and the blockHash

#### Scenario: BLOCK_PROCESSED delivery with retries
- **WHEN** a BLOCK_PROCESSED callback delivery fails
- **THEN** the delivery service retries with the same linear backoff as other callback types

### Requirement: BLOCK_PROCESSED deduplication
The system SHALL deduplicate BLOCK_PROCESSED callbacks using the blockHash as the dedup key (instead of txid).

#### Scenario: Duplicate BLOCK_PROCESSED is skipped
- **WHEN** a BLOCK_PROCESSED callback for a given blockHash and callbackUrl has already been delivered
- **THEN** the delivery service skips the duplicate without re-delivering

#### Scenario: BLOCK_PROCESSED idempotency key
- **WHEN** building the idempotency key for a BLOCK_PROCESSED message
- **THEN** the key SHALL be `blockHash:BLOCK_PROCESSED`
