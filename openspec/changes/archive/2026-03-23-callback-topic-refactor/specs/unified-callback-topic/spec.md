## ADDED Requirements

### Requirement: Unified callback Kafka topic
All callback events SHALL be published to a single `callback` Kafka topic (configurable via `kafka.callbackTopic`). The `stumps` topic SHALL no longer be used for callback delivery.

#### Scenario: Callback topic configured
- **WHEN** the service starts with `kafka.callbackTopic` set to `callback`
- **THEN** all producers SHALL publish callback messages to the `callback` topic AND all consumers SHALL consume from the `callback` topic

#### Scenario: Dead-letter queue topic
- **WHEN** a callback message exceeds max retry attempts
- **THEN** the message SHALL be published to the `callback-dlq` topic (configurable via `kafka.callbackDlqTopic`)

### Requirement: CallbackTopicMessage format
Messages on the `callback` topic SHALL use a `CallbackTopicMessage` struct containing all fields from Arcade's `CallbackMessage` (`type`, `txid`, `blockHash`, `subtreeIndex`, `stump`) plus delivery metadata (`callbackURL`, `retryCount`, `nextRetryAt`).

#### Scenario: SEEN_ON_NETWORK message
- **WHEN** a registered txid is found in a subtree
- **THEN** the published message SHALL have `type` set to `SEEN_ON_NETWORK`, `txid` set to the transaction hash, and all other CallbackMessage fields empty

#### Scenario: SEEN_MULTIPLE_NODES message
- **WHEN** a registered txid is seen in subtrees meeting the configured threshold
- **THEN** the published message SHALL have `type` set to `SEEN_MULTIPLE_NODES`, `txid` set to the transaction hash, and all other CallbackMessage fields empty

#### Scenario: STUMP message
- **WHEN** a registered txid is found in a block subtree and the STUMP is built
- **THEN** the published message SHALL have `type` set to `STUMP`, `txid` set to the transaction hash, `blockHash` set to the block hash, `subtreeIndex` set to the subtree's index in the block, and `stump` set to the serialized STUMP binary

#### Scenario: BLOCK_PROCESSED message
- **WHEN** all subtrees in a block have been processed
- **THEN** the published message SHALL have `type` set to `BLOCK_PROCESSED`, `blockHash` set to the block hash, and `txid`, `subtreeIndex`, and `stump` fields empty

### Requirement: Inline STUMP data in Kafka messages
STUMP callback messages SHALL embed the serialized STUMP binary directly in the `stump` field of the Kafka message. The system SHALL NOT use cache references (`StumpRef`/`StumpRefs`) for STUMP delivery.

#### Scenario: STUMP data included in message
- **WHEN** a subtree worker builds a STUMP for a registered txid
- **THEN** the worker SHALL serialize the STUMP and set it as the `stump` field in the `CallbackTopicMessage`

#### Scenario: Delivery service reads STUMP
- **WHEN** the delivery service processes a `STUMP` type message
- **THEN** it SHALL read the `stump` field directly without any cache lookup

### Requirement: HTTP POST body matches CallbackMessage
The callback delivery service SHALL POST a JSON body containing exactly the `CallbackMessage` fields: `type`, `txid` (omitted if empty), `blockHash` (omitted if empty), `subtreeIndex` (omitted if zero), and `stump` (hex-encoded, omitted if empty).

#### Scenario: STUMP callback HTTP body
- **WHEN** delivering a `STUMP` callback
- **THEN** the HTTP body SHALL be `{"type":"STUMP","txid":"<hash>","blockHash":"<hash>","subtreeIndex":<n>,"stump":"<hex>"}`

#### Scenario: SEEN_ON_NETWORK callback HTTP body
- **WHEN** delivering a `SEEN_ON_NETWORK` callback
- **THEN** the HTTP body SHALL be `{"type":"SEEN_ON_NETWORK","txid":"<hash>"}`

#### Scenario: BLOCK_PROCESSED callback HTTP body
- **WHEN** delivering a `BLOCK_PROCESSED` callback
- **THEN** the HTTP body SHALL be `{"type":"BLOCK_PROCESSED","blockHash":"<hash>"}`
