## ADDED Requirements

### Requirement: Batch SEEN_ON_NETWORK callbacks per callbackURL per subtree
The subtree processor SHALL group all registered txids by callbackURL and publish one `CallbackTopicMessage` per callbackURL with `type` set to `SEEN_ON_NETWORK` and `TxIDs` containing the full list of matched transaction hashes.

#### Scenario: Multiple registered txids for same callbackURL
- **WHEN** a subtree contains txids T1, T2, T3 all registered to callbackURL U
- **THEN** the processor SHALL publish exactly one `CallbackTopicMessage` with `type` set to `SEEN_ON_NETWORK`, `callbackURL` set to U, and `TxIDs` set to `["T1", "T2", "T3"]`

#### Scenario: Registered txids across different callbackURLs
- **WHEN** a subtree contains txid T1 registered to U1 and txid T2 registered to U2
- **THEN** the processor SHALL publish two `CallbackTopicMessage` messages: one for U1 with `TxIDs: ["T1"]` and one for U2 with `TxIDs: ["T2"]`

#### Scenario: No registered txids in subtree
- **WHEN** a subtree contains no registered txids
- **THEN** the processor SHALL NOT publish any SEEN_ON_NETWORK callbacks

### Requirement: Batch SEEN_MULTIPLE_NODES callbacks per callbackURL per subtree
The subtree processor SHALL increment the seen counter for each registered txid, collect the txids that reached the threshold, group them by callbackURL, and publish one `CallbackTopicMessage` per callbackURL with `type` set to `SEEN_MULTIPLE_NODES` and `TxIDs` containing only the threshold-reached txids.

#### Scenario: Multiple txids reach threshold for same callbackURL
- **WHEN** txids T1 and T3 both reach `callback.seenThreshold` in the same subtree AND both are registered to callbackURL U
- **THEN** the processor SHALL publish one `CallbackTopicMessage` with `type` set to `SEEN_MULTIPLE_NODES`, `callbackURL` set to U, and `TxIDs` set to `["T1", "T3"]`

#### Scenario: Some txids below threshold
- **WHEN** txids T1 and T2 are registered to callbackURL U AND only T1 reaches the threshold
- **THEN** the SEEN_MULTIPLE_NODES message for U SHALL contain `TxIDs: ["T1"]` only

#### Scenario: No txids reach threshold
- **WHEN** no registered txids in the subtree reach the seen threshold
- **THEN** the processor SHALL NOT publish any SEEN_MULTIPLE_NODES callbacks

### Requirement: CallbackTopicMessage carries TxIDs array
The `CallbackTopicMessage` SHALL include a `TxIDs []string` field for batched SEEN callbacks. The existing `TxID` field SHALL remain for backward compatibility with other callback types.

#### Scenario: Batched SEEN message encoding
- **WHEN** a batched SEEN_ON_NETWORK message is encoded with `TxIDs: ["T1", "T2"]`
- **THEN** the JSON SHALL contain a `txids` array field with both values

#### Scenario: Single-txid message backward compatibility
- **WHEN** a SEEN_ON_NETWORK message uses the scalar `TxID` field
- **THEN** the message SHALL encode and decode correctly with `txid` as a string

### Requirement: Delivery payload includes txids array
The callback delivery service SHALL include a `txids` JSON array field in the HTTP POST body for SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES callbacks carrying batched TxIDs.

#### Scenario: Batched SEEN delivery payload
- **WHEN** delivering a SEEN_ON_NETWORK message with `TxIDs: ["T1", "T2", "T3"]`
- **THEN** the HTTP POST body SHALL contain `{"type":"SEEN_ON_NETWORK","txids":["T1","T2","T3"]}`

### Requirement: Partition by callbackURL for batched SEEN messages
Batched SEEN messages SHALL be published to Kafka using `PublishWithHashKey(callbackURL, data)` to ensure all messages for the same callbackURL land on the same partition.

#### Scenario: Consistent partitioning
- **WHEN** a batched SEEN_ON_NETWORK message is published for callbackURL U
- **THEN** the Kafka partition key SHALL be the SHA256 hash prefix of U (matching STUMP and BLOCK_PROCESSED partitioning)
