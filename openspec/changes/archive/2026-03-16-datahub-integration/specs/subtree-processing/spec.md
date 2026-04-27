## ADDED Requirements

### Requirement: Fetch and store subtree on announcement
The subtree processor SHALL fetch full subtree data from the DataHub URL in the announcement message and store the raw binary data in the subtree blob store.

#### Scenario: Subtree announced and stored
- **WHEN** a subtree announcement is received with a DataHubURL
- **THEN** the processor fetches the binary subtree data and stores it in the blob store keyed by subtree hash

#### Scenario: DataHub fetch fails
- **WHEN** a subtree announcement is received but the DataHub fetch fails after retries
- **THEN** the processor logs an error and skips processing the subtree

### Requirement: Check registrations in subtree
The subtree processor SHALL extract TxIDs from the parsed subtree, check which are registered using the registration cache and Aerospike batch lookup, and emit SEEN_ON_NETWORK callbacks for registered txids.

#### Scenario: Registered txid found in subtree
- **WHEN** a subtree contains a txid that has registered callback URLs
- **THEN** the processor emits a StumpsMessage with StatusType SEEN_ON_NETWORK for each callback URL

#### Scenario: No registered txids in subtree
- **WHEN** a subtree contains no registered txids
- **THEN** the processor completes without emitting any callbacks

#### Scenario: Registration cache hit
- **WHEN** a txid is already cached as not-registered
- **THEN** the processor skips Aerospike lookup for that txid

### Requirement: Track seen count and emit SEEN_MULTIPLE_NODES
The subtree processor SHALL increment the seen counter for each registered txid and emit a SEEN_MULTIPLE_NODES callback when the configured threshold is reached.

#### Scenario: Seen threshold reached
- **WHEN** a registered txid's seen count reaches the configured seenThreshold
- **THEN** the processor emits a StumpsMessage with StatusType SEEN_MULTIPLE_NODES for each callback URL

#### Scenario: Seen count below threshold
- **WHEN** a registered txid's seen count is below the threshold
- **THEN** no SEEN_MULTIPLE_NODES callback is emitted
