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
The subtree processor SHALL extract TxIDs from the parsed subtree, check which are registered using the registration cache and Aerospike batch lookup, and emit batched SEEN_ON_NETWORK callbacks grouped by callbackURL. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `SEEN_ON_NETWORK` and `TxIDs` containing all matched txids for that callbackURL.

#### Scenario: Registered txids found in subtree
- **WHEN** a subtree contains txids that have registered callback URLs
- **THEN** the processor SHALL group txids by callbackURL and publish one `CallbackTopicMessage` per callbackURL to the `callback` topic with `type` set to `SEEN_ON_NETWORK` and `TxIDs` set to the list of matched txids for that URL

#### Scenario: No registered txids in subtree
- **WHEN** a subtree contains no registered txids
- **THEN** the processor completes without publishing any callbacks

#### Scenario: Registration cache hit
- **WHEN** a txid is already cached as not-registered
- **THEN** the processor skips Aerospike lookup for that txid

### Requirement: Track seen count and emit SEEN_MULTIPLE_NODES
The subtree processor SHALL increment the seen counter for each registered txid, collect txids that reached the configured threshold, group them by callbackURL, and emit one batched SEEN_MULTIPLE_NODES callback per callbackURL. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `SEEN_MULTIPLE_NODES` and `TxIDs` containing the threshold-reached txids for that callbackURL.

#### Scenario: Threshold reached
- **WHEN** registered txids have been seen in subtrees meeting `callback.seenThreshold`
- **THEN** the processor SHALL group threshold-reached txids by callbackURL and publish one `CallbackTopicMessage` per callbackURL with `type` set to `SEEN_MULTIPLE_NODES` and `TxIDs` set to the threshold-reached txids for that URL

#### Scenario: Below threshold
- **WHEN** no registered txids in the subtree have reached `callback.seenThreshold`
- **THEN** the processor SHALL NOT emit any SEEN_MULTIPLE_NODES callbacks
