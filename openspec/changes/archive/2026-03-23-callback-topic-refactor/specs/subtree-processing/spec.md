## MODIFIED Requirements

### Requirement: Check registrations in subtree
The subtree processor SHALL extract TxIDs from the parsed subtree, check which are registered using the registration cache and Aerospike batch lookup, and emit SEEN_ON_NETWORK callbacks for registered txids. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `SEEN_ON_NETWORK`.

#### Scenario: Registered txid found in subtree
- **WHEN** a subtree contains a txid that has registered callback URLs
- **THEN** the processor SHALL publish a `CallbackTopicMessage` to the `callback` topic with `type` set to `SEEN_ON_NETWORK`, `txid` set to the transaction hash, and `callbackURL` set to the registered URL

#### Scenario: No registered txids in subtree
- **WHEN** a subtree contains no registered txids
- **THEN** the processor completes without publishing any callbacks

#### Scenario: Registration cache hit
- **WHEN** a txid is already cached as not-registered
- **THEN** the processor skips Aerospike lookup for that txid

### Requirement: Track seen count and emit SEEN_MULTIPLE_NODES
The subtree processor SHALL increment the seen counter for each registered txid and emit a SEEN_MULTIPLE_NODES callback when the configured threshold is reached. Callbacks SHALL be published to the `callback` topic using `CallbackTopicMessage` format with `type` set to `SEEN_MULTIPLE_NODES`.

#### Scenario: Threshold reached
- **WHEN** a registered txid has been seen in subtrees meeting `callback.seenThreshold`
- **THEN** the processor SHALL publish a `CallbackTopicMessage` to the `callback` topic with `type` set to `SEEN_MULTIPLE_NODES` and `txid` set to the transaction hash

#### Scenario: Below threshold
- **WHEN** a registered txid has been seen in fewer subtrees than `callback.seenThreshold`
- **THEN** the processor SHALL NOT emit a `SEEN_MULTIPLE_NODES` callback
