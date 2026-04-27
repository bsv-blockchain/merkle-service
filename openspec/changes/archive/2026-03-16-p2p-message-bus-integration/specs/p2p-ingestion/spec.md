## MODIFIED Requirements

### Requirement: Connect to Teranode P2P network
The system SHALL use a go-p2p-message-bus Client to connect to the Teranode P2P network. The client SHALL be configured with network-specific settings (protocol version, topic prefix, bootstrap peers) to ensure connectivity with the correct network's peers.

> Traces to: requirements.md — "p2p client: teranode libp2p client"

#### Scenario: Successful P2P connection
- **WHEN** the P2P ingestion service starts
- **THEN** the system creates a go-p2p-message-bus Client with the configured network, bootstrap peers, and DHT settings
- **THEN** the system connects to the Teranode P2P network

#### Scenario: Reconnection on disconnect
- **WHEN** the P2P connection to Teranode is lost
- **THEN** the go-p2p-message-bus Client handles reconnection automatically via its built-in peer management
- **THEN** the system logs the reconnection events

### Requirement: Subscribe to subtree topic
The system SHALL subscribe to the Teranode subtree P2P topic using `Client.Subscribe()` which returns a `<-chan Message`. The topic name SHALL be constructed as `{network}-subtree`.

> Traces to: requirements.md — "p2p client: listens for subtrees and blocks"

#### Scenario: Receive subtree message
- **WHEN** Teranode publishes a subtree message on the subtree P2P topic
- **THEN** the system receives a `Message` from the subscription channel containing the subtree data in `Message.Data`

### Requirement: Subscribe to block topic
The system SHALL subscribe to the Teranode block P2P topic using `Client.Subscribe()` which returns a `<-chan Message`. The topic name SHALL be constructed as `{network}-block`.

> Traces to: requirements.md — "p2p client: listens for subtrees and blocks"

#### Scenario: Receive block message
- **WHEN** Teranode publishes a block message on the block P2P topic
- **THEN** the system receives a `Message` from the subscription channel containing the block data in `Message.Data`

### Requirement: Publish P2P messages to Kafka
The system SHALL publish received P2P messages to Kafka topics. Messages received from `<-chan Message` SHALL be deserialized and published using the Kafka producer with the appropriate partition key.

> Traces to: requirements.md — "p2p client: publishes to kafka"

#### Scenario: Publish subtree to Kafka
- **WHEN** a subtree `Message` is received from the subscription channel
- **THEN** the system deserializes `Message.Data`, publishes to the Kafka subtree topic with the subtree ID as partition key

#### Scenario: Publish block to Kafka
- **WHEN** a block `Message` is received from the subscription channel
- **THEN** the system deserializes `Message.Data`, publishes to the Kafka block topic with the block hash as partition key

#### Scenario: Kafka publish failure
- **WHEN** publishing to Kafka fails
- **THEN** the system retries the publish with exponential backoff
- **THEN** the system logs the failure and increments an error metric
