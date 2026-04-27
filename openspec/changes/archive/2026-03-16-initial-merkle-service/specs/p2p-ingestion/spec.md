## ADDED Requirements

### Requirement: Connect to Teranode P2P network
The system SHALL maintain a libp2p client that connects to the Teranode P2P network using DHT-based peer discovery. The client SHALL handle connection lifecycle including reconnection on failure.

> Traces to: requirements.md — "p2p client: teranode libp2p client"

#### Scenario: Successful P2P connection
- **WHEN** the P2P ingestion service starts
- **THEN** the system establishes a libp2p connection to the Teranode network using configured bootstrap peers
- **THEN** the system discovers additional peers via DHT

#### Scenario: Reconnection on disconnect
- **WHEN** the P2P connection to Teranode is lost
- **THEN** the system attempts to reconnect with exponential backoff
- **THEN** the system logs the disconnection and reconnection attempts

### Requirement: Subscribe to subtree topic
The system SHALL subscribe to the Teranode libp2p subtree topic and receive subtree messages as they are produced by Teranode validators.

> Traces to: requirements.md — "p2p client: listens for subtrees and blocks"

#### Scenario: Receive subtree message
- **WHEN** Teranode publishes a subtree message on the subtree P2P topic
- **THEN** the system receives the subtree message containing subtree ID, transaction list, and merkle data

### Requirement: Subscribe to block topic
The system SHALL subscribe to the Teranode libp2p block topic and receive block messages when blocks are mined.

> Traces to: requirements.md — "p2p client: listens for subtrees and blocks"

#### Scenario: Receive block message
- **WHEN** Teranode publishes a block message on the block P2P topic
- **THEN** the system receives the block message containing block hash, block header, and list of subtree references

### Requirement: Publish P2P messages to Kafka
The system SHALL publish received subtree messages to the Kafka subtree topic and block messages to the Kafka block topic. Messages SHALL be published with appropriate partition keys.

> Traces to: requirements.md — "p2p client: publishes to kafka"

#### Scenario: Publish subtree to Kafka
- **WHEN** a subtree message is received from the P2P network
- **THEN** the system publishes the subtree message to the Kafka `subtree` topic with the subtree ID as partition key

#### Scenario: Publish block to Kafka
- **WHEN** a block message is received from the P2P network
- **THEN** the system publishes the block message to the Kafka `block` topic with the block hash as partition key

#### Scenario: Kafka publish failure
- **WHEN** publishing to Kafka fails
- **THEN** the system retries the publish with backoff
- **THEN** the system logs the failure and increments an error metric
