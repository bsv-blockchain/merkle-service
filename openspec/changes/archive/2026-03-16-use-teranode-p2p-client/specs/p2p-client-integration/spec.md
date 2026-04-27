## ADDED Requirements

### Requirement: P2P client uses go-teranode-p2p-client for initialization
The P2P client SHALL use `go-teranode-p2p-client`'s `Config.Initialize()` method to create the underlying P2P connection, providing the configured network name and storage path.

#### Scenario: Client initializes with network configuration
- **WHEN** the P2P client starts with `Network` set to "main"
- **THEN** the client creates a `p2p.Config{Network: "main", StoragePath: <configured path>}` and calls `Initialize(ctx, "merkle-service")` to obtain a connected p2p client with correct mainnet bootstrap peers and topic configuration

#### Scenario: Client initializes with testnet
- **WHEN** the P2P client starts with `Network` set to "test"
- **THEN** the client initializes with testnet bootstrap peers and topic configuration automatically

### Requirement: P2P client subscribes using typed subscription methods
The P2P client SHALL use `SubscribeSubtrees(ctx)` and `SubscribeBlocks(ctx)` to receive typed messages from the Teranode P2P network.

#### Scenario: Subtree messages received via typed subscription
- **WHEN** the P2P client is started and a subtree message arrives on the network
- **THEN** the message is received as a typed `teranode.SubtreeMessage` on the subscription channel (not raw bytes)

#### Scenario: Block messages received via typed subscription
- **WHEN** the P2P client is started and a block message arrives on the network
- **THEN** the message is received as a typed `teranode.BlockMessage` on the subscription channel (not raw bytes)

### Requirement: Typed messages are mapped to Kafka message types
The P2P client SHALL convert received teranode message types to the internal Kafka message types before publishing to Kafka.

#### Scenario: Subtree message mapped and published to Kafka
- **WHEN** a `teranode.SubtreeMessage` is received from the P2P subscription
- **THEN** the client maps it to a `kafka.SubtreeMessage`, encodes it, and publishes to the subtree Kafka producer with the subtree ID as the key

#### Scenario: Block message mapped and published to Kafka
- **WHEN** a `teranode.BlockMessage` is received from the P2P subscription
- **THEN** the client maps it to a `kafka.BlockMessage`, encodes it, and publishes to the block Kafka producer with the block hash as the key

#### Scenario: Malformed message does not crash the client
- **WHEN** a message cannot be mapped to the expected Kafka type (e.g., missing required fields)
- **THEN** the client logs an error and continues processing subsequent messages without interruption

### Requirement: P2P configuration is simplified
The `P2PConfig` struct SHALL contain only `Network` and `StoragePath` fields for P2P configuration. Topic names, bootstrap peers, DHT mode, NAT, mDNS, and private IP settings SHALL be managed by the `go-teranode-p2p-client` library.

#### Scenario: Minimal configuration required
- **WHEN** the P2P client is configured with only `Network` and `StoragePath`
- **THEN** the client initializes successfully with library-provided defaults for all other P2P settings

#### Scenario: Default network is mainnet
- **WHEN** `Network` is not explicitly configured
- **THEN** the P2P client defaults to "main" (mainnet)

### Requirement: P2P identity is managed by the library
The P2P client SHALL delegate private key loading, generation, and persistence to `go-teranode-p2p-client` via its `StoragePath` configuration.

#### Scenario: Key is auto-generated on first run
- **WHEN** the P2P client starts with an empty `StoragePath` directory
- **THEN** the library generates and persists a new P2P identity key

#### Scenario: Key is loaded on subsequent runs
- **WHEN** the P2P client starts with a `StoragePath` containing a previously generated key
- **THEN** the library loads the existing key, preserving the peer identity across restarts

### Requirement: P2P client lifecycle follows Init/Start/Stop/Health pattern
The P2P client SHALL maintain the existing service lifecycle interface with Init, Start, Stop, and Health methods.

#### Scenario: Health reports peer count and connection status
- **WHEN** the P2P client is running and Health is called
- **THEN** it returns the peer count from `GetPeers()` and connection status

#### Scenario: Stop gracefully shuts down the client
- **WHEN** Stop is called on a running P2P client
- **THEN** the context is cancelled, message processing goroutines complete, and `client.Close()` is called on the p2p-client
