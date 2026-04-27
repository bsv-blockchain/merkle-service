## ADDED Requirements

### Requirement: P2P client via go-p2p-message-bus
The system SHALL use `go-p2p-message-bus.NewClient()` to establish P2P connectivity instead of directly managing libp2p, DHT, and GossipSub. The client SHALL be configured with identity, network, bootstrap, and discovery settings matching Teranode's patterns.

> Traces to: requirements.md — "p2p client: teranode libp2p client" and "Leverage teranode learnings wherever possible"

#### Scenario: Successful client initialization
- **WHEN** the P2P service is initialized with valid configuration (name, private key, network)
- **THEN** the system creates a go-p2p-message-bus Client that connects to the configured bootstrap peers
- **THEN** the Client is ready to subscribe to topics

#### Scenario: Client shutdown
- **WHEN** the P2P service receives a stop signal
- **THEN** the system calls `Client.Close()` to release all P2P resources
- **THEN** all subscription channels are closed and goroutines exit

### Requirement: Network-configurable topic naming
The system SHALL construct P2P topic names using the pattern `{topicPrefix}-{topicSuffix}` where `topicPrefix` is derived from the configured network (mainnet, testnet, teratestnet) and `topicSuffix` identifies the message type (block, subtree). This SHALL match Teranode's topic naming convention.

> Traces to: requirements.md — "p2p client: listens for subtrees and blocks"

#### Scenario: Mainnet topic naming
- **WHEN** the system is configured with network "mainnet"
- **THEN** the subtree topic name SHALL be "mainnet-subtree"
- **THEN** the block topic name SHALL be "mainnet-block"

#### Scenario: Testnet topic naming
- **WHEN** the system is configured with network "testnet"
- **THEN** the subtree topic name SHALL be "testnet-subtree"
- **THEN** the block topic name SHALL be "testnet-block"

#### Scenario: Teratestnet topic naming
- **WHEN** the system is configured with network "teratestnet"
- **THEN** the subtree topic name SHALL be "teratestnet-subtree"
- **THEN** the block topic name SHALL be "teratestnet-block"

### Requirement: Protocol version matching
The system SHALL use a libp2p protocol version string matching Teranode's pattern: `/teranode/bitcoin/{network}/1.0.0`. This ensures the merkle-service joins the correct GossipSub mesh for the configured network.

> Traces to: requirements.md — "Follow the same daemon/service pattern that Teranode uses"

#### Scenario: Protocol version on mainnet
- **WHEN** the system is configured with network "mainnet"
- **THEN** the protocol version SHALL be "/teranode/bitcoin/mainnet/1.0.0"

### Requirement: Channel-based message subscription
The system SHALL subscribe to P2P topics using `Client.Subscribe(topicName)` which returns a `<-chan Message`. Message processing SHALL read from these channels in dedicated goroutines.

> Traces to: requirements.md — "p2p client: listens for subtrees and blocks"

#### Scenario: Subscribe to subtree topic
- **WHEN** the P2P service starts
- **THEN** the system subscribes to the network-prefixed subtree topic
- **THEN** a goroutine reads `Message` values from the returned channel

#### Scenario: Subscribe to block topic
- **WHEN** the P2P service starts
- **THEN** the system subscribes to the network-prefixed block topic
- **THEN** a goroutine reads `Message` values from the returned channel

### Requirement: Ed25519 private key management
The system SHALL support loading an Ed25519 private key from (1) environment variable `P2P_PRIVATE_KEY` (hex-encoded), (2) a key file in the configured peer cache directory, or (3) auto-generating a new key and persisting it to the peer cache directory.

> Traces to: requirements.md — "Follow the same daemon/service pattern that Teranode uses"

#### Scenario: Key from environment variable
- **WHEN** `P2P_PRIVATE_KEY` environment variable is set with a valid hex-encoded Ed25519 key
- **THEN** the system uses that key for the P2P identity

#### Scenario: Key from file
- **WHEN** `P2P_PRIVATE_KEY` is not set and a key file exists in the peer cache directory
- **THEN** the system loads the key from the file

#### Scenario: Auto-generate key
- **WHEN** no key is available from env or file
- **THEN** the system generates a new Ed25519 key pair and persists it to the peer cache directory

### Requirement: DHT mode selection
The system SHALL support configuring the DHT mode to "server", "client", or "off" via configuration. The default SHALL be "off" for lightweight operation.

> Traces to: requirements.md — "Leverage teranode learnings wherever possible"

#### Scenario: DHT off mode (default)
- **WHEN** DHT mode is "off"
- **THEN** the client connects only to bootstrap peers and discovers topic peers via GossipSub mesh
- **THEN** no DHT routing table is maintained

#### Scenario: DHT server mode
- **WHEN** DHT mode is "server"
- **THEN** the client participates fully in the DHT network, advertising itself and routing queries

### Requirement: Peer caching
The system SHALL support persisting discovered peers to a JSON file in the configured peer cache directory. This SHALL enable faster reconnection after restarts.

> Traces to: requirements.md — "Leverage teranode learnings wherever possible"

#### Scenario: Peer cache persistence
- **WHEN** the P2P client discovers peers during operation
- **THEN** peer information is periodically saved to the cache file
- **THEN** on restart, previously discovered peers are loaded from the cache
