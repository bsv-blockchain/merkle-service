## Context

The merkle-service P2P client (`internal/p2p/client.go`) currently uses `go-p2p-message-bus` directly. It manually constructs topic names (`buildTopicName`, `buildProtocolVersion`), manages private key loading/generation (`key.go`), and requires explicit configuration of bootstrap peers, DHT mode, NAT traversal, and mDNS settings.

The `go-teranode-p2p-client` package (used by Arcade in production) wraps `go-p2p-message-bus` and provides:
- Network-aware topic naming (`TopicName(network, type)` producing `teranode/bitcoin/1.0.0/{network}-{type}`)
- Embedded bootstrap peers per network (mainnet, testnet, STN, teratestnet)
- Typed subscription methods (`SubscribeBlocks`, `SubscribeSubtrees`) returning typed channels
- P2P identity management via `LoadOrGeneratePrivateKey` and `Config.Initialize()`
- Sensible defaults for connection limits, DHT, peer cache TTL

The current manual approach risks topic naming mismatches and requires operators to discover and configure bootstrap peers.

## Goals / Non-Goals

**Goals:**
- Replace direct `go-p2p-message-bus` usage with `go-teranode-p2p-client` for correct topic/peer configuration
- Simplify `P2PConfig` by removing fields the library manages internally
- Follow the same integration pattern as Arcade (`Config.Initialize()` → typed subscriptions)
- Maintain the existing Kafka publish pipeline (message handlers remain the same)

**Non-Goals:**
- Subscribing to additional topics (rejected-tx, node-status) — not needed for merkle-service currently
- Changing the Kafka message format or encoding
- Changing the Init/Start/Stop/Health lifecycle pattern

## Decisions

### 1. Use `Config.Initialize()` for client creation

**Choice**: Use `p2p.Config{Network, StoragePath}.Initialize(ctx, "merkle-service")` instead of manually building `p2pMessageBus.Config`.

**Rationale**: This is the intended API. It applies network-specific defaults (bootstrap peers, protocol version, connection limits), loads/generates the private key, and creates the underlying message bus client in one call. Arcade uses this same pattern.

**Alternative considered**: Manually configure `go-teranode-p2p-client` fields. Rejected because it defeats the purpose of using the library — we'd still be responsible for knowing the correct bootstrap peers and topic formats.

### 2. Use typed subscription methods

**Choice**: Use `client.SubscribeBlocks(ctx)` and `client.SubscribeSubtrees(ctx)` which return `<-chan teranode.BlockMessage` and `<-chan teranode.SubtreeMessage`.

**Rationale**: Typed channels eliminate the JSON deserialization step in our message handlers. The library already handles unmarshaling from the raw P2P wire format. This removes a potential source of deserialization bugs.

**Impact on message handlers**: `handleSubtreeMessage` and `handleBlockMessage` will receive typed structs instead of `[]byte`. They'll need to map from the teranode message types to our Kafka message types. The mapping is straightforward field-by-field copy.

**Alternative considered**: Continue using raw `Subscribe(topicName)` on the underlying client. Rejected because the typed API is the library's primary interface and handles topic name construction internally.

### 3. Simplify P2PConfig to Network + StoragePath

**Choice**: Reduce `P2PConfig` to `Network` and `StoragePath` (replaces `PeerCacheDir`). Remove `BootstrapPeers`, `SubtreeTopic`, `BlockTopic`, `DHTMode`, `EnableNAT`, `EnableMDNS`, `AllowPrivateIPs`, `Port`, `AnnounceAddrs`, `Name`, `PrivateKey`.

**Rationale**: The library handles all of these via `SetDefaults()` and network-specific configuration. Exposing them invites misconfiguration. The `StoragePath` covers both key persistence and peer cache.

**Alternative considered**: Keep fields as overrides. Rejected for simplicity — if advanced overrides are needed in the future, they can be added back on a case-by-case basis.

### 4. Remove `key.go`

**Choice**: Delete `internal/p2p/key.go` entirely.

**Rationale**: `go-teranode-p2p-client` provides `LoadOrGeneratePrivateKey(storagePath)` and handles key management internally during `Initialize()`. Our custom implementation is redundant.

### 5. Map teranode message types to Kafka message types

**Choice**: Convert `teranode.SubtreeMessage` → `kafka.SubtreeMessage` and `teranode.BlockMessage` → `kafka.BlockMessage` in the handlers.

**Rationale**: We want to keep our internal Kafka message types decoupled from the external teranode types. This is a thin mapping layer. If the teranode types evolve independently, only the mapping code needs updating.

## Risks / Trade-offs

- **[Teranode message type mismatch]** → The teranode package's `BlockMessage` and `SubtreeMessage` fields may not map 1:1 to our Kafka message types. Mitigation: verify field mapping during implementation; add adapter logic as needed.
- **[Loss of configuration flexibility]** → Removing advanced P2P config fields means operators can't override bootstrap peers or connection settings. Mitigation: acceptable trade-off since the library provides correct defaults; can re-add specific overrides if a real need arises.
- **[Version coupling]** → We depend on `go-teranode-p2p-client`'s topic naming and bootstrap peers staying correct. Mitigation: this is an official BSV Blockchain repository maintained alongside Teranode itself.
- **[Breaking config change]** → Existing deployments using `P2P_BOOTSTRAP_PEERS`, `P2P_SUBTREE_TOPIC`, etc. will have those settings silently ignored. Mitigation: document in release notes; log a warning if deprecated env vars are set.
