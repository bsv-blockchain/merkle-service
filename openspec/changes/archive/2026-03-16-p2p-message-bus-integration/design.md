## Context

The merkle-service P2P client currently hand-rolls libp2p host creation, Kademlia DHT, GossipSub pubsub, peer discovery, and reconnection logic in ~550 lines. This duplicates what Teranode has packaged in `go-p2p-message-bus` — a production-hardened library that wraps libp2p with secure defaults, channel-based messaging, automatic peer management, and network isolation.

Teranode's P2P server (`services/p2p/Server.go`) uses go-p2p-message-bus by:
1. Building a `Config` with network-specific settings (protocol version, topic prefix, bootstrap peers)
2. Calling `NewClient(config)` to get a fully initialized `Client`
3. Calling `client.Subscribe(topicName)` which returns a `<-chan Message`
4. Reading messages from channels in goroutines and dispatching to handlers

The merkle-service needs to receive subtree and block messages from Teranode peers on a configurable network. It does NOT need to publish to P2P topics — it only subscribes.

**Stakeholders**: Arcade team (consumer of merkle proofs), Teranode team (P2P protocol compatibility), infrastructure team (deployment across networks).

**Constraints**:
- Must use the same topic naming and protocol version as Teranode to interoperate
- Must support mainnet, testnet, and teratestnet via configuration
- Must follow Teranode's daemon/service lifecycle pattern (Init/Start/Stop/Health)

## Goals / Non-Goals

**Goals:**
- Replace raw libp2p usage with go-p2p-message-bus for Teranode-compatible P2P connectivity
- Support configurable network selection (mainnet, testnet, teratestnet)
- Match Teranode's topic naming pattern: `{topicPrefix}-{topicSuffix}`
- Match Teranode's protocol version pattern: `/teranode/bitcoin/{network}/1.0.0`
- Support DHT mode selection (server, client, off) matching Teranode's architecture
- Preserve Kafka publish-with-retry behavior for received P2P messages

**Non-Goals:**
- Publishing messages to P2P topics (merkle-service is a subscriber only)
- Peer reputation/ban management (not needed for a read-only subscriber)
- Node status broadcasting (merkle-service doesn't participate as a validator)

## Decisions

### 1. Use go-p2p-message-bus Client directly

**Decision**: Replace all libp2p/DHT/PubSub code with a single `go-p2p-message-bus.NewClient(config)` call and use its `Subscribe()` channel API.

**Rationale**: go-p2p-message-bus encapsulates ~2000 lines of production-tested P2P infrastructure including connection management, peer caching, NAT traversal, and mDNS discovery. Using it directly ensures network compatibility with Teranode peers and eliminates maintenance of bespoke libp2p wiring.

**Alternatives considered**:
- Keep raw libp2p: more control but duplicates solved problems, risks protocol drift with Teranode
- Wrap go-p2p-message-bus with a local interface: unnecessary indirection given the stable API

### 2. Network configuration via topic prefix and protocol version

**Decision**: Derive topic names and protocol version from a `Network` config field matching Teranode's pattern:
- Topics: `{network}-block`, `{network}-subtree` (e.g., `mainnet-block`)
- Protocol: `/teranode/bitcoin/{network}/1.0.0`

**Rationale**: This is exactly how Teranode isolates networks. Using the same pattern ensures merkle-service peers join the correct GossipSub mesh and exchange messages only with same-network Teranode nodes.

**Alternatives considered**:
- Custom topic naming: would break interoperability with Teranode

### 3. Default to DHT mode "off" for merkle-service

**Decision**: Default `DHTMode` to `"off"`. Users can override to `"server"` or `"client"` if needed.

**Rationale**: Teranode recommends `"off"` for lightweight clients that only need topic subscription. In off mode, the client connects to bootstrap peers directly and discovers topic peers via GossipSub mesh — sufficient for receiving subtree/block messages. This avoids the overhead of DHT crawling and the 100+ IPFS connections that server/client mode introduces.

**Alternatives considered**:
- Default to "client": still crawls DHT network, high overhead for a subscriber
- Default to "server": makes merkle-service a DHT node, unnecessary responsibility

### 4. Ed25519 private key management

**Decision**: Support three key sources in priority order: (1) `P2P_PRIVATE_KEY` env var (hex-encoded), (2) key file in `PeerCacheDir`, (3) auto-generate and persist.

**Rationale**: Matches Teranode's pattern. Auto-generation ensures zero-config local dev. File persistence ensures stable peer identity across restarts. Env var override enables deployment automation.

### 5. Expand P2PConfig, preserve backward compatibility

**Decision**: Add new fields to `P2PConfig` with sensible defaults so existing configs continue to work. The `SubtreeTopic` and `BlockTopic` fields become topic suffixes (default `"subtree"`, `"block"`) that are prefixed with the network name at runtime.

**Rationale**: Existing configs that set `SubtreeTopic: "teranode-subtree"` will need to change to `SubtreeTopic: "subtree"` (or just use the default). This is a minor migration but aligns with Teranode's convention.

## Risks / Trade-offs

**[go-p2p-message-bus version coupling]** — Merkle-service becomes coupled to the go-p2p-message-bus version, which may evolve independently.
→ Mitigation: Pin to the same version Teranode uses (v0.1.10). Update when Teranode updates.

**[Bootstrap peer dependency]** — In DHT-off mode, the client ONLY connects to bootstrap peers. If all bootstrap peers are down, no messages are received.
→ Mitigation: Configure multiple bootstrap peers. Peer cache file preserves previously-discovered peers for faster reconnection.

**[Topic naming compatibility]** — If Teranode changes topic naming convention, merkle-service must update.
→ Mitigation: Topic suffixes are configurable, so operators can adapt without code changes.

## Migration Plan

1. Update `go.mod` to add `go-p2p-message-bus` dependency
2. Expand `P2PConfig` with new fields and defaults
3. Rewrite `internal/p2p/client.go` to use go-p2p-message-bus
4. Update tests
5. Update entry points if constructor signatures change
6. Update docker-compose and docs with new config options

**Rollback**: The previous raw-libp2p implementation can be restored from git history.

## Open Questions

1. **Which go-p2p-message-bus version?** Plan: use v0.1.10 (same as Teranode). Confirm this resolves with our existing go.mod dependencies.
2. **Default bootstrap peers per network?** Teranode configures these per deployment. We should document required bootstrap peers but not hardcode any.
