## Why

The current P2P client in merkle-service manually manages libp2p, DHT, and GossipSub — duplicating logic that Teranode already packages in `go-p2p-message-bus`. This library provides a production-hardened, channel-based P2P client with secure defaults (NAT traversal, connection gating, peer caching, mDNS), DHT mode selection, and automatic peer management. Reusing it eliminates ~400 lines of bespoke libp2p wiring, ensures network compatibility with Teranode peers, and adds network configurability (mainnet, testnet, teratestnet) that the current implementation lacks.

## What Changes

- Replace the hand-rolled libp2p/DHT/GossipSub setup in `internal/p2p/client.go` with `go-p2p-message-bus.NewClient()`
- Use the `Client.Subscribe(topic)` channel-based API instead of managing pubsub subscriptions directly
- Add network configuration (mainnet, testnet, teratestnet) using topic prefixes and protocol version strings matching Teranode's pattern (`{topicPrefix}-block`, `{topicPrefix}-subtree`)
- Add Ed25519 private key management (generate, load from file/env, persist to peer cache directory)
- Expand `P2PConfig` with go-p2p-message-bus configuration fields: `Network`, `PrivateKey`, `PeerCacheDir`, `DHTMode`, `EnableNAT`, `EnableMDNS`, `AllowPrivateIPs`, `Port`, `AnnounceAddrs`, `ProtocolVersion`
- Remove direct dependencies on `go-libp2p`, `go-libp2p-kad-dht`, `go-libp2p-pubsub`, `go-multiaddr` from the P2P client (these become transitive via go-p2p-message-bus)

## Capabilities

### New Capabilities

- `p2p-message-bus`: Replace direct libp2p usage with go-p2p-message-bus Client for P2P connectivity, topic subscription, peer discovery, and network isolation by chain (mainnet/testnet/teratestnet)

### Modified Capabilities

- `p2p-ingestion`: P2P client now uses go-p2p-message-bus instead of raw libp2p; subscription model changes from pubsub.Subscription to channel-based Message receive; network-aware topic naming

## Impact

- **Modified code**: `internal/p2p/client.go` (major rewrite), `internal/config/config.go` (expanded P2PConfig)
- **Dependencies**: Add `github.com/bsv-blockchain/go-p2p-message-bus`; direct imports of `go-libp2p`, `go-libp2p-kad-dht`, `go-libp2p-pubsub`, `go-multiaddr` become transitive only
- **Tests**: `internal/p2p/client_test.go` must be updated for new Client API
- **Entry points**: `cmd/p2p-client/main.go` and `cmd/merkle-service/main.go` may need minor updates for new config fields
- **Config**: New environment variables for P2P settings (network, private key, DHT mode, etc.)
