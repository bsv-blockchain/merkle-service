## Why

The merkle-service currently uses `go-p2p-message-bus` directly, manually constructing topic names and requiring bootstrap peers to be configured externally. The `go-teranode-p2p-client` package wraps `go-p2p-message-bus` and provides the correct Teranode topic naming conventions, embedded bootstrap peers per network, typed message subscriptions, and automatic P2P identity management — eliminating configuration error and ensuring compatibility with the Teranode P2P network.

## What Changes

- Replace direct `go-p2p-message-bus` usage with `go-teranode-p2p-client` as the P2P client layer
- Remove manual topic name construction (`buildTopicName`, `buildProtocolVersion`) in favor of the client's built-in topic management
- Use typed subscription methods (`SubscribeBlocks`, `SubscribeSubtrees`) instead of raw `Subscribe(topicName)`
- Simplify `P2PConfig` — remove fields that the p2p-client handles internally (bootstrap peers, topic suffixes, DHT mode, NAT, mDNS, private IPs)
- Leverage the p2p-client's `Config.Initialize()` pattern for client creation and key management
- Remove custom `slogAdapter` — the p2p-client uses `slog` natively

## Capabilities

### New Capabilities
- `p2p-client-integration`: Integration with `go-teranode-p2p-client` for network-aware P2P connectivity with correct topics, bootstrap peers, and typed message subscriptions

### Modified Capabilities

## Impact

- **Dependencies**: Add `github.com/bsv-blockchain/go-teranode-p2p-client`; direct `go-p2p-message-bus` import removed from our code (still a transitive dependency)
- **Code**: `internal/p2p/client.go` — major refactor; `internal/p2p/key.go` — likely removable (p2p-client manages keys); `internal/config/config.go` — simplified P2P config
- **Configuration**: `P2P_BOOTSTRAP_PEERS`, `P2P_SUBTREE_TOPIC`, `P2P_BLOCK_TOPIC`, `P2P_DHT_MODE`, `P2P_ENABLE_NAT`, `P2P_ENABLE_MDNS`, `P2P_ALLOW_PRIVATE_IPS` become unnecessary (handled by the library). **BREAKING** for anyone setting these explicitly.
- **Tests**: `internal/p2p/client_test.go` needs updating for new API surface
