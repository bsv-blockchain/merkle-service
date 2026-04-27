## 1. Dependencies & Configuration

- [x] 1.1 Add `github.com/bsv-blockchain/go-p2p-message-bus` to `go.mod` and run `go mod tidy`
- [x] 1.2 Expand `P2PConfig` in `internal/config/config.go` with new fields: `Network` (string, default "mainnet"), `Name` (string, default "merkle-service"), `PrivateKey` (string), `PeerCacheDir` (string), `Port` (int, default 9906), `AnnounceAddrs` ([]string), `DHTMode` (string, default "off"), `EnableNAT` (bool), `EnableMDNS` (bool), `AllowPrivateIPs` (bool)
- [x] 1.3 Add environment variable overrides for new P2P config fields: `P2P_NETWORK`, `P2P_NAME`, `P2P_PRIVATE_KEY`, `P2P_PEER_CACHE_DIR`, `P2P_PORT`, `P2P_ANNOUNCE_ADDRS`, `P2P_DHT_MODE`, `P2P_ENABLE_NAT`, `P2P_ENABLE_MDNS`, `P2P_ALLOW_PRIVATE_IPS`
- [x] 1.4 Update `SubtreeTopic` and `BlockTopic` defaults to topic suffixes ("subtree", "block") instead of full topic names

## 2. Private Key Management

- [x] 2.1 Implement `loadOrGeneratePrivateKey(cfg P2PConfig)` in `internal/p2p/key.go`: load from env var (hex-encoded), load from file in PeerCacheDir, or auto-generate and persist
- [x] 2.2 Write unit tests for key loading: from hex string, from file, auto-generation

## 3. P2P Client Rewrite

- [x] 3.1 Rewrite `internal/p2p/client.go`: replace libp2p/DHT/PubSub fields with a single `p2pClient p2pMessageBus.Client` field
- [x] 3.2 Implement network-aware topic naming: `buildTopicName(network, suffix)` returns `{network}-{suffix}`
- [x] 3.3 Implement protocol version construction: `buildProtocolVersion(network)` returns `/teranode/bitcoin/{network}/1.0.0`
- [x] 3.4 Rewrite `Init()`: build `p2pMessageBus.Config` from P2PConfig, call `loadOrGeneratePrivateKey`, validate config
- [x] 3.5 Rewrite `Start()`: call `p2pMessageBus.NewClient(config)`, subscribe to subtree and block topics via `client.Subscribe()`, launch goroutines to read from channels and publish to Kafka
- [x] 3.6 Rewrite `Stop()`: cancel context, wait for goroutines, call `client.Close()`
- [x] 3.7 Rewrite `Health()`: use `client.GetPeers()` to report peer count and connectivity status
- [x] 3.8 Preserve `publishWithRetry` logic for Kafka publish failures
- [x] 3.9 Update message processing goroutines to use `p2pMessageBus.Message` (access `.Data` for payload, `.FromID` for sender)

## 4. Remove Direct libp2p Dependencies

- [x] 4.1 Remove direct imports of `go-libp2p`, `go-libp2p-kad-dht`, `go-libp2p-pubsub`, `go-multiaddr` from `internal/p2p/client.go`
- [x] 4.2 Run `go mod tidy` to clean up transitive dependencies

## 5. Update Tests

- [x] 5.1 Update `internal/p2p/client_test.go` to test new Client API (Init validation, topic naming, protocol version construction, message handling)
- [x] 5.2 Write unit tests for `buildTopicName` and `buildProtocolVersion` across all networks (mainnet, testnet, teratestnet)
- [x] 5.3 Update config tests in `internal/config/config_test.go` for new P2PConfig defaults and env overrides

## 6. Update Entry Points

- [x] 6.1 Update `cmd/merkle-service/main.go` if P2P client constructor signature changed
- [x] 6.2 Update `cmd/p2p-client/main.go` if P2P client constructor signature changed

## 7. Documentation

- [x] 7.1 Update `docs/deployment.md` with new P2P configuration options (network, DHT mode, private key, peer cache, etc.)
- [x] 7.2 Add environment variable reference for all new P2P settings
