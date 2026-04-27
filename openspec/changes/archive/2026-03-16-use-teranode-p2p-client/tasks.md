## 1. Add Dependency

- [x] 1.1 Run `go get github.com/bsv-blockchain/go-teranode-p2p-client` to add the package to go.mod
- [x] 1.2 Run `go mod tidy` to clean up direct/indirect dependency changes

## 2. Simplify P2P Configuration

- [x] 2.1 Update `P2PConfig` in `internal/config/config.go` to only contain `Network` and `StoragePath` fields
- [x] 2.2 Update `registerDefaults()` to set defaults for `p2p.network` ("main") and `p2p.storagepath` ("~/.merkle-service/p2p"); remove old defaults (bootstrappeers, subtreetopic, blocktopic, dhtmode, enablenat, enablemdns, allowprivateips, port, name)
- [x] 2.3 Update `bindEnvVars()` to bind only `P2P_NETWORK` and `P2P_STORAGE_PATH`; remove old env var bindings
- [x] 2.4 Update `internal/config/config_test.go` for new P2PConfig shape

## 3. Update Kafka Message Types to Announcement Format

- [x] 3.1 Update `kafka.SubtreeMessage` to match teranode announcement fields: Hash, DataHubURL, PeerID, ClientName
- [x] 3.2 Update `kafka.BlockMessage` to match teranode announcement fields: Hash, Height, Header, Coinbase, DataHubURL, PeerID, ClientName
- [x] 3.3 Update `kafka.messages_test.go` for new message shapes
- [x] 3.4 Update downstream consumers (`internal/block/processor.go`, `internal/subtree/processor.go`) to handle new message fields

## 4. Refactor P2P Client

- [x] 4.1 Update `Client` struct in `internal/p2p/client.go` to store a `*p2p.Client` (from go-teranode-p2p-client) instead of `p2pMessageBus.Client`; update `cfg` field type to the simplified `config.P2PConfig`
- [x] 4.2 Remove `buildTopicName()` and `buildProtocolVersion()` helper functions
- [x] 4.3 Remove `slogAdapter` struct and its methods
- [x] 4.4 Update `NewClient()` constructor to accept the simplified config
- [x] 4.5 Rewrite `Init()` to validate Kafka producers only (no topic/key validation — library handles those)
- [x] 4.6 Rewrite `Start()` to use `p2p.Config{Network, StoragePath}.Initialize(ctx, "merkle-service")`, then call `SubscribeSubtrees(ctx)` and `SubscribeBlocks(ctx)` for typed channels
- [x] 4.7 Rewrite message processing goroutines for typed channels (teranode.SubtreeMessage, teranode.BlockMessage)
- [x] 4.8 Rewrite `handleSubtreeMessage` to accept `teranode.SubtreeMessage`, map to `kafka.SubtreeMessage`, encode and publish
- [x] 4.9 Rewrite `handleBlockMessage` to accept `teranode.BlockMessage`, map to `kafka.BlockMessage`, encode and publish
- [x] 4.10 Update `Stop()` to call `Close()` on the `p2p.Client`
- [x] 4.11 Update `Health()` to use `p2p.Client.GetPeers()` for peer count

## 5. Remove Key Management

- [x] 5.1 Delete `internal/p2p/key.go` — key management is handled by the library's `Initialize()` via `StoragePath`

## 6. Update P2P Client Entry Point

- [x] 6.1 Update `cmd/p2p-client/main.go` to pass the simplified config to `p2p.NewClient()`

## 7. Update Tests

- [x] 7.1 Update `newTestClient` in `internal/p2p/client_test.go` for new constructor signature
- [x] 7.2 Update `handleSubtreeMessage` tests to pass `teranode.SubtreeMessage` structs instead of JSON `[]byte`
- [x] 7.3 Update `handleBlockMessage` tests to pass `teranode.BlockMessage` structs instead of JSON `[]byte`
- [x] 7.4 Remove tests for invalid/empty/nil JSON data (no longer applicable with typed messages)
- [x] 7.5 Update Init tests to reflect simplified validation (no topic validation)
- [x] 7.6 Update Health tests if struct fields changed
- [x] 7.7 Run `go test ./internal/p2p/...` and verify all tests pass

## 8. Update Config File

- [x] 8.1 Update `config.yaml` to reflect the simplified P2P config section with only `network` and `storagePath`

## 9. Build Verification

- [x] 9.1 Run `go build ./...` to verify the entire project compiles
- [x] 9.2 Run `go vet ./...` to check for issues
- [x] 9.3 Run full test suite `go test ./...` to verify no regressions
