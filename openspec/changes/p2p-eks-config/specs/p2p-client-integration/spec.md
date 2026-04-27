## MODIFIED Requirements

### Requirement: P2P configuration is simplified
The `P2PConfig` struct SHALL contain `Network`, `StoragePath`, and a `MsgBus` sub-struct exposing the underlying message bus options (`DHTMode`, `Port`, `AnnounceAddrs`, `BootstrapPeers`, `MaxConnections`, `MinConnections`, `EnableNAT`, `EnableMDNS`). The `go-teranode-p2p-client` library SHALL still manage topic names and bootstrap peer defaults (when `BootstrapPeers` is empty). All exposed fields SHALL have production-safe defaults.

#### Scenario: Minimal configuration still works
- **WHEN** the P2P client is configured with only `Network` and `StoragePath`
- **THEN** the client initializes successfully using the safe defaults: `DHTMode = "off"`, `Port = 9905`, `EnableNAT = false`, `EnableMDNS = false`

#### Scenario: Default network is mainnet
- **WHEN** `Network` is not explicitly configured
- **THEN** the P2P client defaults to `"main"` (mainnet)

#### Scenario: MsgBus fields override library defaults
- **WHEN** any `P2PConfig.MsgBus` field is set to a non-zero value
- **THEN** that value is passed to `p2p.Config.MsgBus` and takes precedence over the library's own defaults
