## ADDED Requirements

### Requirement: MsgBus options are configurable via P2PConfig
`P2PConfig` SHALL expose a `MsgBus` sub-struct containing: `DHTMode`, `Port`, `AnnounceAddrs`, `BootstrapPeers`, `MaxConnections`, `MinConnections`, `EnableNAT`, and `EnableMDNS`. These SHALL be mapped to the library's `msgbus.Config` fields when the p2p client is initialized.

#### Scenario: DHTMode forwarded to library
- **WHEN** `p2p.msgbus.dhtMode` is set to `"off"` in config
- **THEN** the `go-p2p-message-bus` client is created with `DHTMode = "off"`, disabling DHT crawling and enabling relay-based connectivity

#### Scenario: All MsgBus fields forwarded
- **WHEN** any `p2p.msgbus.*` config field is set
- **THEN** the corresponding `msgbus.Config` field is populated before calling `p2p.Config.Initialize()`

### Requirement: EKS-safe defaults for MsgBus options
The config defaults SHALL be safe for cloud-hosted EKS deployment: `DHTMode = "off"`, `Port = 9905`, `EnableNAT = false`, `EnableMDNS = false`. These prevent DHT crawling, random port assignment, and network scanning behavior that would be problematic on AWS.

#### Scenario: Default DHTMode is off
- **WHEN** no `p2p.msgbus.dhtMode` value is provided in config or environment
- **THEN** the p2p client starts with `DHTMode = "off"`

#### Scenario: Default port is 9905
- **WHEN** no `p2p.msgbus.port` value is provided
- **THEN** the p2p client listens on port 9905

#### Scenario: NAT and mDNS disabled by default
- **WHEN** no `p2p.msgbus.enableNAT` or `p2p.msgbus.enableMDNS` values are provided
- **THEN** the p2p client starts with `EnableNAT = false` and `EnableMDNS = false`

### Requirement: MsgBus options are configurable via environment variables
Each `p2p.msgbus.*` field SHALL be bindable through a corresponding environment variable: `P2P_DHT_MODE`, `P2P_PORT`, `P2P_ANNOUNCE_ADDRS`, `P2P_BOOTSTRAP_PEERS`, `P2P_MAX_CONNECTIONS`, `P2P_MIN_CONNECTIONS`, `P2P_ENABLE_NAT`, `P2P_ENABLE_MDNS`.

#### Scenario: DHT mode overridable via env var
- **WHEN** the `P2P_DHT_MODE` environment variable is set to `"server"`
- **THEN** the p2p client starts with `DHTMode = "server"`, overriding the default

#### Scenario: Announce addrs configurable via env var
- **WHEN** `P2P_ANNOUNCE_ADDRS` is set to a comma-separated list of multiaddr strings
- **THEN** the p2p client announces those addresses to peers

### Requirement: p2p-client Deployment uses a PVC for identity storage
The p2p-client K8s Deployment SHALL mount a `PersistentVolumeClaim` (not `emptyDir`) at `/data/p2p` so the libp2p private key and peer cache survive pod restarts and rescheduling.

#### Scenario: Identity persists across pod restart
- **WHEN** the p2p-client pod is restarted
- **THEN** the same peer ID is used on reconnection (the key file on the PVC is loaded)

#### Scenario: PVC is created with the Deployment
- **WHEN** the p2p-client Deployment manifest is applied
- **THEN** a `PersistentVolumeClaim` named `p2p-data` is created with `ReadWriteOnce` access and at least 100Mi capacity

### Requirement: p2p-client Deployment declares a container port
The p2p-client K8s Deployment SHALL declare the libp2p listen port (9905/TCP) as a container port so NetworkPolicy and observability tooling can target it.

#### Scenario: Container port declared
- **WHEN** the p2p-client pod is scheduled
- **THEN** the container spec includes a port named `p2p` on 9905/TCP
