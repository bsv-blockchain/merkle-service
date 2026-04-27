## Why

The p2p-client deployed on EKS is not receiving messages from the p2p topic. The root cause is that the `go-p2p-message-bus` `DHTMode` defaults to `"server"` when unconfigured, causing the pod to attempt DHT crawling (connecting to 100+ peers) while advertising a private RFC1918 pod IP that external peers cannot reach — the GossipSub mesh drops unreachable peers, so the pod receives nothing. Additionally, the pod's libp2p identity is stored on an `emptyDir` volume and regenerated on every restart, making it an unknown peer each time.

## What Changes

- Expose `MsgBus` configuration fields in `P2PConfig` (and corresponding `config.go` defaults and env bindings): `DHTMode`, `Port`, `AnnounceAddrs`, `BootstrapPeers`, `MaxConnections`, `MinConnections`, `EnableNAT`, `EnableMDNS`
- In `internal/p2p/client.go`, forward all `MsgBus` fields from `P2PConfig` when constructing the `p2p.Config` passed to `Initialize()`
- Set production-safe defaults: `dhtMode: "off"`, `enableNAT: false`, `enableMDNS: false`
- Add a fixed `port` default (9905) so K8s Services and NetworkPolicies can target a stable port
- Replace the `emptyDir` p2p-data volume in the K8s Deployment with a `PersistentVolumeClaim` so the libp2p private key (and thus peer ID) survives pod restarts
- Update the K8s ConfigMap to set `dhtMode: off` and document the `announceAddrs` field for operators

## Capabilities

### New Capabilities

- `p2p-eks-config`: Expose and configure the full `MsgBus` option set in `P2PConfig`, with EKS-appropriate defaults and persistent identity via PVC

### Modified Capabilities

- `p2p-client-integration`: The p2p client initialization now forwards MsgBus options; the K8s Deployment gains a PVC for p2p-data

## Impact

- `internal/config/config.go`: `P2PConfig` struct gains new fields; `registerDefaults` and `bindEnvVars` updated
- `internal/p2p/client.go`: `Start()` constructs `p2p.Config` with all `MsgBus` fields from config
- `deploy/k8s/p2p-client.yaml`: `emptyDir` replaced with PVC; container port declared
- `deploy/k8s/configmap.yaml`: `p2p` section updated with `dhtMode`, `port`, and `announceAddrs` docs
- No Kafka, Aerospike, or API changes
