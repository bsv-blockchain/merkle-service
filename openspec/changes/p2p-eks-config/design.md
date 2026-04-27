## Context

The p2p-client pod on EKS is not receiving messages from the GossipSub topic. Investigation reveals two compounding root causes:

1. **DHT mode defaults to "server"**: `go-p2p-message-bus` `Config.DHTMode` is empty-string by default, which the library interprets as `"server"` mode. In server mode, the node tries to connect to 100+ DHT peers and advertises itself for inbound connections. Because the pod only has a private RFC1918 IP with no `AnnounceAddrs` configured, other network peers cannot establish inbound connections to it. GossipSub scores unreachable peers negatively and prunes them from the mesh, so the pod stops receiving topic messages.

2. **Identity regenerated on pod restart**: The p2p private key is stored under `storagePath` (`/data/p2p`), which is backed by an `emptyDir` volume. Every pod restart generates a new peer ID. New/unknown peers are given low GossipSub scores and take longer to be admitted to the mesh — compounding the connectivity problem.

The `go-teranode-p2p-client` library already supports all needed configuration via its `msgbus.Config` embedded in `p2p.Config`, but `P2PConfig` in `internal/config/config.go` only exposes `Network` and `StoragePath`. The missing bridge is passing through the `MsgBus` options from application config to the library.

## Goals / Non-Goals

**Goals:**
- Fix the production outage: configure `DHTMode = "off"` so the pod operates as a lightweight gossip-only client that connects to known bootstrap servers via relay, regardless of inbound reachability
- Expose the relevant `MsgBus` fields in `P2PConfig` so operators can tune DHT mode, port, announce addresses, bootstrap peers, and connection limits through the standard config/env system
- Persist the p2p identity across pod restarts by replacing the `emptyDir` volume with a PVC
- Set a fixed listen port (9905) so K8s Services and NetworkPolicies have a stable target

**Non-Goals:**
- Making the pod publicly reachable via LoadBalancer or NodePort (not required when DHTMode is "off" with relay-capable bootstrap peers)
- Changing how topics, Kafka publishing, or message processing work
- Supporting multiple p2p-client replicas (singleton constraint is unchanged)

## Decisions

### Decision 1: DHTMode = "off" as the production default

**Chosen:** Set `p2p.msgbus.dhtMode = "off"` as the config default for all deployments.

**Rationale:** The library documentation explicitly states: *"RECOMMENDED for abuse-sensitive cloud hosting (Hetzner, OVH, etc.) to avoid network scanning alerts from DHT peer discovery connecting to 100+ IPs."* AWS EKS falls squarely in this category. With `"off"`, the node only connects to the configured bootstrap peers and discovers topic peers via GossipSub mesh; relay circuits through bootstrap servers handle NAT traversal so the pod can receive messages even with a private IP.

**Alternative considered:** `DHTMode = "client"` — rejected because the library notes it *"still participates in DHT routing"* and connects to 100+ peers. It reduces overhead vs server mode but is explicitly described as "generally not recommended."

**Alternative considered:** Configure `AnnounceAddrs` to point to a LoadBalancer IP — rejected as it adds a LoadBalancer cost and operational burden for a singleton service. Relay via bootstrap servers is sufficient for receive-only topic subscription.

### Decision 2: Expose MsgBus fields through P2PConfig (not Viper SetDefaults)

**Chosen:** Add `MsgBus` sub-struct fields to `P2PConfig` (`DHTMode`, `Port`, `AnnounceAddrs`, `BootstrapPeers`, `MaxConnections`, `MinConnections`, `EnableNAT`, `EnableMDNS`) and populate the library's `p2p.Config.MsgBus` from them in `client.Start()`.

**Rationale:** The codebase uses Viper for config loading but constructs `p2p.Config` directly as a struct. The library's `SetDefaults()` method applies defaults to a Viper instance — it is not called in the current flow. Adding struct fields maintains consistency with every other config section and allows env var overrides via the existing `bindEnvVars` mechanism.

**Alternative considered:** Call `p2p.Config.SetDefaults(viper.GetViper(), "p2p")` and load via Viper — rejected because it would mix two config systems for the same section and require threading a Viper instance into the p2p client.

### Decision 3: PVC for p2p identity persistence

**Chosen:** Replace `emptyDir` with a `PersistentVolumeClaim` (`ReadWriteOnce`, 100Mi) for the `/data/p2p` volume in the K8s Deployment.

**Rationale:** The libp2p private key and peer cache are stored in `storagePath`. Losing them on restart means every restart produces a new peer ID with zero GossipSub score, requiring re-admission to the mesh. A PVC on EKS uses EBS by default (or the cluster's default StorageClass), providing persistence across pod restarts and rescheduling within the same AZ.

**Alternative considered:** Inject the private key as a Kubernetes Secret — viable but more operationally complex (key must be pre-generated and managed separately). A PVC lets the library manage key generation/loading transparently.

**Trade-off:** The PVC binds the pod to the AZ where the EBS volume was created. For a singleton deployment this is acceptable; multi-AZ failover would require switching to a shared filesystem or the Secret approach.

### Decision 4: Fixed listen port 9905

**Chosen:** Default `p2p.port = 9905`, matching the port used by the Teranode bootstrap servers.

**Rationale:** A fixed port allows declaring a container port in the K8s Deployment and writing stable NetworkPolicy rules. Port 9905 is already used by the Teranode bootstrap servers in the library's default peer list, making it the natural choice.

## Risks / Trade-offs

- **AZ-locked PVC** → Mitigation: Document in README; for multi-AZ HA, operators can use an EFS StorageClass or the Secret injection approach.
- **DHTMode "off" relies on bootstrap server relay** → If bootstrap servers are unreachable, the pod cannot join the mesh. Mitigation: bootstrap peers are DNS-based hosted services maintained by BSV Blockchain; existing monitoring of Kafka message flow will surface relay failures.
- **Existing pods have no peer identity on restart** → On first restart after deploy, the pod gets a new peer ID (same as today). The improvement takes effect from the second restart onward, once the PVC holds the generated key.

## Migration Plan

1. Deploy updated ConfigMap with `dhtMode: off` and `port: 9905`
2. Apply updated Deployment that adds the PVC and removes the `emptyDir`
3. The pod restarts; on first start it generates a new key stored in the PVC and connects in "off" mode — expect a brief warm-up period (30–60s) while GossipSub mesh forms via bootstrap relay
4. Rollback: revert Deployment to previous manifest; the PVC remains but is unused (data is kept)
