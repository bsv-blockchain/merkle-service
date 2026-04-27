## 1. Extend P2PConfig with MsgBus fields

- [x] 1.1 Add `P2PMsgBusConfig` sub-struct to `internal/config/config.go` with fields: `DHTMode string`, `Port int`, `AnnounceAddrs []string`, `BootstrapPeers []string`, `MaxConnections int`, `MinConnections int`, `EnableNAT bool`, `EnableMDNS bool`
- [x] 1.2 Add `MsgBus P2PMsgBusConfig` field to `P2PConfig` struct with `yaml:"msgbus" mapstructure:"msgbus"` tags
- [x] 1.3 Add defaults in `registerDefaults()`: `p2p.msgbus.dhtmode = "off"`, `p2p.msgbus.port = 9905`, `p2p.msgbus.enablenat = false`, `p2p.msgbus.enablemdns = false`
- [x] 1.4 Add env var bindings in `bindEnvVars()`: `P2P_DHT_MODE`, `P2P_PORT`, `P2P_ANNOUNCE_ADDRS`, `P2P_BOOTSTRAP_PEERS`, `P2P_MAX_CONNECTIONS`, `P2P_MIN_CONNECTIONS`, `P2P_ENABLE_NAT`, `P2P_ENABLE_MDNS`

## 2. Wire MsgBus config into p2p client initialization

- [x] 2.1 In `internal/p2p/client.go` `Start()`, populate `p2pCfg.MsgBus` from `c.cfg.MsgBus` fields: set `DHTMode`, `Port`, `AnnounceAddrs`, `BootstrapPeers`, `MaxConnections`, `MinConnections`, `EnableNAT`, `EnableMDNS`
- [x] 2.2 Log the effective MsgBus settings at startup (DHTMode, Port) alongside existing peerID/network log

## 3. Config tests

- [x] 3.1 Add test in `internal/config/config_test.go` verifying default `P2P.MsgBus.DHTMode = "off"`
- [x] 3.2 Add test verifying default `P2P.MsgBus.Port = 9905`
- [x] 3.3 Add test verifying `P2P_DHT_MODE` env var overrides the default
- [x] 3.4 Add test verifying `EnableNAT` and `EnableMDNS` default to false

## 4. p2p client unit tests

- [x] 4.1 Add test in `internal/p2p/client_test.go` verifying that `Init` succeeds when MsgBus fields are zero-value (relying on defaults)
- [x] 4.2 Add test verifying that the `P2PMsgBusConfig` fields set on `P2PConfig` are forwarded into the `p2p.Config.MsgBus` struct constructed in `Start()`

## 5. Kubernetes Deployment — PVC for p2p identity

- [x] 5.1 Add a `PersistentVolumeClaim` manifest named `p2p-data` to `deploy/k8s/p2p-client.yaml` with `ReadWriteOnce` access mode and `100Mi` storage request
- [x] 5.2 Replace the `emptyDir` volume entry in the `spec.volumes` section with a `persistentVolumeClaim` referencing `p2p-data`
- [x] 5.3 Verify the existing `volumeMounts` entry (`mountPath: /data/p2p`) is unchanged (no mount path change needed)

## 6. Kubernetes Deployment — container port

- [x] 6.1 Add `ports` entry to the container spec in `deploy/k8s/p2p-client.yaml`: `name: p2p`, `containerPort: 9905`, `protocol: TCP`

## 7. Kubernetes ConfigMap

- [x] 7.1 Add `msgbus` sub-section under `p2p` in `deploy/k8s/configmap.yaml` with `dhtMode: off` and `port: 9905`
- [x] 7.2 Add a YAML comment in the configmap documenting `announceAddrs` for operators who want to make the pod reachable without relay (e.g., when assigning a LoadBalancer IP)

## 8. Verification

- [x] 8.1 Run `go build ./...` to confirm no compile errors
- [x] 8.2 Run `go test ./internal/config/...` and `go test ./internal/p2p/...` to confirm all tests pass
