# Private and regtest networks

merkle-service can join a closed network — regtest, a local Arcade
end-to-end test, or any private deployment. The shipped
[config.yaml](../config.yaml) is a `teratestnet` sample with an explicit
bootstrap peer under `p2p.msgbus`. Any other `p2p.network` string is
accepted. It does not come with bootstrap peers.

The keys below are the ones `internal/config` actually loads, and they must
sit under `p2p.msgbus`. Viper lowercases keys and does not strip underscores,
so `dhtMode`, `DhtMode`, and `dhtmode` all bind. `dht_mode` and
`bootstrap_peers` do not, and the built-in default is left in place. The
same names placed directly under `p2p` (no `msgbus` parent) are also ignored.

## The network name is a topic namespace

`p2p.network` (`P2P_NETWORK`) is a string, not an enum. A blank value
defaults to `main` (the viper default, and again in `internal/p2p/client.go`
before the client starts). Any other string is passed through.

[go-teranode-p2p-client](https://github.com/bsv-blockchain/go-teranode-p2p-client)
v0.3.2 rewrites a few aliases when it builds gossipsub topic names, and
uses every other string unchanged. Topics look like
`teranode/bitcoin/1.0.0/{name}-block` (and `-subtree`, `-rejected-tx`,
`-node_status`).

| `p2p.network`             | Topic name    |
|---------------------------|---------------|
| `main`, `mainnet`         | `mainnet`     |
| `test`, `testnet`         | `testnet`     |
| `stn`                     | `stn`         |
| `teratest`, `teratestnet` | `teratestnet` |
| `regtest`                 | `regtest`     |
| anything else             | that string   |

Every peer that should exchange announcements must land on the same topic
name. `main` and `mainnet` do. `regtest` and a custom name do not.

## Bootstrap peers, with DHT off

When `bootstrapPeers` is empty, go-teranode-p2p-client v0.3.2 looks up
`getDefaultBootstrapPeers`. That map has three keys: `main`, `test`, and
`stn`. The `stn` value is `/dnsaddr/teratestnet.bootstrap.teranode.bsvb.tech`,
which does not match the `stn` topic. `teratestnet` is not a key, so an
empty list on that network is not a Teranode bootstrap list. The topic
aliases above do not apply here either: `main` and `mainnet` share the
`mainnet` topic, but only the raw value `main` selects the mainnet bootstrap
host. Do not treat the `stn` map entry as the way to join teratestnet. The
shipped config sets the teratestnet dnsaddr explicitly. Changing the
upstream map is a separate fix.

`p2p.msgbus.dhtMode` defaults to `off`. In that mode
[go-p2p-message-bus](https://github.com/bsv-blockchain/go-p2p-message-bus)
v0.2.0 does not create a DHT. The node dials its bootstrap peers and learns
other topic peers through the GossipSub mesh and the peer-address exchange.
A peer with no public address stays on a relay circuit through a bootstrap
peer. On a local regtest where each process can dial the others directly,
list those peers and leave DHT off.

Hole punching is not a DHT setting. `EnableHolePunching` is added only when
`enableNAT` is true, together with UPnP/NAT-PMP. The library also notes that
DHT-based hole punching is unavailable while DHT mode is `off`; turning DHT
on does not by itself enable hole punching.

If `bootstrapPeers` is empty, the message bus falls back to the public IPFS
bootstrap peers. `DisableDefaultBootstrapPeers` would stop that fallback,
but merkle-service does not expose the flag. On a closed network, set
`p2p.msgbus.bootstrapPeers` (`P2P_BOOTSTRAP_PEERS`, comma-separated) to the
peers you actually want. List each peer on the other. With DHT off, nothing
else introduces the first peer.

`StaticPeers` on the message-bus `Config` is the "always dial these, and
treat them as publishers" list. merkle-service does not copy that field
onto the client, so it cannot be set from this service's config. Use
`bootstrapPeers`.

Give each process its own `p2p.storagePath` (`P2P_STORAGE_PATH`). The
library writes `p2p_key.hex` there and logs the resulting id as `peer_id`
at startup. Put that id in the other peers' multiaddrs
(`/ip4/127.0.0.1/tcp/9906/p2p/<peer-id>` or
`/dns4/host/tcp/9905/p2p/<peer-id>`). The default listen port is `9905`
(`p2p.msgbus.port` / `P2P_PORT`); a second process on the same host needs
another port.

`enableMDNS` and `enableNAT` both default to false, which is what a closed
network wants:

- `enableMDNS` turns on LAN multicast discovery. It does not replace
  bootstrap peers. Leave it false unless the hosts share an isolated LAN.
- `enableNAT` turns on UPnP/NAT-PMP, NAT service, and libp2p hole punching,
  and scans the local gateway. Leave it false.

## Private IP addresses

Two different checks apply, and only one of them is tied to the network name.

The message-bus connection gater rejects loopback and RFC1918 dials unless
`AllowPrivateIPs` is set. go-teranode-p2p-client sets that flag only when
`p2p.network` is exactly `regtest`. Any other name, including a custom
private name, keeps the default and will not dial `127.0.0.1` or `10.0.0.0/8`.
merkle-service has no config key for this flag. Use `regtest` when the
peers themselves are on loopback or a private address.

HTTP URLs are a separate guard. Announced DataHub URLs and callback URLs
that point at loopback, link-local, or RFC1918 addresses are rejected unless
you opt in:

```yaml
datahub:
  allowPrivateIPs: true    # DATAHUB_ALLOW_PRIVATE_IPS
callback:
  allowPrivateIPs: true    # CALLBACK_ALLOW_PRIVATE_IPS
```

Set those when the Teranode asset service or the Arcade callback is on
localhost or a private IP. They do not change who the libp2p host dials.

## Example

One node of a regtest mesh. Replace the multiaddr with the other peer's
listen address and `peer_id`. A second process uses a different `port` and
`storagePath`, and lists this node in return.

```yaml
p2p:
  network: regtest
  storagePath: ~/.merkle-service/p2p-regtest
  msgbus:
    dhtMode: off
    port: 9905
    bootstrapPeers:
      - "/dns4/bootstrap.example/tcp/9906/p2p/12D3KooW..."
    enableMDNS: false
    enableNAT: false

datahub:
  allowPrivateIPs: true
callback:
  allowPrivateIPs: true
```

The same settings as environment variables (highest priority):

```shell
P2P_NETWORK=regtest
P2P_STORAGE_PATH=~/.merkle-service/p2p-regtest
P2P_DHT_MODE=off
P2P_PORT=9905
P2P_BOOTSTRAP_PEERS=/dns4/bootstrap.example/tcp/9906/p2p/12D3KooW...
P2P_ENABLE_MDNS=false
P2P_ENABLE_NAT=false
DATAHUB_ALLOW_PRIVATE_IPS=true
CALLBACK_ALLOW_PRIVATE_IPS=true
```

## Other message-bus options

merkle-service forwards only these `p2p.msgbus` fields to the library:
`dhtMode`, `port`, `announceAddrs`, `bootstrapPeers`, `maxConnections`,
`minConnections`, `enableNAT`, `enableMDNS`. A zero `maxConnections` or
`minConnections` leaves the library defaults (35 and 25).

Everything else — `StaticPeers`, `DisableDefaultBootstrapPeers`,
`AllowPrivateIPs`, `AllowedPublisherIDs`, peer cache, scoring — lives on
`Config` in go-p2p-message-bus v0.2.0:

<https://github.com/bsv-blockchain/go-p2p-message-bus/blob/v0.2.0/config.go>

Those fields are not read from merkle-service config. `announceAddrs`
(`P2P_ANNOUNCE_ADDRS`) is the list of multiaddrs this node advertises when
its reachable address differs from the address it listens on. Leave it
empty when peers dial the listen address directly.
