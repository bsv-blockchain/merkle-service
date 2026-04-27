## Why

The `kafka-stump-dedup-scaling` change introduces a STUMP cache and Kafka-based subtree fan-out for horizontal scaling. However, its design uses an in-process `sync.Map` cache shared between the subtree worker and delivery service — which only works when both run in the same process (all-in-one mode). In a Kubernetes deployment where each subtree processor runs in a separate container/pod, and delivery services run in their own pods, an in-process cache cannot be shared across process boundaries.

To achieve the target of 1000+ concurrent subtree processors (one per subtree in a block) running across a Kubernetes cluster, the STUMP cache must be externalized to a shared store that all pods can access. Aerospike is already a core dependency and provides the required low-latency key-value operations with TTL-based eviction.

## What Changes

- **Replace in-process STUMP cache with Aerospike-backed distributed cache**: The `StumpCache` interface remains the same (`Put`/`Get` by `subtreeHash:blockHash`), but the implementation writes to Aerospike instead of `sync.Map`. This allows subtree worker pods to write STUMPs and delivery service pods to read them across process boundaries.
- **Add local in-process LRU cache layer**: To avoid hitting Aerospike for every STUMP lookup (delivery workers read the same STUMP N times, once per callback URL), add an in-process LRU read-through cache in front of Aerospike. First `Get` fetches from Aerospike and caches locally; subsequent `Get`s for the same key are served from memory.
- **Kubernetes deployment manifests and guidance**: Add Kubernetes Deployment/Service manifests for the subtree-worker, delivery service, and block processor as separate containers, all sharing the same Aerospike cluster and Kafka brokers.
- **Configurable deployment mode**: The `StumpCache` implementation is selected by config — `memory` for all-in-one/dev, `aerospike` for K8s/production. Both satisfy the same interface.
- **Update `kafka-stump-dedup-scaling` to use the `StumpCache` interface**: Ensure the prior change's tasks reference the interface, not a concrete `sync.Map` implementation.

## Capabilities

### New Capabilities
- `distributed-stump-cache`: Aerospike-backed STUMP cache with local LRU read-through, enabling cross-pod STUMP sharing in Kubernetes deployments
- `k8s-deployment`: Kubernetes manifests and configuration for running subtree workers, delivery services, and block processors as separate horizontally-scalable pods

### Modified Capabilities
- `stump-cache`: Cache interface updated to support both in-process and distributed backends; backend selected by configuration

## Impact

- **internal/store/stump_cache.go**: Refactored from `sync.Map` to interface with two implementations: `MemoryStumpCache` (dev/all-in-one) and `AerospikeStumpCache` (K8s/production)
- **internal/store/stump_cache_aerospike.go**: New Aerospike-backed implementation with local LRU read-through
- **internal/config/config.go**: New `StumpCacheMode` field (`memory` | `aerospike`)
- **config.yaml**: New stump cache configuration section
- **cmd/merkle-service/main.go**: Updated to create cache based on config mode
- **cmd/block-processor/main.go**, **cmd/callback-delivery/main.go**: Updated to accept distributed cache config
- **deploy/k8s/**: New directory with Kubernetes manifests (Deployments, Services, ConfigMap)
- **Aerospike**: New set for STUMP cache entries with TTL
