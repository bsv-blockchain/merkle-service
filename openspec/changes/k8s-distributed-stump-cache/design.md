## Context

The `kafka-stump-dedup-scaling` change introduces:
1. **STUMP reference passing** — Kafka messages carry a `StumpRef` string instead of the full ~365 KB STUMP binary
2. **Subtree work fan-out** — Block processor publishes to a `subtree-work` Kafka topic; independent `SubtreeWorkerService` consumers process subtrees
3. **In-process STUMP cache** — `sync.Map` shared between subtree worker and delivery service

Problem: #3 only works when all components share a process. In Kubernetes, where each subtree worker and delivery service runs in a separate pod, the in-process cache cannot be shared. The subtree worker in pod A writes a STUMP, but the delivery service in pod B has no way to read it.

### Target Kubernetes Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│ Kubernetes Cluster                                                  │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐           │
│  │ Block        │   │ Block        │   │ ...          │           │
│  │ Processor    │   │ Processor    │   │              │           │
│  │ Pod (1-3)    │   │ Pod          │   │              │           │
│  └──────┬───────┘   └──────────────┘   └──────────────┘           │
│         │ publish SubtreeWorkMessages                               │
│         ▼                                                           │
│  ┌──────────────────────────────────────────────────┐              │
│  │ Kafka: subtree-work topic (256+ partitions)       │              │
│  └──────────────────────────────────────────────────┘              │
│         │ consumed by                                               │
│         ▼                                                           │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐           │
│  │ Subtree      │   │ Subtree      │   │ Subtree      │           │
│  │ Worker Pod   │   │ Worker Pod   │   │ Worker Pod   │           │
│  │ (1 of N)     │   │ (2 of N)     │   │ (N of N)     │           │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘           │
│         │ write STUMP to Aerospike                                  │
│         │ publish slim MINED msgs to stumps topic                   │
│         ▼                                                           │
│  ┌──────────────────────────────────────────────────┐              │
│  │ Aerospike: stump_cache set (shared, TTL-evicted)  │              │
│  └──────────────────────────────────────────────────┘              │
│         │ read STUMP by StumpRef                                    │
│         ▼                                                           │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐           │
│  │ Delivery     │   │ Delivery     │   │ Delivery     │           │
│  │ Service Pod  │   │ Service Pod  │   │ Service Pod  │           │
│  │ (1 of M)     │   │ (2 of M)     │   │ (M of M)     │           │
│  └──────────────┘   └──────────────┘   └──────────────┘           │
│                                                                     │
│  Scaling: N subtree workers = Kafka partitions (256+)               │
│           M delivery pods = stumps topic partitions                 │
└────────────────────────────────────────────────────────────────────┘
```

## Goals / Non-Goals

**Goals:**
- STUMP cache works across pod boundaries in Kubernetes
- Subtree worker pods scale to 256+ via Kafka consumer group rebalancing
- Delivery service pods scale independently via stumps topic partitions
- Single configuration switch (`stumpCacheMode: aerospike`) enables distributed mode
- Local LRU cache reduces Aerospike read amplification (same STUMP read N times by delivery workers)
- Kubernetes manifests for all services as separate Deployments
- All-in-one mode continues to work with in-process cache (`stumpCacheMode: memory`)

**Non-Goals:**
- Helm charts or operator patterns (raw manifests are sufficient for now)
- Auto-scaling (HPA) configuration — that depends on cluster-specific metrics
- Multi-cluster or geo-distributed deployment
- Service mesh (Istio/Linkerd) integration
- Changing Kafka or Aerospike deployment topology

## Decisions

### 1. StumpCache interface with pluggable backends

Define a `StumpCache` interface with two implementations:

```go
type StumpCache interface {
    Put(subtreeHash, blockHash string, data []byte) error
    Get(subtreeHash, blockHash string) ([]byte, bool, error)
    Close() error
}
```

- **`MemoryStumpCache`**: `sync.Map` with background TTL sweep. Used in all-in-one/dev mode. Zero external dependencies.
- **`AerospikeStumpCache`**: Writes to Aerospike with TTL; reads through a local LRU cache. Used in K8s/production mode.

Selected by config: `callback.stumpCacheMode: memory | aerospike`.

**Rationale:** Interface pattern allows the prior change's code to work unchanged — only the wiring in `main.go` changes based on config. Tests can use the memory implementation. Production K8s uses Aerospike.

**Alternative considered:** Always use Aerospike — simpler to maintain one implementation, but adds Aerospike latency even in all-in-one mode where it's unnecessary. The memory implementation is trivial to maintain.

### 2. Aerospike-backed STUMP cache with TTL

Store STUMPs in a dedicated Aerospike set (`stump_cache`) with TTL-based eviction:

```
Set:    stump_cache
Key:    "subtreeHash:blockHash"
Bin:    "data" → []byte (raw STUMP binary)
TTL:    300 seconds (5 minutes, configurable)
```

**Write path** (subtree worker):
1. Build STUMP from subtree data
2. `cache.Put(subtreeHash, blockHash, stumpData)` → Aerospike `Put` with TTL
3. Publish MINED messages with `StumpRef` to stumps topic

**Read path** (delivery service):
1. Receive StumpsMessage with `StumpRef`
2. `cache.Get(subtreeHash, blockHash)` → check local LRU → miss → Aerospike `Get`
3. If found: build HTTP payload with STUMP data
4. If not found: re-enqueue for retry (existing mechanism)

**Rationale:** Aerospike already handles all persistent state in this system (registrations, seen counters, dedup records). Adding another set is natural. TTL-based eviction matches the pattern used by callback dedup (86400s TTL) and registration TTL (1800s post-mine). The STUMP cache TTL is shorter (300s) because STUMPs are ephemeral — only needed during the delivery window.

### 3. Local LRU read-through cache

The delivery service processes N callbacks per subtree, each resolving the same `StumpRef`. Without local caching, this means N Aerospike reads for the same key per subtree (e.g., 100 reads for 100 callbacks).

Add a local LRU cache (e.g., `hashicorp/golang-lru/v2`) in front of Aerospike reads:

```go
type AerospikeStumpCache struct {
    asClient  *store.AerospikeClient
    setName   string
    ttlSec    int
    localLRU  *lru.Cache[string, []byte]  // capacity: 1024 entries
}
```

**Read logic:**
1. Check local LRU (`O(1)`, in-process)
2. On miss: read from Aerospike, store in LRU
3. LRU eviction by size (1024 entries ≈ 1024 × 271 KB ≈ 270 MB max memory)

**Rationale:** Reduces Aerospike read amplification by ~100× (from N reads per subtree to 1 read per subtree per delivery pod). 270 MB memory is acceptable for production pods. The LRU naturally evicts old entries as new blocks arrive.

**Alternative considered:** No local cache — simpler but 100× more Aerospike reads. At 25,000 callbacks and 250 subtrees, that's 25,000 Aerospike reads vs 250 with LRU. Aerospike can handle it, but it's wasteful.

### 4. Kubernetes manifests structure

```
deploy/k8s/
  ├── namespace.yaml           # merkle-service namespace
  ├── configmap.yaml           # shared config.yaml mounted into all pods
  ├── block-processor.yaml     # Deployment: 1-3 replicas
  ├── subtree-worker.yaml      # Deployment: 16-256 replicas (HPA-ready)
  ├── callback-delivery.yaml   # Deployment: 4-32 replicas
  ├── api-server.yaml          # Deployment + Service: 2-4 replicas
  └── p2p-client.yaml          # Deployment: 1 replica (singleton)
```

Each manifest uses the same container image (`merkle-service`) with different CMD entrypoints (`cmd/block-processor`, `cmd/subtree-processor`, `cmd/callback-delivery`, etc.).

**Environment variables** override config.yaml for K8s-specific settings:
- `MODE=microservice`
- `CALLBACK_STUMP_CACHE_MODE=aerospike`
- `AEROSPIKE_HOST=aerospike-service.merkle-service.svc.cluster.local`
- `KAFKA_BROKERS=kafka-0.kafka.merkle-service.svc.cluster.local:9092`

**Subtree worker scaling:**
- `replicas` controls the number of pods
- Each pod consumes partitions from the `subtree-work` topic via Kafka consumer group
- Max useful replicas = partition count of `subtree-work` topic
- Document: set `subtreeWorkPartitions ≥ max replicas` at topic creation

### 5. Microservice entrypoints use same service.BaseService pattern

Each `cmd/` entrypoint already exists. They need to:
1. Create an `AerospikeStumpCache` (when `stumpCacheMode=aerospike`)
2. Inject it into the service being started
3. Everything else (Kafka consumer groups, producers, stores) already works per-service

The `cmd/subtree-processor/main.go` needs updating to become a `SubtreeWorkerService` consumer (currently it's the P2P subtree processor, not the block subtree worker).

**Decision:** Create a new `cmd/subtree-worker/main.go` entrypoint for the Kafka-based subtree work consumer, distinct from the existing `cmd/subtree-processor` (which handles P2P subtree announcements).

## Risks / Trade-offs

- **[Aerospike latency on write path]** Each subtree worker writes ~271 KB STUMP to Aerospike (one per subtree). At 250 subtrees per block, that's ~66 MB of writes per block → Aerospike handles this easily (designed for millions of ops/sec). Writes are non-blocking from the worker's perspective (write → publish MINED messages, don't wait for read confirmation).
- **[LRU memory pressure]** 1024 × 271 KB = ~270 MB max LRU size per delivery pod → Configurable capacity. For pods with tight memory limits, reduce LRU size. Cache misses go to Aerospike (still fast, ~1ms latency).
- **[New dependency: golang-lru]** Adds one external dependency → Well-maintained HashiCorp library, widely used in Go ecosystem. Minimal attack surface (pure Go, no CGo).
- **[Subtree worker pod scaling]** Must match Kafka partition count → Document the relationship. Default 256 partitions supports 256 concurrent workers. For blocks with 1000+ subtrees, increase partition count.
- **[Pod startup ordering]** Subtree workers must start after Kafka topics exist → Kafka auto-creates topics on first publish. Block processor publishes to `subtree-work` topic on first block, creating it. Workers that start before the topic simply wait for the consumer group to rebalance.
- **[All-in-one mode regression risk]** Changes to cache interface could break all-in-one mode → Memory implementation is a simple adapter, tested in unit tests. Both implementations conform to the same interface.
