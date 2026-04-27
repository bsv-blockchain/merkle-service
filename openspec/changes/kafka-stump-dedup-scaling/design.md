## Context

The callback delivery pipeline achieves ~22K txids/sec after adding a 256-worker pool with tuned HTTP transport. The bottleneck is now Kafka message volume: each MINED StumpsMessage carries the full STUMP binary (~365 KB base64-encoded), duplicated for every callback URL per subtree. For a 1M-tx block (250 subtrees × 100 callbacks), this is 9.1 GB through Kafka, limited by single-consumer throughput (~200 MB/s).

### Current Data Flow

```
Block Processor (16 goroutines)
  └─ per subtree: build STUMP, group txids by callback
       └─ per callback: publish StumpsMessage (365 KB each) to stumps topic
            └─ Kafka (9.1 GB total)
                 └─ Delivery Service (single consumer, 256 workers)
                      └─ HTTP POST (365 KB each) to callback URL
```

Two independent problems:
1. **STUMP duplication**: Same ~271 KB binary sent 100× through Kafka per subtree
2. **Block processor bottleneck**: Internal goroutine pool (16 workers) limits subtree parallelism; can't horizontally scale across machines

## Goals / Non-Goals

**Goals:**
- Reduce Kafka message size from ~365 KB to ~3.6 KB per MINED message (100× reduction)
- Enable horizontal scaling of subtree processing via Kafka fan-out
- Achieve 100K+ txids/sec delivery throughput on the mega scale test
- Maintain at-least-once delivery guarantees with existing dedup
- Keep backward compatibility during rollout (inline StumpData still works)

**Non-Goals:**
- Changing the BRC-0074 STUMP format or callback HTTP payload
- Distributed STUMP cache (in-process cache per delivery service instance is sufficient since the block processor and delivery service share a process in all-in-one mode, and in microservice mode each delivery instance caches its own STUMPs)
- HTTP/2 or gRPC transport for callback delivery
- Exactly-once delivery semantics

## Decisions

### 1. In-process STUMP cache with TTL eviction

Store each encoded STUMP once in an in-process `sync.Map`-backed cache, keyed by `subtreeHash:blockHash`. The block processor (or subtree worker) writes the STUMP to the cache after building it. The delivery service reads from the cache when resolving a reference.

```
Key:   "subtreeHash:blockHash"
Value: []byte (encoded STUMP binary, BRC-0074 format)
TTL:   5 minutes (covers delivery time + retry window)
```

**Rationale:** In all-in-one mode (the primary deployment), the block processor and delivery service share the same process, so an in-process cache provides zero-copy access. For microservice mode, the subtree worker and delivery service would also need to share the cache — but since the subtree worker publishes to the stumps Kafka topic which the delivery service consumes, the delivery service can populate its own cache from a separate Kafka message or shared store. We start with in-process for all-in-one and defer distributed caching.

**Alternative considered:** External cache (Redis/Aerospike) — adds latency and a new dependency. Not needed when all components share a process. Can be added later if microservice mode requires it.

**Alternative considered:** Blob store reference — the subtree store already stores raw subtree data, but STUMPs are derived (filtered merkle proofs for registered txids), not raw subtrees. Storing derived STUMPs in the blob store adds complexity and I/O.

### 2. StumpsMessage reference field

Add `StumpRef string` to `StumpsMessage`. When the block processor publishes a MINED message, it sets `StumpRef = "subtreeHash:blockHash"` and omits `StumpData`. The delivery service resolves the reference from the STUMP cache before building the HTTP payload.

```go
type StumpsMessage struct {
    // ... existing fields ...
    StumpData []byte  `json:"stumpData,omitempty"` // inline (backward compat)
    StumpRef  string  `json:"stumpRef,omitempty"`  // cache reference (new)
}
```

**Delivery resolution logic:**
1. If `StumpData` is non-empty → use directly (backward compat)
2. If `StumpRef` is non-empty → look up in STUMP cache
3. If cache miss → log warning, re-enqueue for retry (STUMP may arrive shortly)

**Rationale:** Backward-compatible — old messages with inline data still work. The reference is a simple string lookup, no complex protocol. Cache misses during startup or after eviction are handled by the existing retry mechanism.

### 3. Subtree work fan-out via Kafka

Replace the block processor's internal goroutine pool with Kafka-based fan-out:

```
Block Processor (coordinator)
  └─ Fetch block metadata (subtree list)
  └─ Publish N SubtreeWorkMessages to "subtree-work" topic (one per subtree)
       └─ Key: subtreeHash (distributes across partitions)

Subtree Workers (N consumers, horizontally scalable)
  └─ Consume SubtreeWorkMessage
  └─ Retrieve subtree data, check registrations, build STUMP
  └─ Store STUMP in cache
  └─ Publish slim MINED messages (with StumpRef) to stumps topic
  └─ Publish BLOCK_PROCESSED when all subtrees for a block are done
```

**New message type:**
```go
type SubtreeWorkMessage struct {
    BlockHash   string `json:"blockHash"`
    BlockHeight uint64 `json:"blockHeight"`
    SubtreeHash string `json:"subtreeHash"`
    DataHubURL  string `json:"dataHubURL"`
}
```

**Scaling model:**
- `subtree-work` topic with N partitions (e.g., 256)
- Each partition consumed by one subtree worker instance
- A block with 250 subtrees distributes across 250 partitions → 250 concurrent workers
- A block with 1000 subtrees distributes across 256 partitions → some queuing, still highly parallel

**Rationale:** Kafka consumer groups provide automatic partition rebalancing, fault tolerance, and horizontal scaling without custom coordination. The block processor becomes a lightweight coordinator that just dispatches work. Subtree workers are stateless and can scale independently.

**Alternative considered:** Keep internal goroutine pool, just increase default from 16 to 256 — limited to single machine, can't scale beyond process memory/CPU. Kafka fan-out enables multi-machine scaling.

### 4. BLOCK_PROCESSED coordination

With subtree processing distributed across workers, BLOCK_PROCESSED callbacks must be emitted only after ALL subtrees for a block are complete. Two approaches:

**Chosen: Atomic counter in Aerospike**
- Block processor writes `{blockHash: subtreeCount}` to Aerospike when dispatching work
- Each subtree worker decrements the counter after completing its subtree
- The worker that decrements to zero emits BLOCK_PROCESSED callbacks for all registered URLs
- Uses Aerospike CDT atomic `Increment(-1)` operation for thread safety

**Rationale:** Aerospike atomic operations are already used throughout the codebase. The counter is lightweight (one key per block) and naturally handles out-of-order completion. TTL on the counter handles cleanup for failed blocks.

**Alternative considered:** Kafka compacted topic for completion tracking — more complex, requires polling or additional consumer. Aerospike is simpler and already a dependency.

### 5. STUMP cache shared via delivery service injection

In all-in-one mode, the STUMP cache is a shared `*StumpCache` instance injected into both the subtree worker and the delivery service. The subtree worker writes STUMPs after building them; the delivery service reads them when resolving references.

```go
type StumpCache struct {
    entries sync.Map // key: "subtreeHash:blockHash" → value: *cacheEntry
}

type cacheEntry struct {
    data      []byte
    expiresAt time.Time
}
```

A background goroutine sweeps expired entries every 30 seconds. Default TTL: 5 minutes.

**Rationale:** `sync.Map` provides lock-free reads (common path) and is ideal for this write-once-read-many pattern. Each STUMP is written once by the subtree worker and read N times by delivery workers (once per callback URL).

### 6. Kafka topic configuration

Add new config fields:

```yaml
kafka:
  subtreeWorkTopic: subtree-work    # NEW
  subtreeWorkPartitions: 256        # NEW - for topic auto-creation
```

The consumer group for subtree workers: `{consumerGroup}-subtree-worker`.

**Partition strategy for stumps topic:** The existing hash(callbackURL) partitioning ensures all messages for the same callback go to the same partition, maintaining ordering. With 100× smaller messages, a single delivery consumer can now handle the volume. Multiple consumers can be added by increasing partition count.

## Risks / Trade-offs

- **[Cache miss on delivery]** If the STUMP cache entry expires before delivery, the message has no STUMP data → Mitigated by retry: re-enqueue the message. The STUMP cache TTL (5 min) far exceeds typical delivery time (~1 sec). For retried messages with backoff > TTL, the STUMP must be rebuilt (subtree worker reprocesses) or the message includes inline data as fallback.
- **[All-in-one coupling]** In-process cache works in all-in-one mode but not when block processor and delivery service are separate processes → Acceptable for now. Microservice mode can use a shared cache (Redis/Aerospike) in a future change. Document this limitation.
- **[Aerospike counter for BLOCK_PROCESSED]** Counter decrement failures could prevent BLOCK_PROCESSED emission → Aerospike retry logic (already 3 retries with backoff) mitigates transient failures. For persistent failures, the counter TTL expires and the block can be reprocessed.
- **[Subtree work topic partition count]** Fixed at creation time; changing requires topic recreation → Choose a generous default (256) and document how to resize. For 1000+ subtree blocks, 256 partitions means ~4 subtrees queued per partition, still highly parallel.
- **[Message ordering]** Distributing subtree processing across workers means MINED callbacks may arrive in different order per subtree → Acceptable because each MINED callback is independent and idempotent (carries complete STUMP, not a delta).
- **[Migration]** Switching from inline StumpData to StumpRef requires coordinated deployment → Backward-compatible design: delivery service handles both formats. Deploy delivery service first (understands StumpRef), then block processor (starts using StumpRef).
