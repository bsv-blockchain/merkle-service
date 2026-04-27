## Context

Merkle Service is a new greenfield Go service that bridges Teranode (BSV blockchain node) and Arcade (transaction broadcast client). Arcade needs merkle proofs (STUMPs) for transactions it broadcasts so it can construct full BUMPs (BRC-0074) for SPV verification. Currently no such service exists.

The system must handle blocks containing millions of transactions with subtrees of thousands of transactions each. It follows Teranode's established patterns for daemon lifecycle, batched Aerospike operations, libp2p networking, and directly reuses Teranode's `stores/blob` package for subtree storage and `stores/txmetacache` package for registration deduplication.

**Stakeholders**: Arcade team (primary consumer), Teranode team (P2P protocol dependency), infrastructure team (deployment/operations).

**Constraints**:
- Must use Go to align with Teranode ecosystem
- Must use Aerospike for registration storage (scale requirements rule out traditional databases)
- Must use Kafka for async inter-service messaging (proven at Teranode scale)
- Must use libp2p for Teranode P2P connectivity (protocol requirement)

## Goals / Non-Goals

**Goals:**
- Deliver STUMPs to Arcade via callback when registered transactions are mined
- Notify Arcade when registered transactions are first seen on the network
- Support millions of transactions per block with bounded resource usage
- Run as all-in-one process or as independent microservices
- Follow Teranode daemon/service patterns for operational consistency

**Non-Goals:**
- Coinbase transaction replacement (Arcade's responsibility to combine STUMP with coinbase BEEF)
- Full BUMP construction (only subtree-scoped STUMPs)
- Transaction validation or propagation (Teranode's domain)
- Block storage or chain state management (Arcade stores blocks)
- Real-time streaming API (callback-based delivery only)

## Decisions

### 1. Aerospike schema: txid → set of callback URLs

**Decision**: Store registrations as txid (key) → CDT set of callback URLs (bin). Use Aerospike CDT list/set operations for multi-callback support.

**Rationale**: CDT set operations provide atomic, idempotent add/remove of callback URLs without read-modify-write cycles. This supports the requirement for multiple callbacks per txid (e.g., multiple Arcade instances watching the same transaction) while eliminating duplicates at the storage layer.

**Alternatives considered**:
- Separate record per txid+callback pair: simpler but requires scan-by-prefix for lookups, no batch-friendly pattern
- JSON blob bin: loses atomic set operations, requires read-modify-write

### 2. Seen-count tracking with Aerospike atomic counter and Teranode txmetacache

**Decision**: Maintain an atomic counter per txid in a separate Aerospike bin (or namespace) using the `operate` command with `Add` operation. A configurable threshold triggers SEEN_MULTIPLE_NODES status. Reuse Teranode's `stores/txmetacache` package (`github.com/bsv-blockchain/teranode/stores/txmetacache`) for in-memory deduplication to reduce Aerospike load.

**Rationale**: Aerospike's `operate` command provides atomic increment-and-read in a single call, making it ideal for distributed counting. Starting seen-count tracking from the first subtree (not deferring it) gives Arcade early confidence that a transaction has propagated across the network. Teranode's txmetacache is purpose-built for this workload — it uses off-heap mmap'd ring buffers with xxhash-based sharding across 8192 buckets, generation-based expiration, and batch operations (`SetMulti`, `GetMetaCached`). This avoids redundant Aerospike hits when the same txid appears in multiple subtrees within a short window, and keeps GC pressure low via off-heap memory allocation.

**Alternatives considered**:
- Custom LRU cache: reinvents what Teranode already solved; txmetacache is battle-tested at scale
- Redis counter: adds another infrastructure dependency
- In-memory counter only: loses state on restart, doesn't work across microservice instances

### 3. Subtree store: Teranode blob.Store with delete-at-height (DAH)

**Decision**: Reuse Teranode's `stores/blob` package (`github.com/bsv-blockchain/teranode/stores/blob`) for subtree storage, matching how Teranode's own `services/subtreevalidation` stores subtrees. Subtrees are stored via the `blob.Store` interface keyed by subtree hash with `fileformat.FileType` set to `SUBTREESTORE`. Expiry uses delete-at-height (DAH) — blockchain-height-based TTL — rather than wall-clock TTL. The blob store's `SetDAH` method schedules automatic deletion at a configured block height offset, and `SetCurrentBlockHeight` is called as the chain advances to trigger pruning.

**Rationale**: Direct reuse of Teranode's blob store provides proven-at-scale subtree storage with zero new infrastructure. The blob store supports pluggable backends (memory, file, HTTP remote, S3) via URL-scheme factory, so the deployment can start with filesystem-backed storage and scale to HTTP blob servers when needed. Key features that match our workload:
- **Streaming I/O** (`GetIoReader`/`SetFromReader`): processes large subtrees without loading entire blobs into memory
- **ConcurrentBlob wrapper**: prevents duplicate fetches when multiple goroutines request the same subtree (common during block processing)
- **DAH (delete-at-height)**: blockchain-native expiry that aligns with block processing lifecycle — no wall-clock TTL drift
- **Batching wrapper** (`batch=true` URL parameter): configurable batch size/interval for write throughput
- Battle-tested in Teranode's subtreevalidation service at the same scale we target

**Alternatives considered**:
- Aerospike for subtree storage: additional infrastructure dependency when blob.Store already solves this exact problem in Teranode
- In-memory store: doesn't scale to millions of transactions per block
- Redis: memory-bound for large binary payloads
- Custom disk-backed store: reinvents what blob.Store already provides

### 4. Kafka topic partitioning strategy

**Decision**:
- `subtree` topic: partitioned by subtree ID (distributes subtree processing across consumers)
- `block` topic: partitioned by block hash (single partition often sufficient; ordering matters less since blocks are sequential)
- `stumps` topic: partitioned by hash of callback URL (ensures ordered delivery per callback recipient)

**Rationale**: Partitioning by subtree ID enables horizontal scaling of subtree processors. Partitioning stumps by callback URL hash ensures that retry re-enqueue and original delivery go to the same partition, maintaining ordering guarantees for each callback recipient.

**Alternatives considered**:
- Partition stumps by txid: loses per-callback ordering
- Single partition for all topics: doesn't scale

### 5. STUMP construction: BRC-0074 BUMP format scoped to subtree

**Decision**: Construct STUMPs following the BRC-0074 BUMP binary format but scoped to the subtree merkle tree rather than the full block merkle tree. When multiple registered transactions share a callback URL within the same subtree, their merkle paths are merged into a single STUMP.

**Rationale**: BRC-0074 already defines the binary encoding for merkle paths. By scoping to subtree level, Merkle Service avoids needing the full block tree (which it doesn't have until Arcade combines with coinbase). Merging paths per callback URL reduces the number of callback deliveries and message sizes.

### 6. All-in-one wiring via Go composition root

**Decision**: A single `cmd/merkle-service` entry point that instantiates all services using a composition root pattern. Each service also has its own `cmd/<service-name>` entry point for microservice deployment. Mode selection via environment variable or CLI flag.

**Rationale**: Matches Teranode's approach where the same codebase supports both modes. The composition root pattern (a single `main` that creates and wires all dependencies) avoids service discovery complexity in all-in-one mode while keeping services decoupled enough for independent deployment.

**Structure**:
```
cmd/
  merkle-service/     # all-in-one
  api-server/         # microservice entry points
  p2p-client/
  subtree-processor/
  block-processor/
  callback-delivery/
internal/
  service/            # daemon interface + base implementation
  api/                # HTTP server, handlers
  p2p/                # libp2p client
  subtree/            # subtree processor
  block/              # block processor, block-subtree processor
  callback/           # callback delivery
  store/              # Aerospike client, registration store, blob.Store subtree integration
  kafka/              # producer/consumer wrappers
  stump/              # STUMP/BUMP construction (BRC-0074)
  config/             # configuration loading
```

### 7. Callback retry: linear backoff via Kafka re-enqueue

**Decision**: Failed callbacks are re-enqueued to the Kafka `stumps` topic with an incremented retry counter and a delay header. Consumers skip messages whose delay has not elapsed. After a configurable max retries (default: 5), messages go to a `stumps-dlq` dead letter topic.

**Rationale**: Using Kafka for retry avoids a separate retry infrastructure (e.g., a scheduler or cron). Linear backoff (e.g., 30s, 60s, 90s, 120s, 150s) is simpler to reason about than exponential and sufficient for transient callback failures. The dead letter topic preserves failed messages for operational investigation.

**Alternatives considered**:
- Exponential backoff: over-aggressive delays for short outages
- Separate retry queue per callback: operational complexity
- In-memory retry: lost on restart

### 8. Block parallelism: bounded goroutine worker pool

**Decision**: The block processor uses a bounded worker pool (configurable size, default matching typical subtree count) to process subtrees in parallel. Workers pull subtree IDs from a channel, process each independently, and report completion via a WaitGroup or errgroup.

**Rationale**: Unbounded goroutines risk memory exhaustion on large blocks (thousands of subtrees). A bounded pool provides backpressure while maintaining parallelism. The pattern matches Go's idiomatic concurrent processing with errgroup.

### 9. Coinbase boundary: STUMP only, no BEEF combination

**Decision**: Merkle Service constructs and delivers STUMPs only. The coinbase transaction in subtree 0 is treated as an opaque placeholder. Arcade is responsible for combining the STUMP with the coinbase BEEF (from the Teranode block definition) to produce a full BUMP.

**Rationale**: Separating concerns keeps Merkle Service focused on merkle proof construction. The Teranode block definition already includes coinbase BEEF, which Arcade stores. Duplicating coinbase handling in Merkle Service would create a coupling to block storage that belongs in Arcade.

### 10. Service communication: Kafka async, gRPC for sync

**Decision**: All inter-service communication uses Kafka for asynchronous message passing (subtrees, blocks, STUMPs). If any synchronous service-to-service calls are needed in the future, gRPC will be used following Teranode patterns.

**Rationale**: The entire pipeline is naturally async — subtrees arrive, get processed, blocks arrive, STUMPs get delivered. Kafka provides the durability, ordering, and consumer group semantics needed. gRPC is held in reserve for potential future needs (e.g., admin/control plane) consistent with Teranode's tech choices.

## Risks / Trade-offs

**[Subtree store size at peak load]** — During large blocks, the subtree store may hold gigabytes of data temporarily.
→ Mitigation: Teranode's blob.Store supports filesystem and S3 backends that handle this naturally. DAH ensures automatic cleanup at the next block height. For filesystem backend, monitor disk usage; for HTTP/S3 backends, the storage is distributed.

**[Callback delivery latency]** — Re-enqueue retry via Kafka adds consumer-side delay checking overhead.
→ Mitigation: Delay checking is lightweight (compare timestamp); most callbacks succeed on first attempt. Dead letter topic catches persistent failures.

**[P2P message ordering]** — libp2p does not guarantee ordered delivery; subtree messages may arrive after the block message.
→ Mitigation: Block processor waits for subtrees with a configurable timeout; subtrees stored in blob.Store are available regardless of arrival order.

**[Aerospike CDT set size]** — If a txid has a very large number of callback registrations, CDT operations may slow down.
→ Mitigation: In practice, the number of callbacks per txid is small (a few Arcade instances). Add a configurable max-callbacks-per-txid limit if needed.

**[Kafka consumer lag during large blocks]** — Block processing is inherently bursty.
→ Mitigation: Consumer group scaling (add more instances), bounded worker pools, and monitoring consumer lag metrics.

**[txmetacache memory usage]** — Teranode's txmetacache uses off-heap mmap'd memory which avoids GC pressure, but total allocation must be configured.
→ Mitigation: txmetacache uses generation-based expiration with configurable max bytes. Memory is mmap'd (off-heap) so it doesn't affect Go GC. Configure `maxBytes` based on expected registered txid cardinality. The ring buffer architecture naturally evicts oldest entries when capacity is reached.

## Migration Plan

This is a greenfield service — no migration from existing systems is needed.

**Deployment steps**:
1. Deploy Aerospike namespace, configure blob.Store backend (filesystem or HTTP), and create Kafka topics (infrastructure provisioning)
2. Deploy Merkle Service in all-in-one mode for initial testing
3. Configure Arcade to register transactions via `/watch` endpoint
4. Monitor SEEN_ON_NETWORK and MINED callbacks in Arcade
5. Scale to microservice mode when load requires independent scaling of components

**Rollback**: Since Arcade can operate without merkle proofs (degraded mode), disabling Merkle Service registration in Arcade effectively rolls back.

## Open Questions

1. **Exact libp2p topic names and message formats** — Need to confirm with Teranode team the exact topic strings and protobuf/binary message schemas for subtree and block announcements.
2. **Block height offset for DAH** — What block height offset should be used for subtree store DAH (delete-at-height)? A value of 1 means subtrees are pruned after the next block; higher values provide more buffer for late processing.
3. **Callback payload format** — Should STUMP callbacks use a JSON wrapper around the BRC-0074 binary, or deliver raw binary? Need alignment with Arcade team.
4. **Seen-count threshold default** — What is a reasonable default for SEEN_MULTIPLE_NODES threshold? Depends on typical Teranode validator count.
5. **Subtree store cleanup timing** — DAH handles automatic expiry, but should we also explicitly call `Del` after block processing completes for immediate cleanup, or rely solely on DAH-based pruning?
6. **blob.Store backend selection** — Start with `file://` for simplicity, or go directly to `http://` (remote blob server) for microservice mode compatibility? The factory pattern makes this a configuration choice, not a code change.
