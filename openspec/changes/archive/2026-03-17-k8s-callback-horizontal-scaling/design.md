## Context

The callback delivery service consumes MINED and BLOCK_PROCESSED messages from the `stumps` Kafka topic and delivers them as HTTP POSTs to registered callback URLs. In the current TestScaleMega benchmark (1M txids, 244 subtrees, 100 callback instances):

- Block processing takes 22ms (45M txids/sec) — not a bottleneck
- Callback delivery takes 10.8s (92K txids/sec) — **99.8% of wall clock time**
- The subtree worker publishes 24,400 MINED messages (244 subtrees × 100 URLs) plus 100 BLOCK_PROCESSED messages

The delivery service already supports Kafka consumer-group scaling: multiple pods consume different partitions. The K8s manifest already deploys 4 replicas. However, this multi-instance behavior has never been validated in the scale test, and there's an opportunity to reduce Kafka message volume through cross-subtree batching.

## Goals / Non-Goals

**Goals:**
- Validate multi-instance callback delivery in the scale test (prove horizontal scaling works)
- Reduce Kafka message count on the stumps topic through cross-subtree batching
- Quantify the throughput improvement from both multi-instance scaling and batching

**Non-Goals:**
- Changing the HTTP callback payload format (receivers expect current JSON structure)
- Modifying the STUMP cache architecture (already distributed via Aerospike)
- Adding callback-delivery-specific Kafka topics (reuse existing stumps topic)

## Decisions

### 1. Aggregation in the subtree worker, not a new service

**Decision**: Add a per-block accumulation buffer in the subtree worker that batches MINED messages across subtrees for the same callback URL, flushing once all subtrees for a block are processed (triggered by the subtree counter reaching zero).

**Rationale**: Adding a separate aggregation consumer between subtree workers and delivery adds latency and operational complexity. The subtree worker already has the subtree counter that signals "all subtrees done" — this is the natural flush point. The BLOCK_PROCESSED emission already happens at counter==0, so batching MINED messages at the same point is a clean extension.

**Alternatives considered**:
- *Separate aggregation service*: Adds another Kafka topic hop and a new service to deploy/monitor. Rejected for complexity.
- *Batching in the delivery service*: The delivery service would need windowing logic to accumulate messages before HTTP POST, adding latency. Rejected because the subtree worker already has the natural "block complete" signal.
- *No batching (scale via replicas only)*: Works but leaves 100x Kafka overhead on the table. The 24,400 messages could be 100.

### 2. Flush strategy: atomic counter == 0

**Decision**: Each subtree worker accumulates `{callbackURL → []txids, stumpRefs}` in a shared Aerospike map (keyed by blockHash). When the subtree counter reaches zero, the last worker reads the accumulated map, publishes one batched StumpsMessage per callback URL, then deletes the accumulation record.

**Rationale**: In a distributed K8s deployment, different subtree worker pods process different subtrees. They can't accumulate in local memory — the buffer must be in shared state (Aerospike). Aerospike CDT map operations allow atomic append-to-list, and the existing subtree counter already provides the "all done" signal.

**Alternatives considered**:
- *Local in-memory accumulation*: Only works in single-process (all-in-one) mode. Not viable for K8s where subtrees fan out across pods.
- *Redis sorted sets*: Adds a new dependency. Aerospike is already available and supports CDT maps.

### 3. Multi-instance scale test with partitioned stumps topic

**Decision**: Create the stumps topic with multiple partitions in the scale test and run 2+ delivery service instances to validate Kafka consumer-group rebalancing delivers all callbacks correctly.

**Rationale**: The existing scale test runs 1 delivery instance. Multi-instance testing proves the architecture works before claiming horizontal scalability. We need at least 2 instances to verify partition assignment and dedup correctness.

### 4. Batched StumpsMessage format

**Decision**: Extend StumpsMessage with a `StumpRefs []string` field (plural) alongside the existing singular `StumpRef`. The batched message carries all txids for one callback URL across all subtrees, plus one StumpRef per subtree that contained registered txids.

**Rationale**: The delivery service needs to resolve and send STUMP data. A block may have txids spread across multiple subtrees, each with its own STUMP. The batched message must reference all relevant STUMPs. The delivery HTTP POST already supports `TxIDs []string`, so the payload just gets larger.

## Risks / Trade-offs

- **[Aerospike CDT map size]** For blocks with many subtrees and many URLs, the accumulation map could grow large → Mitigation: Set a TTL on accumulation records. The existing subtree counter TTL (600s) provides a natural cleanup. Record size is bounded by `subtrees × URLs × ~100 bytes` ≈ 244 × 100 × 100 = 2.4 MB, well within Aerospike's record size limit.

- **[Partial block failure]** If some subtree workers fail, the accumulation buffer holds partial data forever → Mitigation: TTL on accumulation records ensures cleanup. Failed subtrees are logged. The subtree counter won't reach zero, so no batched message is published, and the individual per-subtree messages (which are still published as a fallback) ensure delivery.

- **[All-in-one vs K8s divergence]** In all-in-one mode, the accumulation buffer could be in-memory, but in K8s it must be Aerospike → Decision: Always use Aerospike for the accumulation buffer. Uniform behavior across modes, and the Aerospike operations are fast (~1ms per CDT append).

- **[Expected performance improvement]** The batching reduces Kafka messages from ~24,400 to ~100 (244x reduction) but the HTTP delivery throughput is bounded by network I/O. Expected improvement from batching alone: 10-20% (fewer Kafka reads, fewer HTTP POSTs). Expected improvement from multi-instance scaling: linear with replica count (2 pods ≈ 2x, 4 pods ≈ 4x). Combined: 4 delivery pods with batching should reach ~400K+ txids/sec, well above the 100K target.

## Open Questions

- Should the per-subtree individual messages be kept as a fallback alongside batched messages, or should we switch entirely to batched mode? Keeping both means some callbacks could be delivered twice (handled by dedup), but provides resilience. Dropping individual messages makes batching a hard dependency.
