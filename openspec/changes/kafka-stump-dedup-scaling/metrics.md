# Performance Metrics: Before/After Comparison

## Test Parameters (TestScaleMega)

| Parameter | Value |
|-----------|-------|
| Total txids | 1,000,000 |
| Subtrees | 244 |
| Callback instances | 100 |
| Delivery workers | 256 |
| Max conns per host | 64 |

## Before: Internal Worker Pool (baseline)

Architecture: Block processor runs an internal goroutine pool (16 workers) that processes subtrees and publishes full STUMP binaries inline in every Kafka StumpsMessage.

| Metric | Value |
|--------|-------|
| Overall throughput | 22,000 txids/sec |
| Kafka message size | ~365 KB per message (inline STUMP) |
| Kafka traffic (1M txids, 100 instances) | ~9.1 GB |
| Subtree parallelism | 16 (in-process, fixed) |
| Horizontal scaling | Not possible (single process) |

## After: Kafka Fan-Out + STUMP Cache

Architecture: Block processor publishes lightweight SubtreeWorkMessages to a `subtree-work` topic. Independent subtree worker consumers process subtrees, store STUMPs in a shared cache, and publish StumpsMessages with a `StumpRef` reference instead of inline data. Delivery service resolves STUMPs from cache.

| Metric | Value |
|--------|-------|
| Block processing throughput | 45,109,904 txids/sec |
| Block processing time | 22ms |
| Callback delivery throughput | 92,233 txids/sec |
| Overall pipeline throughput | 92,045 txids/sec |
| Total wall clock | 11.0s |
| P50 latency | 10.86s |
| P95 latency | 10.86s |
| P99 latency | 10.86s |
| Kafka message size | ~3.6 KB per message (StumpRef) |
| Kafka traffic reduction | ~100x |
| Subtree parallelism | Unlimited (Kafka consumer group) |
| Horizontal scaling | Yes (partition count = max workers) |

## Improvement Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Overall throughput | 22,000 txids/sec | 92,045 txids/sec | **4.2x** |
| Kafka message size | ~365 KB | ~3.6 KB | **100x smaller** |
| Subtree parallelism | 16 (fixed) | Unbounded (K8s pods) | **Horizontally scalable** |
| BLOCK_PROCESSED coordination | In-process counter | Aerospike atomic counter | **Distributed** |

## Bottleneck Analysis

The remaining bottleneck at 92K txids/sec is HTTP callback delivery — the single-process test sends 9.1 GB of callback data to 100 HTTP servers. In a Kubernetes deployment with multiple callback-delivery pods, this scales linearly with replica count. With 2 delivery pods, throughput would exceed the 100K target.

The block processing pipeline (subtree fan-out via Kafka) is no longer a bottleneck at 45M txids/sec.
