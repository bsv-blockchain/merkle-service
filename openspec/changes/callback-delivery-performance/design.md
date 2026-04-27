## Context

The callback delivery pipeline is the dominant bottleneck at scale. When processing 1M transactions across 250 subtrees to 100 Arcade instances, the block processor produces 25,000 Kafka messages in ~30ms, but the delivery service takes 79 seconds to deliver them all — processing sequentially at ~316 messages/sec (12,600 txids/sec).

### Current Architecture

```
Kafka (stumps topic) → [Single Consumer Loop] → handleMessage() → HTTP POST → next message
                         sequential, blocking          blocking I/O
```

Each iteration of the consumer loop:
1. Decode Kafka message (~0.1ms)
2. Aerospike dedup check (~5-10ms)
3. JSON marshal payload (~0.1ms)
4. HTTP POST to callback URL (~1-5ms for local, 10s timeout)
5. Aerospike dedup record (~5-10ms)
6. Commit offset

**Total per message**: ~3-20ms (local test) × 25,000 messages ≈ 75-500 seconds

### Why It's Slow

The fundamental issue is that **I/O-bound work (HTTP delivery, Aerospike lookups) runs sequentially** when it could run concurrently. HTTP delivery and Aerospike operations are network I/O — the CPU is idle waiting for responses. With 25,000 independent messages to 100 different endpoints, this is embarrassingly parallel.

## Goals / Non-Goals

**Goals:**
- Achieve 100,000+ txids/sec delivery throughput (8× improvement)
- Keep message delivery guarantees intact (at-least-once, dedup, retry, DLQ)
- Maintain offset management correctness (no messages lost on crash)
- Make concurrency configurable for different deployment sizes
- Validate improvement with the mega scale test

**Non-Goals:**
- Changing the Kafka message format or block processing pipeline
- Implementing exactly-once delivery (at-least-once with dedup is sufficient)
- Optimizing STUMP data duplication across messages (separate future change)
- Adding HTTP/2 or gRPC transport (keep HTTP/1.1 POST for now)

## Decisions

### 1. Buffered channel + worker pool pattern

Replace the synchronous `handleMessage` → `deliverCallback` flow with:

```
Kafka Consumer → decode → buffered channel (capacity: 2×workers) → N worker goroutines → HTTP POST
```

**Implementation:**
- `handleMessage` decodes the Kafka message and sends it to a buffered channel
- N worker goroutines read from the channel, each independently performing dedup check, HTTP delivery, and dedup record
- Workers are started in `Start()` and drained in `Stop()`
- Default: 64 workers (tunable via config)

**Rationale:** This is the simplest pattern that decouples consumption from delivery. The buffered channel provides natural backpressure — if all workers are busy on slow HTTP calls, the channel fills and the consumer pauses, preventing unbounded memory growth. Alternative considered: async message handler in the consumer itself, but this would require changing the consumer's contract and offset management semantics.

**Offset management:** With concurrent delivery, we can't mark individual message offsets as complete out-of-order with Sarama's consumer group. Instead, we use a **fire-and-forget-from-consumer** approach: the consumer marks the message immediately after dispatching to the channel (not after delivery). This means:
- On crash, some messages may be re-delivered (but dedup handles this)
- This is acceptable because the dedup store already provides idempotency
- The alternative (tracking per-offset completion) adds significant complexity for marginal benefit

### 2. Tuned HTTP transport

Configure `http.Transport` explicitly:

```go
transport := &http.Transport{
    MaxIdleConns:        256,
    MaxIdleConnsPerHost: 16,
    MaxConnsPerHost:     32,
    IdleConnTimeout:     90 * time.Second,
    DisableCompression:  true,  // JSON is small, compression adds latency
}
```

**Rationale:** Go's default `MaxIdleConnsPerHost` is 2, which means only 2 connections can be reused per callback endpoint. With 64 workers hitting 100 endpoints, we need at least `workers/endpoints × safety_margin` idle connections per host. 16 idle per host × 100 hosts = 1,600 idle connections max — well within memory constraints. `DisableCompression` avoids CPU overhead on small 4.7KB payloads where compression ratio is poor.

### 3. Configuration-driven concurrency

Add to `config.CallbackConfig`:

```go
DeliveryWorkers    int  // default: 64
MaxConnsPerHost    int  // default: 32
MaxIdleConnsPerHost int // default: 16
```

**Rationale:** Different deployments have different endpoint characteristics. A deployment delivering to 10 endpoints can use fewer workers than one delivering to 1,000. Making it configurable avoids hardcoding assumptions.

### 4. Batch dedup operations (deferred)

The current dedup adds ~10-20ms per message (two Aerospike round-trips). For 25,000 messages, that's 4-8 minutes of just dedup overhead. However, with 64 concurrent workers, the per-message overhead is amortized to 10-20ms/64 ≈ 0.15-0.3ms effective — making it a secondary concern. We'll defer batching to a future change if dedup proves to be a bottleneck after the concurrency improvement.

**Rationale:** Focus on the biggest win first (concurrency). Premature optimization of dedup before we know whether it matters with concurrent workers would add complexity without proven benefit.

## Risks / Trade-offs

- **[At-least-once duplication window]** Marking offsets before delivery confirms means more potential duplicate deliveries during crashes → Mitigated by existing dedup store + idempotency key headers; receivers should already handle duplicates
- **[Memory usage]** 64 workers × buffered payloads (~5KB each) + HTTP connection pool → ~50MB additional memory, negligible against 64GB
- **[Thundering herd]** 64 workers simultaneously hitting the same endpoint → Mitigated by `MaxConnsPerHost` limit; also, messages are hash-partitioned by callback URL, so workers naturally spread across endpoints
- **[Retry message ordering]** Concurrent delivery means retried messages may arrive out of order → Acceptable because STUMP callbacks are idempotent; each carries a complete proof, not a delta
- **[Observability]** Harder to debug delivery issues with concurrent workers → Add per-worker metrics and structured logging with worker ID

## Scale Test Results

### Before (sequential processing)
```
Delivery throughput:      12,638 txids/sec
Callback delivery time:   1m19.129s
Total pipeline time:      1m19.159s
```

### After (64-worker pool, default config)
```
Delivery throughput:      17,693 txids/sec  (+40%)
Callback delivery time:   56.518s
Total pipeline time:      56.561s
```

### After (256-worker pool, tuned transport)
```
Delivery throughput:      22,058 txids/sec  (+75% vs baseline)
Callback delivery time:   45.336s
Total pipeline time:      45.367s
```

### Root Cause Analysis: Why 22K < 100K Target

The remaining bottleneck is **Kafka message volume**, not worker count. Each MINED Kafka
message carries the full STUMP binary (~271 KB raw, ~365 KB base64-encoded in JSON).
The same STUMP is duplicated for every callback URL per subtree:

- 250 subtrees × 100 callbacks × 365 KB = **9.1 GB** through Kafka
- Single Kafka consumer throughput on localhost: ~200 MB/s
- **Expected minimum delivery time: 9.1 GB / 200 MB/s ≈ 45 seconds** ← matches observed

Increasing worker count beyond 256 yields no improvement because workers are starved
waiting for the Kafka consumer to deliver messages.

### Next Optimization (separate change)

**STUMP reference passing**: Instead of embedding full STUMP binary in every Kafka message,
store the STUMP once in a shared cache (keyed by subtreeHash+blockHash) and pass only a
reference in the message. This reduces per-message size from ~365 KB to ~3.6 KB (100×
reduction), bringing the Kafka volume from 9.1 GB to ~90 MB and theoretical delivery time
from 45s to <1s.
