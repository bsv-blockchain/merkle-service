## Why

Scale testing with 1M transactions (100 Arcade instances × 10,000 txids, 250 subtrees × 4,000 txids) reveals that callback delivery is the overwhelming bottleneck: block processing takes 30ms but callback delivery takes **79 seconds** — 99.96% of total pipeline time. Current throughput is ~12,600 txids/sec; target is 100,000+ txids/sec (an 8× improvement).

The root cause is that the callback delivery service processes Kafka messages **sequentially** — one message at a time with a blocking HTTP POST per message. With 25,000 Kafka messages (250 subtrees × 100 callback URLs), each requiring JSON serialization, HTTP delivery, and Aerospike dedup operations, the pipeline is fundamentally I/O-bound with no concurrency.

### Current Bottleneck Analysis

| Metric | Value | Notes |
|--------|-------|-------|
| Block processing (T0→T1) | 30ms | 33.7M txids/sec — not a bottleneck |
| Callback delivery (T1→T2) | 79.1s | 12,638 txids/sec — **the bottleneck** |
| Total pipeline (T0→T3) | 79.2s | 12,633 txids/sec |
| P50/P95/P99 latency | 79.2s/79.2s/79.2s | All instances finish together (sequential) |
| Kafka messages produced | 25,000 | 250 subtrees × 100 callback URLs |
| HTTP payload per message | ~4.7KB JSON | 40 txids + base64 STUMP + metadata |
| Total HTTP data | 9.1 GB | Across 100 callback servers |

### Identified Bottlenecks (Priority Order)

1. **Sequential Kafka consumption** (`consumer.go:99-120`): `ConsumeClaim` processes one message at a time; the handler blocks on HTTP delivery before consuming the next message
2. **No worker pool for HTTP delivery** (`delivery.go:237`): Unlike the block processor (which uses a semaphore-bounded goroutine pool), delivery has zero concurrency
3. **Default HTTP transport** (`delivery.go:66-68`): No `MaxIdleConnsPerHost` tuning (Go default: 2); no keepalive configuration
4. **Dedup in critical path** (`delivery.go:208-248`): Two Aerospike round-trips per message (Exists + Record) adding ~10-20ms overhead
5. **STUMP data duplication** (`subtree_processor.go:124-147`): Same binary STUMP encoded into every Kafka message for each callback URL in a subtree

## What Changes

- Add a **concurrent worker pool** to the callback delivery service with configurable concurrency (e.g., 64-256 workers), decoupling Kafka consumption from HTTP delivery
- **Tune HTTP transport** with proper connection pooling (`MaxIdleConnsPerHost`, `MaxConnsPerHost`, keepalive)
- **Batch Aerospike dedup operations** to reduce per-message overhead
- Add **configurable delivery concurrency** to `config.yaml`
- Measure improvement with the mega scale test targeting 100K+ txids/sec

## Capabilities

### New Capabilities

_(none — performance improvements to existing delivery pipeline)_

### Modified Capabilities

- `callback-dedup`: Batch dedup operations instead of per-message lookups to reduce Aerospike round-trips

## Impact

- **`internal/callback/delivery.go`**: Major refactor — add worker pool consuming from internal channel, concurrent HTTP delivery, batched dedup
- **`internal/kafka/consumer.go`**: Consider async message handling option or buffered channel dispatch
- **`internal/config/config.go`**: New config fields for delivery concurrency, HTTP transport tuning
- **`internal/store/callback_dedup.go`**: Add batch Exists/Record methods
- **`config.yaml`**: New configuration section for delivery performance tuning
- **Test coverage**: Update delivery tests, verify with mega scale test
