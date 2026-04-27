## Why

The callback delivery pipeline is bottlenecked by Kafka message volume, not concurrency. Each MINED StumpsMessage carries the full STUMP binary (~365 KB), duplicated for every callback URL per subtree. For a 1M-transaction block with 250 subtrees and 100 callback endpoints, this produces 9.1 GB of Kafka traffic — capping delivery throughput at ~22K txids/sec regardless of worker count. The target is 100K+ txids/sec to support blocks with millions of transactions.

Additionally, the block processor's internal subtree worker pool (default 16) limits parallelism. The architecture should support horizontal scaling where each subtree can be processed by an independent consumer, enabling 1000+ subtrees to process concurrently across multiple instances.

## What Changes

- **STUMP cache with reference passing**: Store each STUMP once in a shared in-process cache (keyed by `subtreeHash:blockHash`), and replace the `StumpData []byte` field in Kafka messages with a `StumpRef string` reference. The delivery service resolves the reference from cache before HTTP delivery. Reduces per-message Kafka size from ~365 KB to ~3.6 KB (100× reduction).
- **Kafka message format change**: **BREAKING** — StumpsMessage gains a `StumpRef` field. The delivery service looks up STUMP data from the cache when `StumpRef` is set and `StumpData` is empty. Backward-compatible: messages with inline `StumpData` still work.
- **Subtree-level Kafka fan-out**: Replace the block processor's internal goroutine worker pool with Kafka-based fan-out. The block processor publishes one lightweight message per subtree to a new `subtree-work` topic. Independent subtree worker consumers (horizontally scalable) consume these messages, build STUMPs, and publish MINED callbacks. This enables scaling from 16 in-process workers to hundreds of independent consumers.
- **Configurable Kafka partition counts**: Add topic partition configuration and documentation to support scaling consumer parallelism (partition count = max parallel consumers).
- **Scale test validation**: Update TestScaleMega to measure the improvements and validate 100K+ txids/sec delivery throughput.

## Capabilities

### New Capabilities
- `stump-cache`: In-process cache for STUMP binary data, enabling reference-based passing through Kafka instead of inline duplication
- `subtree-work-fanout`: Kafka-based fan-out of subtree processing work, enabling horizontal scaling of subtree processors

### Modified Capabilities
- `block-processing`: Block processor changes from internal worker pool to Kafka fan-out for subtree processing. Publishes subtree work items instead of directly processing subtrees.

## Impact

- **internal/kafka/messages.go**: New `StumpRef` field on `StumpsMessage`; new `SubtreeWorkMessage` type
- **internal/block/processor.go**: Refactored to publish subtree work items to Kafka instead of processing in-process
- **internal/block/subtree_processor.go**: Becomes a standalone consumer service that processes subtree work items
- **internal/callback/delivery.go**: Resolves STUMP references from cache before HTTP delivery
- **internal/store/**: New STUMP cache store
- **internal/config/config.go**: New config fields for subtree-work topic, STUMP cache settings
- **config.yaml**: New topic and cache configuration
- **test/scale/**: Updated to validate throughput improvements
- **Kafka topics**: New `subtree-work` topic required; existing `stumps` topic messages become smaller
