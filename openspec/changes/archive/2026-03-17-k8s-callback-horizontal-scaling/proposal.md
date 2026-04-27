## Why

The callback delivery service is the pipeline bottleneck at 92K txids/sec (99.8% of wall-clock time). The architecture already supports Kafka consumer-group scaling across multiple K8s pods, but the scale test only validates a single delivery instance, and there's no test evidence that multi-pod scaling works correctly with Aerospike-backed STUMP cache and dedup. Additionally, the current message fan-out publishes one StumpsMessage per callback URL per subtree — for 244 subtrees × 100 URLs, that's 24,400 Kafka messages. Cross-subtree batching could reduce this to ~100 messages (one per URL) with all txids aggregated, dramatically cutting Kafka overhead and HTTP roundtrips.

## What Changes

- **Multi-instance delivery scale test**: Validate that running multiple delivery service instances against the same Kafka consumer group correctly delivers all callbacks without duplication or loss. This proves the existing K8s scaling path works.
- **Cross-subtree callback batching** (optional optimization): Introduce an aggregation stage that collects MINED messages for the same callback URL across subtrees before publishing to the stumps topic. Instead of 244 messages per URL (one per subtree), emit a single batched message per URL. This reduces Kafka message count by ~244x and HTTP POSTs by the same factor.
- **Stumps topic partitioning guidance**: Document and validate the relationship between stumps topic partition count and maximum useful callback-delivery replicas. Ensure the scale test creates topics with enough partitions to demonstrate multi-instance behavior.

## Capabilities

### New Capabilities
- `callback-batching`: Optional aggregation stage that batches MINED callback messages across subtrees for the same callback URL, reducing Kafka message count and HTTP delivery roundtrips

### Modified Capabilities
- `callback-dedup`: Dedup behavior needs to work correctly across multiple delivery service instances sharing an Aerospike dedup store

## Impact

- **internal/block/subtree_worker.go**: May buffer and batch MINED messages before publishing, or a new aggregation consumer sits between subtree workers and delivery
- **internal/kafka/messages.go**: StumpsMessage may carry multiple StumpRefs (one per subtree) instead of a single StumpRef
- **test/scale/scale_test.go**: Add multi-instance delivery configuration to validate horizontal scaling
- **deploy/k8s/callback-delivery.yaml**: Update scaling documentation and partition guidance
- **config.yaml**: New batching config if aggregation stage is added
