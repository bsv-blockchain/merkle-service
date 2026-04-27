## Why

The merkle-service receives duplicate subtree and block announcements from multiple P2P nodes via libp2p topics. Currently, each duplicate message is fully processed — fetching data from DataHub, checking registrations, and emitting callbacks — resulting in duplicate SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, and MINED callbacks to registered callback URLs. The seen counter is also incremented by duplicates, causing premature threshold triggering. This wastes resources, confuses downstream consumers, and undermines the reliability of the callback system.

## What Changes

- Add in-memory deduplication caches to the subtree and block processors that track successfully-processed message hashes, preventing reprocessing of already-completed work while allowing retries on failure
- Make the seen counter idempotent by tracking which subtree IDs have contributed to a txid's count, preventing duplicate subtree announcements from inflating the counter
- Add per-txid/callbackURL/status dedup tracking in callback delivery to ensure each callback URL receives exactly one SEEN_ON_NETWORK, one SEEN_MULTIPLE_NODES, and one MINED callback per txid
- Add idempotency keys to callback HTTP deliveries so downstream consumers can detect retried deliveries

## Capabilities

### New Capabilities
- `message-dedup`: In-memory deduplication layer for subtree and block message processors — tracks successfully processed hashes and skips duplicates while allowing retries on failure
- `callback-dedup`: Per-txid/callbackURL/status deduplication for callback delivery — ensures at-most-once delivery semantics per callback type using Aerospike-backed tracking

### Modified Capabilities

## Impact

- `internal/subtree/processor.go` — add processed-subtree-hash tracking before message handling
- `internal/block/processor.go` — add processed-block-hash tracking before message handling
- `internal/store/seen_counter.go` — make increment idempotent by tracking contributing subtree IDs per txid
- `internal/callback/delivery.go` — add dedup check before HTTP delivery, add idempotency headers
- `internal/store/` — new dedup store or Aerospike sets for callback delivery tracking
- Aerospike schema — new set for callback delivery dedup records
- Kafka message format — add optional idempotency key field to StumpsMessage
