## 1. Bounded LRU Dedup Cache

- [x] 1.1 Create `internal/cache/dedup_cache.go` — a thread-safe bounded LRU set that tracks processed hashes. API: `Add(key string) bool` (returns false if already present), `Contains(key string) bool`, configurable max size with oldest-entry eviction.
- [x] 1.2 Write unit tests for `dedup_cache.go` — test add/contains, duplicate detection, eviction at capacity, thread safety with concurrent access.

## 2. Subtree Processor Message Dedup

- [x] 2.1 Add `dedupCacheSize` field to `config.SubtreeConfig` with a sensible default (e.g., 100000). Add `SUBTREE_DEDUP_CACHE_SIZE` env var support.
- [x] 2.2 Add a `dedupCache *cache.DedupCache` field to the subtree `Processor`. Initialize it in `Init()` using the config value.
- [x] 2.3 In `handleMessage()`, check `dedupCache.Contains(subtreeMsg.Hash)` before processing. If present, log at debug level and return nil (skip processing, allow offset commit).
- [x] 2.4 On successful completion of `handleMessage()`, call `dedupCache.Add(subtreeMsg.Hash)` to mark the hash as processed.
- [x] 2.5 Write unit tests: duplicate subtree message skipped, failed processing allows retry, cache miss processes normally.

## 3. Block Processor Message Dedup

- [x] 3.1 Add `dedupCacheSize` field to `config.BlockConfig` with a sensible default (e.g., 10000). Add `BLOCK_DEDUP_CACHE_SIZE` env var support.
- [x] 3.2 Add a `dedupCache *cache.DedupCache` field to the block `Processor`. Initialize it in `Init()`.
- [x] 3.3 In `handleMessage()`, check `dedupCache.Contains(blockHash)` before processing. If present, log at debug level and return nil.
- [x] 3.4 On successful completion of block processing, call `dedupCache.Add(blockHash)`.
- [x] 3.5 Write unit tests: duplicate block message skipped, failed processing allows retry.

## 4. Idempotent Seen Counter

- [x] 4.1 Change `SeenCounterStore.Increment` signature to accept `subtreeID string` parameter: `Increment(txid string, subtreeID string)`.
- [x] 4.2 Replace the atomic integer `AddOp` with CDT list operations: use `ListAppendWithPolicyOp` with `ListWriteFlagsAddUnique|ListWriteFlagsNoFail` to append subtreeID to a `subtrees` bin.
- [x] 4.3 After the append, use `ListSizeOp` on the `subtrees` bin to get the unique count. Determine `ThresholdReached` by checking if the list size equals the threshold AND the append actually increased the list size.
- [x] 4.4 Update all callers of `Increment()` to pass the subtreeID — in `subtree/processor.go` `emitSeenCallbacks()`.
- [x] 4.5 Write unit tests for idempotent seen counter: first subtree increments, duplicate subtree does not increment, threshold fires exactly once on the unique count reaching threshold, threshold does not fire on duplicates.

## 5. Callback Dedup Store

- [x] 5.1 Add `callbackDedupSet` field to `config.AerospikeConfig` with default `"callback_dedup"`. Add `AEROSPIKE_CALLBACK_DEDUP_SET` env var support.
- [x] 5.2 Add `dedupTTLSec` field to `config.CallbackConfig` with default `86400` (24 hours). Add `CALLBACK_DEDUP_TTL_SEC` env var support.
- [x] 5.3 Create `internal/store/callback_dedup.go` with `CallbackDedupStore` struct. Methods: `Exists(txid, callbackURL, statusType string) (bool, error)` and `Record(txid, callbackURL, statusType string, ttl time.Duration) error`.
- [x] 5.4 Implement `Exists`: build key as SHA-256 hash of `{txid}:{callbackURL}:{statusType}`, check if record exists in Aerospike using a Get with the key.
- [x] 5.5 Implement `Record`: write a minimal record (single bin with value `1`) with the configured TTL expiration.
- [x] 5.6 Write unit tests for `CallbackDedupStore` — test key generation is deterministic, exists returns false when not recorded, true after recording.

## 6. Callback Delivery Dedup Integration

- [x] 6.1 Add `CallbackDedupStore` to the callback `Delivery` struct and wire it in `cmd/callback-delivery/main.go` and `cmd/merkle-service/main.go`.
- [x] 6.2 In the callback delivery handler, before making the HTTP POST, call `dedupStore.Exists(txid, callbackURL, statusType)`. If true, log at debug level and ack the message without delivering.
- [x] 6.3 After a successful HTTP POST response (2xx), call `dedupStore.Record(txid, callbackURL, statusType, ttl)`.
- [x] 6.4 Write unit tests: first delivery proceeds and records, duplicate delivery is skipped, failed delivery does not record (allows retry).

## 7. Idempotency Key in HTTP Requests

- [x] 7.1 Add `X-Idempotency-Key` header to callback HTTP POST requests in the delivery handler. Format: `{txid}:{statusType}` for single-txid callbacks, `{blockHash}:{subtreeID}:{statusType}` for MINED callbacks with multiple txids.
- [x] 7.2 Write unit test verifying the idempotency key header is present and correctly formatted for each status type.

## 8. Configuration Updates

- [x] 8.1 Update `config.yaml` with new configuration fields: `subtree.dedupCacheSize`, `block.dedupCacheSize`, `aerospike.callbackDedupSet`, `callback.dedupTTLSec`.
- [x] 8.2 Update `internal/config/config.go` to add the new fields with proper defaults and env var mappings.

## 9. Wiring and Integration

- [x] 9.1 Wire `CallbackDedupStore` creation in `cmd/merkle-service/main.go` (all-in-one mode).
- [x] 9.2 Wire `CallbackDedupStore` creation in `cmd/callback-delivery/main.go` (microservice mode).
- [x] 9.3 Verify all three cmd binaries compile and pass `go vet`.

## 10. Tests

- [x] 10.1 Write integration-style test for subtree processor: send duplicate subtree messages, verify only one set of callbacks is emitted.
- [x] 10.2 Write integration-style test for block processor: send duplicate block messages, verify only one set of MINED callbacks is emitted.
- [x] 10.3 Write test for seen counter idempotency: same subtreeID for same txid incremented multiple times, verify count stays at 1 and threshold fires correctly.
- [x] 10.4 Write end-to-end test: register a txid, process a subtree (duplicate), process a block (duplicate), verify exactly one SEEN_ON_NETWORK, one SEEN_MULTIPLE_NODES, one MINED callback per txid/callbackURL.
- [x] 10.5 Run full test suite (`go test ./...`) and verify all tests pass.
