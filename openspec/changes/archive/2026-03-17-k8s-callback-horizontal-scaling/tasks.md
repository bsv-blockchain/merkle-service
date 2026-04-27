## 1. Batched StumpsMessage Format

- [x] 1.1 Add `StumpRefs []string` field to `StumpsMessage` in `internal/kafka/messages.go` with `json:"stumpRefs,omitempty"` tag
- [x] 1.2 Add unit test for StumpsMessage encode/decode round-trip with both `StumpRef` (singular) and `StumpRefs` (plural) fields

## 2. Callback Accumulation Store

- [x] 2.1 Create `internal/store/callback_accumulator.go` with `CallbackAccumulatorStore` struct backed by Aerospike CDT maps: `Append(blockHash, callbackURL string, txids []string, stumpRef string) error`, `ReadAndDelete(blockHash string) (map[string]AccumulatedCallback, error)`
- [x] 2.2 Define `AccumulatedCallback` struct with `TxIDs []string` and `StumpRefs []string` fields
- [x] 2.3 Implement `Append()` using Aerospike CDT map operations to atomically add txids and stumpRef to the map entry keyed by callbackURL within the record keyed by blockHash; set TTL on write
- [x] 2.4 Implement `ReadAndDelete()` to read the full accumulation map for a blockHash and delete the record atomically
- [x] 2.5 Add unit tests for CallbackAccumulatorStore: append single entry, append multiple entries for same URL, append entries for different URLs, read-and-delete returns all data and removes record, TTL is set

## 3. Configuration

- [x] 3.1 Add `CallbackAccumulatorSet string` (default `callback_accum`) to `config.AerospikeConfig`
- [x] 3.2 Add `CallbackAccumulatorTTLSec int` (default 600, matching subtree counter TTL) to `config.AerospikeConfig`
- [x] 3.3 Add viper defaults, env var bindings (`AEROSPIKE_CALLBACK_ACCUMULATOR_SET`, `AEROSPIKE_CALLBACK_ACCUMULATOR_TTL_SEC`), and `config.yaml` entries

## 4. Subtree Worker Batching Integration

- [x] 4.1 Add `callbackAccumulator *store.CallbackAccumulatorStore` field to `SubtreeWorkerService` and wire in constructor
- [x] 4.2 Modify `handleMessage()`: after `ProcessBlockSubtree()`, instead of publishing individual MINED StumpsMessages per callback URL, call `callbackAccumulator.Append()` for each callback URL group
- [x] 4.3 Modify the counter==0 branch: after emitting BLOCK_PROCESSED, call `callbackAccumulator.ReadAndDelete()` and publish one batched StumpsMessage per callback URL with aggregated txids and StumpRefs
- [x] 4.4 Keep `ProcessBlockSubtree()` itself unchanged — it still builds STUMPs and writes to cache; only the message publishing moves to the flush step
- [x] 4.5 Update `emitBlockProcessed()` to also handle the batched MINED flush (or extract into a new `flushBatchedCallbacks()` method)
- [x] 4.6 Add unit tests: single subtree flushes immediately at counter==0, multi-subtree accumulates then flushes, accumulator failure falls back to unbatched publishing

## 5. Delivery Service Multi-StumpRef Resolution

- [x] 5.1 Update `processDelivery()` in `delivery.go` to check `StumpRefs` (plural) field: if set, resolve all STUMPs from cache and concatenate
- [x] 5.2 If any StumpRef in the `StumpRefs` list fails to resolve, re-enqueue the message for retry (same as current single miss behavior)
- [x] 5.3 Update `deliverCallback()` to accept multiple STUMP data blobs and encode all in the callback payload (multiple base64-encoded entries or concatenated)
- [x] 5.4 Maintain backward compatibility: if only singular `StumpRef` is set, resolve as before
- [x] 5.5 Add unit tests: delivery with single StumpRef, delivery with multiple StumpRefs, partial cache miss triggers retry

## 6. All-in-One Wiring

- [x] 6.1 Create `CallbackAccumulatorStore` in the all-in-one orchestrator and inject into `SubtreeWorkerService`
- [x] 6.2 Verify the all-in-one process starts and stops cleanly with the new accumulator store
- [x] 6.3 Update `cmd/subtree-worker/main.go` standalone entrypoint to create and inject `CallbackAccumulatorStore`

## 7. Multi-Instance Scale Test

- [x] 7.1 Modify `runScaleTest()` to create the stumps topic with 4+ partitions (currently defaults to 1)
- [x] 7.2 Start 2 delivery service instances in the scale test, each in the same consumer group
- [x] 7.3 Verify all callbacks are delivered correctly (no loss, no unexpected duplicates) with multi-instance delivery
- [x] 7.4 Add metrics logging for per-instance delivery counts to validate partition distribution
- [x] 7.5 Run `TestScaleSmoke` to validate multi-instance + batching works at small scale

## 8. Scale Test Benchmarking

- [x] 8.1 Run `TestScaleMega` with batching enabled and capture metrics
- [x] 8.2 Run `TestScaleMega` with 2 delivery instances and capture metrics
- [x] 8.3 Compare before/after: document Kafka message count reduction, HTTP POST count reduction, and overall throughput improvement
- [x] 8.4 If throughput is below expectations, profile and identify remaining bottlenecks

## 9. K8s Manifest Updates

- [x] 9.1 Update `deploy/k8s/callback-delivery.yaml` with updated scaling guidance documenting stumps topic partition requirements
- [x] 9.2 Update `deploy/k8s/subtree-worker.yaml` to document the callback accumulator Aerospike set
- [x] 9.3 Update `deploy/k8s/configmap.yaml` with new accumulator config fields
- [x] 9.4 Update `deploy/k8s/README.md` with batching architecture description and multi-instance validation results

## 10. Cleanup and Full Test Suite

- [x] 10.1 Remove any unused individual per-subtree MINED message publishing code from `ProcessBlockSubtree()` (now handled by accumulator flush)
- [x] 10.2 Run full test suite (`go test ./...`) and fix any regressions
- [x] 10.3 Run `TestScaleSmoke` and `TestScaleMega` final validation
