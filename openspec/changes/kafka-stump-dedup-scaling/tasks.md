## 1. STUMP Cache

- [x] 1.1 Create `internal/store/stump_cache.go` with `StumpCache` struct: `sync.Map` storage, `Put(subtreeHash, blockHash string, data []byte)`, `Get(subtreeHash, blockHash string) ([]byte, bool)`, configurable TTL (default 5 min)
- [x] 1.2 Add background sweep goroutine that runs every 30 seconds to evict expired entries; `Start()`/`Stop()` lifecycle methods
- [x] 1.3 Add unit tests for `StumpCache`: put/get, TTL expiry, concurrent access safety, stop drains goroutine

## 2. StumpsMessage StumpRef Field

- [x] 2.1 Add `StumpRef string` field to `StumpsMessage` in `internal/kafka/messages.go` with `json:"stumpRef,omitempty"` tag
- [x] 2.2 Update `delivery.go` `processDelivery()` to resolve STUMP: if `StumpData` is set use it directly; if `StumpRef` is set look up from injected `StumpCache`; if cache miss, re-enqueue for retry
- [x] 2.3 Add `StumpCache` field to `DeliveryService` and wire it in `NewDeliveryService()` / `Init()`
- [x] 2.4 Update `deliverCallback()` to accept resolved STUMP data (passed from `processDelivery` after resolution) rather than reading from message directly
- [x] 2.5 Add unit tests: delivery with StumpRef resolves from cache; delivery with inline StumpData still works; cache miss triggers retry

## 3. Configuration

- [x] 3.1 Add `SubtreeWorkTopic string` (default `subtree-work`) to `config.KafkaConfig`
- [x] 3.2 Add `StumpCacheTTLSec int` (default 300) to `config.CallbackConfig`
- [x] 3.3 Add `SubtreeCounterSet string` (default `subtree_counters`) and `SubtreeCounterTTLSec int` (default 600) to `config.AerospikeConfig`
- [x] 3.4 Add viper defaults, env var bindings, and `config.yaml` entries for all new fields

## 4. SubtreeWorkMessage

- [x] 4.1 Add `SubtreeWorkMessage` struct to `internal/kafka/messages.go` with `BlockHash`, `BlockHeight`, `SubtreeHash`, `DataHubURL` fields and `Encode()`/`DecodeSubtreeWorkMessage()` methods
- [x] 4.2 Add unit test for SubtreeWorkMessage encode/decode round-trip

## 5. BLOCK_PROCESSED Coordination

- [x] 5.1 Create `internal/store/subtree_counter.go` with `SubtreeCounterStore` backed by Aerospike: `Init(blockHash string, count int) error`, `Decrement(blockHash string) (remaining int, err error)`
- [x] 5.2 Use Aerospike CDT atomic `Increment(-1)` for `Decrement()` and return the new value; set TTL on init (default 10 min)
- [x] 5.3 Add unit tests for SubtreeCounterStore: init, decrement to zero, concurrent decrements

## 6. Block Processor Refactor

- [x] 6.1 Add `subtreeWorkProducer *kafka.Producer` field to `Processor` and initialize in `Init()` for the subtree-work topic
- [x] 6.2 Refactor `processBlock()`: instead of calling `ProcessBlockSubtree()` in goroutines, publish one `SubtreeWorkMessage` per subtree to the subtree-work topic
- [x] 6.3 After publishing all subtree work items, initialize the Aerospike subtree counter with the subtree count
- [x] 6.4 Remove the internal semaphore-based worker pool from `processBlock()` (subtree processing is now external)
- [x] 6.5 Remove BLOCK_PROCESSED emission from the block processor (now handled by the last subtree worker)
- [x] 6.6 Update block processor tests for the new fan-out behavior

## 7. Subtree Worker Service

- [x] 7.1 Create `internal/block/subtree_worker.go` with `SubtreeWorkerService` struct following the `service.BaseService` Init/Start/Stop/Health pattern
- [x] 7.2 `Init()`: create Kafka consumer for subtree-work topic (consumer group: `{consumerGroup}-subtree-worker`), create stumps producer, initialize stores (registration, subtree, STUMP cache, subtree counter, URL registry)
- [x] 7.3 `handleMessage()`: decode `SubtreeWorkMessage`, call existing `ProcessBlockSubtree()` logic but with STUMP cache write + StumpRef publishing instead of inline StumpData
- [x] 7.4 After processing a subtree, decrement the Aerospike subtree counter; if counter reaches zero, emit BLOCK_PROCESSED to all registered callback URLs
- [x] 7.5 `Start()`: start the Kafka consumer; `Stop()`: stop consumer, close producers
- [x] 7.6 Add unit tests for SubtreeWorkerService: processes work item, stores STUMP in cache, publishes StumpRef messages, emits BLOCK_PROCESSED on last subtree

## 8. All-in-One Wiring

- [x] 8.1 Create shared `StumpCache` instance in the all-in-one orchestrator and inject into both `SubtreeWorkerService` and `DeliveryService`
- [x] 8.2 Register `SubtreeWorkerService` in the all-in-one service lifecycle (Init/Start/Stop)
- [x] 8.3 Verify the all-in-one process starts and stops cleanly with the new service

## 9. Scale Test Validation

- [x] 9.1 Update `test/scale/scale_test.go` to create and wire a `SubtreeWorkerService` with shared `StumpCache` alongside the block processor and delivery service
- [x] 9.2 Run `TestScaleMega` and capture new metrics — target: 100K+ txids/sec delivery throughput
- [x] 9.3 If throughput is below target, profile and tune: increase subtree-work partitions, adjust STUMP cache TTL, or identify the next bottleneck
- [x] 9.4 Document before/after metrics comparison (baseline: 22K txids/sec with worker pool only)

## 10. Cleanup

- [x] 10.1 Remove unused inline `StumpData` publishing from `ProcessBlockSubtree()` (now uses StumpRef)
- [x] 10.2 Update any remaining references to the old internal worker pool in comments or docs
- [x] 10.3 Run full test suite (`go test ./...`) and fix any regressions
