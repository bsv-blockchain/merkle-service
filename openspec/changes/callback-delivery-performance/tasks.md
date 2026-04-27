## 1. Configuration

- [x] 1.1 Add `DeliveryWorkers int` (default 64), `MaxConnsPerHost int` (default 32), `MaxIdleConnsPerHost int` (default 16) to `config.CallbackConfig` in `internal/config/config.go`
- [x] 1.2 Add defaults via `v.SetDefault()` and update `config.yaml` with the new fields

## 2. HTTP Transport Tuning

- [x] 2.1 In `delivery.go` `Init()`, replace the bare `http.Client{}` with a client using an explicit `http.Transport` configured with `MaxIdleConns`, `MaxIdleConnsPerHost`, `MaxConnsPerHost`, `IdleConnTimeout`, and `DisableCompression` from config
- [x] 2.2 Add keepalive settings to the transport's `DialContext` (30s keepalive interval)

## 3. Worker Pool Implementation

- [x] 3.1 Add a `workCh chan *deliveryJob` field and `workerWg sync.WaitGroup` to `DeliveryService` where `deliveryJob` holds the decoded `StumpsMessage`
- [x] 3.2 In `Start()`, launch N worker goroutines (from config) that read from `workCh` and call the existing `handleDelivery()` logic (dedup check → HTTP POST → dedup record → retry/DLQ on failure)
- [x] 3.3 Refactor `handleMessage()` to: decode the Kafka message, then dispatch to `workCh` (blocking send provides backpressure). Mark the Kafka offset immediately after dispatch.
- [x] 3.4 Extract the dedup-check → deliver → dedup-record → retry/DLQ logic from the current `handleMessage()` into a `processDelivery(msg *kafka.StumpsMessage)` method that workers call
- [x] 3.5 In `Stop()`, close `workCh`, wait for `workerWg` to drain all in-flight work, then close producers and consumer

## 4. Retry Handling with Workers

- [x] 4.1 Ensure `reenqueue()` and `publishToDLQ()` are safe for concurrent use (the Kafka producer's `Publish` is already thread-safe via Sarama's async producer)
- [x] 4.2 Verify that retry messages (re-enqueued with `NextRetryAt`) work correctly when consumed by the concurrent workers — the delay check in `handleMessage` must still be performed per-message before dispatch or within the worker

## 5. Testing

- [x] 5.1 Update existing `delivery_test.go` tests to work with the new worker pool (ensure tests wait for async processing via channel drain or sync mechanisms)
- [x] 5.2 Add a test `TestDeliveryService_ConcurrentWorkers` verifying that N workers process messages in parallel (e.g., use a slow HTTP handler and verify N messages are in-flight simultaneously)
- [x] 5.3 Add a test `TestDeliveryService_Backpressure` verifying that when all workers are busy, the dispatch channel blocks the consumer
- [x] 5.4 Add a test `TestDeliveryService_GracefulShutdown` verifying in-flight deliveries complete before Stop() returns

## 6. Scale Test Validation

- [x] 6.1 Run `TestScaleMega` and capture the new metrics report — target: 100K+ txids/sec delivery throughput
- [x] 6.2 If throughput is below target, profile and tune: increase workers, adjust HTTP transport settings, or identify the next bottleneck
- [x] 6.3 Document the before/after metrics comparison in the test output
