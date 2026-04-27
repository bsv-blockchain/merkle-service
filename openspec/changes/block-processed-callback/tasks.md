## 1. StatusType and Message Constants

- [x] 1.1 Add `StatusBlockProcessed StatusType = "BLOCK_PROCESSED"` constant to `internal/kafka/messages.go`
- [x] 1.2 Add test in `internal/kafka/messages_test.go` verifying BLOCK_PROCESSED StumpsMessage encode/decode round-trip with blockHash and empty txids/stumpData

## 2. Callback URL Registry Store

- [x] 2.1 Create `internal/store/callback_url_registry.go` with `CallbackURLRegistry` struct holding AerospikeClient, set name, and logger
- [x] 2.2 Implement `NewCallbackURLRegistry(client, setName, maxRetries, retryBaseMs, logger)` constructor
- [x] 2.3 Implement `Add(callbackURL string) error` that appends to a CDT list on the well-known key `__all_urls__` using `ListOrderOrdered` and `ADD_UNIQUE|NO_FAIL` flags
- [x] 2.4 Implement `GetAll() ([]string, error)` that reads the CDT list from the well-known key and returns all callback URLs
- [x] 2.5 Add unit tests for `CallbackURLRegistry` using `MemoryBlobStore` or mock — test Add deduplication, GetAll returns all URLs, empty registry returns empty slice

## 3. Registration Integration

- [x] 3.1 Add `CallbackURLRegistry` field to the registration flow — wherever `RegistrationStore.Add()` is called, also call `CallbackURLRegistry.Add()` with the same callback URL
- [x] 3.2 Wire up `CallbackURLRegistry` in the service initialization (create Aerospike set, pass to relevant services)
- [x] 3.3 Add config for the callback URL registry Aerospike set name (default: `"callback-urls"`)
- [x] 3.4 Add test verifying that registering a txid with a callback URL also adds the URL to the registry

## 4. Block Processor BLOCK_PROCESSED Emission

- [x] 4.1 Add `callbackURLRegistry` field to `block.Processor` struct and pass it via `NewProcessor`
- [x] 4.2 Modify `ProcessBlockSubtree` to return a boolean indicating whether any registrations were found (in addition to error)
- [x] 4.3 After `wg.Wait()` in `handleMessage`, check if any subtree had registrations; if so, call a new `emitBlockProcessed` method
- [x] 4.4 Implement `emitBlockProcessed(blockHash string)` that reads all URLs from the registry and publishes one `StumpsMessage` per URL with `StatusType=StatusBlockProcessed` and the block hash
- [x] 4.5 Add test: block with registered txids emits BLOCK_PROCESSED to all registry URLs after subtree processing
- [x] 4.6 Add test: block with no registered txids does NOT emit BLOCK_PROCESSED
- [x] 4.7 Add test: BLOCK_PROCESSED messages contain correct blockHash and empty stumpData/txids

## 5. Callback Delivery Adjustments

- [x] 5.1 Update `deliverCallback` in `internal/callback/delivery.go` to handle BLOCK_PROCESSED payloads (no stump data encoding needed, just status + blockHash)
- [x] 5.2 Update dedup logic in `handleMessage`: for BLOCK_PROCESSED, use `blockHash` as the dedup key instead of txid
- [x] 5.3 Update `buildIdempotencyKey` to handle BLOCK_PROCESSED: return `blockHash:BLOCK_PROCESSED` when statusType is BLOCK_PROCESSED
- [x] 5.4 Add test: BLOCK_PROCESSED message is delivered via HTTP with correct JSON payload (status + blockHash, no stumpData/txids)
- [x] 5.5 Add test: duplicate BLOCK_PROCESSED for same blockHash+callbackUrl is skipped
- [x] 5.6 Add test: BLOCK_PROCESSED delivery failure triggers retry with linear backoff

## 6. Integration Tests

- [x] 6.1 Add end-to-end test: register txids with 2 different callback URLs, process a block containing those txids, verify both URLs receive MINED callbacks AND BLOCK_PROCESSED callbacks
- [x] 6.2 Add test: process a block with no registered txids, verify no BLOCK_PROCESSED messages are emitted
- [x] 6.3 Add test: verify BLOCK_PROCESSED is emitted even when some subtrees fail processing (as long as at least one had registrations)
