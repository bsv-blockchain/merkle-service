## 1. Kafka Message Schema

- [x] 1.1 Add `TxIDs []string` field to `CallbackTopicMessage` in `internal/kafka/messages.go` with `json:"txids,omitempty"` tag
- [x] 1.2 Add encode/decode tests for `CallbackTopicMessage` with `TxIDs` array in `internal/kafka/messages_test.go`

## 2. Subtree Processor — Batch SEEN_ON_NETWORK

- [x] 2.1 In `internal/subtree/processor.go`, refactor `handleMessage` to invert the `map[txid][]callbackURL` from `findRegisteredTxids` into `map[callbackURL][]txid` for SEEN_ON_NETWORK emission
- [x] 2.2 Replace `emitSeenCallbacks` per-txid loop with a new `emitBatchedSeenCallbacks(callbackGroups map[string][]string, subtreeID string)` that publishes one `CallbackTopicMessage` per callbackURL with `TxIDs` array and `Type: CallbackSeenOnNetwork`
- [x] 2.3 Use `PublishWithHashKey(callbackURL, data)` instead of `Publish(txid, data)` for batched SEEN messages
- [x] 2.4 Remove the individual `registrationStore.Get(txid)` call from `emitSeenCallbacks` — the batch result from `findRegisteredTxids` already has callbackURLs

## 3. Subtree Processor — Batch SEEN_MULTIPLE_NODES

- [x] 3.1 Refactor `findRegisteredTxids` to return `map[string][]string` (txid → callbackURLs) instead of `[]string` so callers have the callbackURL mapping
- [x] 3.2 After batched SEEN_ON_NETWORK emission, iterate all registered txids, call `seenCounterStore.Increment` for each, and collect threshold-reached txids grouped by callbackURL
- [x] 3.3 Publish one `CallbackTopicMessage` per callbackURL with `Type: CallbackSeenMultipleNodes` and `TxIDs` containing only threshold-reached txids
- [x] 3.4 Skip SEEN_MULTIPLE_NODES emission entirely if no txids reached threshold

## 4. Callback Delivery — Payload Update

- [x] 4.1 Add `TxIDs []string` field to `callbackPayload` in `internal/callback/delivery.go` with `json:"txids,omitempty"` tag
- [x] 4.2 In `deliverCallback`, populate `payload.TxIDs` from `msg.TxIDs` for SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES types
- [x] 4.3 Update `buildIdempotencyKey` for batched SEEN types — use a hash of sorted TxIDs + callbackURL as idempotency key
- [x] 4.4 Update `dedupKeyForMessage` for batched SEEN types — use subtreeID or a hash of the TxIDs set

## 5. Subtree Processor Tests

- [x] 5.1 Update `TestHandleMessage` tests in `internal/subtree/processor_test.go` (or create if missing) to verify one SEEN_ON_NETWORK message per callbackURL with `TxIDs` array
- [x] 5.2 Add test: multiple registered txids for same callbackURL → single batched message
- [x] 5.3 Add test: registered txids across different callbackURLs → separate batched messages
- [x] 5.4 Add test: no registered txids → no callbacks published
- [x] 5.5 Add test: SEEN_MULTIPLE_NODES batching — multiple txids reach threshold for same callbackURL → single batched message
- [x] 5.6 Add test: SEEN_MULTIPLE_NODES — some txids below threshold → only threshold-reached txids in batch

## 6. Callback Delivery Tests

- [x] 6.1 Update `TestDeliverCallback_SeenOnNetwork` to use `TxIDs` array and verify `txids` field in HTTP payload
- [x] 6.2 Add test: batched SEEN_ON_NETWORK delivery payload contains correct `txids` array
- [x] 6.3 Update idempotency key and dedup key tests for batched SEEN types

## 7. Integration Verification

- [x] 7.1 Run `go build ./...` — clean compile
- [x] 7.2 Run `go test ./...` — all tests pass
- [x] 7.3 Verify STUMP and BLOCK_PROCESSED callbacks are unaffected by changes

## 8. Arcade Integration Prompt

- [x] 8.1 Create `arcade-callback-change-prompt.md` in the change directory with a detailed prompt for the Arcade team describing the contract change: `txids` array field, affected callback types, example payloads, and backward compatibility notes
