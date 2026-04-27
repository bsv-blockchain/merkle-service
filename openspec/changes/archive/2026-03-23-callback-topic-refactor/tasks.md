## 1. Message Types & Config

- [x] 1.1 Define `CallbackTopicMessage` struct in `internal/kafka/messages.go` with fields: `CallbackURL`, `Type`, `TxID`, `BlockHash`, `SubtreeIndex`, `Stump`, `RetryCount`, `NextRetryAt`
- [x] 1.2 Define `CallbackType` constants (`SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, `STUMP`, `BLOCK_PROCESSED`) in `internal/kafka/messages.go`
- [x] 1.3 Rename config fields `stumpsTopic` → `callbackTopic` and `stumpsDlqTopic` → `callbackDlqTopic` in `internal/config/config.go`
- [x] 1.4 Update `config.yaml` with new topic names (`callback`, `callback-dlq`)
- [x] 1.5 Update K8s configmap (`deploy/k8s/configmap.yaml`) with new topic names
- [x] 1.6 Add serialization helpers: `CallbackTopicMessage.Marshal()` and `UnmarshalCallbackTopicMessage()`

## 2. Subtree Processor — SEEN_ON_NETWORK & SEEN_MULTIPLE_NODES

- [x] 2.1 Update subtree processor to publish `CallbackTopicMessage` with `Type: SEEN_ON_NETWORK` to `callback` topic when registered txid found
- [x] 2.2 Update subtree processor to publish `CallbackTopicMessage` with `Type: SEEN_MULTIPLE_NODES` to `callback` topic when seen threshold reached
- [x] 2.3 Remove references to old `StumpsMessage` / `StatusSeenOnNetwork` / `StatusSeenMultiNodes` from subtree processor
- [x] 2.4 Update subtree processor tests to verify new message format on `callback` topic

## 3. Block Processor — Subtree Work Messages

- [x] 3.1 Add `SubtreeIndex` field to `SubtreeWorkMessage` so subtree workers know the subtree's position in the block
- [x] 3.2 Update block processor to populate `SubtreeIndex` when publishing subtree work messages
- [x] 3.3 Update block processor tests for `SubtreeIndex` field

## 4. Subtree Worker — STUMP Callbacks

- [x] 4.1 Update subtree worker to build `CallbackTopicMessage` with `Type: STUMP`, inline STUMP data, `BlockHash`, and `SubtreeIndex`
- [x] 4.2 Publish individual `CallbackTopicMessage` per registered txid to `callback` topic (instead of batched `StumpsMessage`)
- [x] 4.3 Remove `StumpRef`/`StumpRefs` usage from subtree worker — STUMP data is always inlined
- [x] 4.4 Update callback accumulator to store inline STUMP binary + subtree index per txid instead of cache references
- [x] 4.5 Update flush logic: read accumulated data and publish individual `CallbackTopicMessage` per txid
- [x] 4.6 Update subtree worker tests for new message format and inline STUMP data

## 5. Subtree Worker — BLOCK_PROCESSED Callback

- [x] 5.1 Update block completion handler to publish `CallbackTopicMessage` with `Type: BLOCK_PROCESSED` and `BlockHash` to `callback` topic for each registered callback URL
- [x] 5.2 Remove old `StatusBlockProcessed` / `StumpsMessage` publishing for block completion
- [x] 5.3 Update block completion tests for new message format

## 6. Callback Delivery Service

- [x] 6.1 Update delivery service consumer to read from `callback` topic (was `stumps`)
- [x] 6.2 Update message deserialization to use `UnmarshalCallbackTopicMessage()`
- [x] 6.3 Build HTTP POST body from `CallbackTopicMessage` fields: extract `{type, txid, blockHash, subtreeIndex, stump}` as JSON
- [x] 6.4 Hex-encode `stump` field in HTTP body (matching Arcade's `HexBytes` type)
- [x] 6.5 Remove STUMP cache lookup logic (`StumpRef`/`StumpRefs` resolution) from delivery service
- [x] 6.6 Update retry republish to use `callback` topic (was `stumps`)
- [x] 6.7 Update DLQ publishing to use `callback-dlq` topic (was `stumps-dlq`)
- [x] 6.8 Update delivery service tests for new message format and HTTP body

## 7. Callback Dedup

- [x] 7.1 Update dedup key computation to use `CallbackType` values (`STUMP` instead of `MINED`)
- [x] 7.2 Handle `BLOCK_PROCESSED` dedup key: use `blockHash:callbackURL:BLOCK_PROCESSED` (no txid)
- [x] 7.3 Update dedup tests for new key format

## 8. Remove Stump Cache

- [x] 8.1 Remove `stump_cache` Aerospike set usage from subtree worker
- [x] 8.2 Remove `StumpRef`/`StumpRefs` fields from `StumpsMessage` (or remove `StumpsMessage` entirely if unused)
- [x] 8.3 Remove stump cache store (`internal/store/stump_cache.go`) if no longer referenced
- [x] 8.4 Remove stump cache config fields (`stumpCacheMode`, `stumpCacheTTLSec`) if no longer needed

## 9. Remove Old Message Type

- [x] 9.1 Remove `StumpsMessage` struct from `internal/kafka/messages.go` (verify no remaining references)
- [x] 9.2 Remove old `StatusType` constants (`StatusSeenOnNetwork`, `StatusSeenMultiNodes`, `StatusMined`, `StatusBlockProcessed`)
- [x] 9.3 Clean up any remaining imports or references to removed types across the codebase

## 10. Integration & Verification

- [x] 10.1 Verify all producers compile and publish to `callback` topic with correct `CallbackTopicMessage` format
- [x] 10.2 Verify delivery service consumes from `callback` topic and POSTs correct `CallbackMessage` JSON
- [x] 10.3 Run full test suite and fix any failures from the refactor
- [x] 10.4 Verify `go build ./...` succeeds with no compilation errors
- [x] 10.5 Update any remaining documentation or comments referencing `stumps` topic
