## Context

The subtree processor (`internal/subtree/processor.go`) currently emits one `CallbackTopicMessage` per registered txid for both SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES. For a subtree with 1,000 registered txids all belonging to the same Arcade instance, this produces 1,000 Kafka messages and 1,000 HTTP POST deliveries. Arcade doesn't need individual notification per txid — it just needs the list.

We recently changed STUMP callbacks to one-per-(callbackURL, subtree). This change applies the same pattern to SEEN callbacks, grouping txids by callbackURL and emitting one batched message per callbackURL per subtree.

## Goals / Non-Goals

**Goals:**
- Reduce SEEN_ON_NETWORK callbacks from N-per-txid to 1-per-callbackURL-per-subtree
- Reduce SEEN_MULTIPLE_NODES callbacks from N-per-txid to 1-per-callbackURL-per-subtree
- Carry a `txids` array in the callback payload so Arcade gets all txids in one request
- Provide Arcade a clear prompt describing the contract change on their end

**Non-Goals:**
- Cross-subtree batching (accumulating across multiple subtrees for one block) — each subtree emits independently
- Changing the STUMP or BLOCK_PROCESSED callback flow
- Modifying the seen counter logic (per-txid threshold check remains)

## Decisions

### 1. Add `TxIDs []string` to `CallbackTopicMessage`

**Choice**: Add a new `TxIDs` field alongside existing `TxID`.

**Rationale**: The existing `TxID` field is used by other code paths and `omitempty` handles the empty case. Adding `TxIDs` keeps backward compatibility — old messages with `TxID` still decode. Batched SEEN messages use `TxIDs`; the scalar `TxID` remains for any future single-txid use.

**Alternative considered**: Repurpose `TxID` as a comma-delimited string — rejected as fragile and breaks type safety.

### 2. Group-then-emit in subtree processor

**Choice**: Restructure `handleMessage` to first collect all registered txids grouped by callbackURL, then emit one batched SEEN_ON_NETWORK per callbackURL. Then run seen-counter checks, collect threshold-reached txids grouped by callbackURL, and emit one batched SEEN_MULTIPLE_NODES per callbackURL.

**Rationale**: The current `emitSeenCallbacks(txid, subtreeID)` loop calls `registrationStore.Get(txid)` individually per txid — but `findRegisteredTxids` already does a `BatchGet` that returns `map[txid][]callbackURL`. We can invert the map to `map[callbackURL][]txid` directly from the batch result, avoiding redundant per-txid lookups.

**Alternative considered**: Keep per-txid emission but deduplicate at the delivery layer — rejected because it doesn't reduce Kafka message volume.

### 3. Delivery payload carries `txids` array

**Choice**: Add `TxIDs []string` to `callbackPayload` (JSON field `txids`). For SEEN callbacks, populate `txids` from the message's `TxIDs` field.

**Rationale**: Clean contract. Arcade parses one JSON array instead of N separate HTTP requests.

### 4. Partition key changes from txid to callbackURL hash

**Choice**: Use `PublishWithHashKey(callbackURL, data)` for batched SEEN messages (same as STUMP/BLOCK_PROCESSED), instead of `Publish(txid, data)`.

**Rationale**: Since we're batching per callbackURL, the natural partition key is the callbackURL hash. This also aligns with the stumpGate mechanism in the delivery service, which coordinates per callbackURL.

## Risks / Trade-offs

- **Larger Kafka messages**: A single message now carries N txids instead of 1. For subtrees with thousands of registered txids for one callbackURL, messages could be large. → Mitigation: Kafka default max message size is 1MB; 10,000 txid hashes (64 chars each) ≈ 640KB, well within limits.
- **All-or-nothing delivery**: If the batched HTTP POST fails, all txids are retried together instead of individually. → Mitigation: This matches the STUMP pattern already in use. Partial delivery was never guaranteed.
- **Arcade contract change**: Arcade must update its callback handler to accept `txids` array. → Mitigation: Provide a detailed prompt for the Arcade team.
- **SEEN_MULTIPLE_NODES batching complexity**: Each txid independently reaches the threshold, so the threshold-reached set is a subset of registered txids. → Mitigation: After counter increments, collect only the threshold-reached txids, group by callbackURL, emit batched.
