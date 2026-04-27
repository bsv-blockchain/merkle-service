## Context

The Merkle Service delivers four callback types to Arcade: `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, `STUMP` (mined proof), and `BLOCK_PROCESSED`. Currently, all callbacks flow through the `stumps` Kafka topic using `StumpsMessage`, an internal struct with fields like `StumpData`, `StumpRef`, `StumpRefs`, and `StatusType`. The callback delivery service then transforms this into an ad-hoc JSON payload for HTTP delivery.

Arcade defines a `CallbackMessage` struct with fields: `type` (CallbackType), `txid`, `blockHash`, `subtreeIndex`, and `stump` (HexBytes). The current internal message format doesn't align with this contract — the delivery service must translate between formats, and the naming (`stumps` topic, `StumpsMessage`) is misleading since most callback types carry no STUMP data.

## Goals / Non-Goals

**Goals:**
- Unify all callback events onto a single `callback` Kafka topic with a message format that mirrors Arcade's `CallbackMessage`.
- Eliminate the translation layer in the delivery service — the Kafka message payload maps directly to the HTTP POST body.
- Rename config and code references from `stumps` to `callback` for clarity.
- Maintain existing retry, dedup, and batching behavior.

**Non-Goals:**
- Changing the callback delivery HTTP retry/backoff logic.
- Modifying Arcade's `CallbackMessage` type.
- Changing the STUMP building or merkle tree construction logic.
- Supporting backwards-compatible dual-topic consumption during migration.

## Decisions

### 1. Unified Kafka message struct aligned to CallbackMessage

**Decision**: Replace `StumpsMessage` with `CallbackTopicMessage` that wraps `CallbackMessage` fields plus delivery metadata (`callbackURL`, `retryCount`, `nextRetryAt`).

```go
type CallbackTopicMessage struct {
    CallbackURL string       `json:"callbackURL"`
    Type        string       `json:"type"`
    TxID        string       `json:"txid,omitempty"`
    BlockHash   string       `json:"blockHash,omitempty"`
    SubtreeIndex int         `json:"subtreeIndex,omitempty"`
    Stump       []byte       `json:"stump,omitempty"`
    RetryCount  int          `json:"retryCount,omitempty"`
    NextRetryAt time.Time    `json:"nextRetryAt,omitempty"`
}
```

**Rationale**: Embedding the `CallbackMessage` fields directly means the delivery service can extract `{type, txid, blockHash, subtreeIndex, stump}` without transformation. The delivery-specific fields (`callbackURL`, `retryCount`, `nextRetryAt`) are stripped before HTTP POST.

**Alternative considered**: Keeping `StumpsMessage` and adding a mapping layer. Rejected because it perpetuates naming confusion and adds unnecessary complexity.

### 2. Inline STUMP data instead of cache references

**Decision**: For `STUMP` callbacks, embed the STUMP binary directly in the Kafka message (`Stump` field) rather than using `StumpRef`/`StumpRefs` pointing to an Aerospike cache.

**Rationale**: Kafka messages can comfortably hold STUMP data (typically a few KB per subtree proof). Inlining removes the cache lookup dependency from the delivery service, simplifying the architecture and eliminating cache-miss retry logic. The `stump_cache` Aerospike set can be removed.

**Alternative considered**: Keeping the cache reference pattern. Rejected because it adds latency, complexity (cache miss handling), and an Aerospike dependency in the delivery service.

**Trade-off**: Slightly larger Kafka messages, but well within Kafka's default 1MB message limit.

### 3. STUMP type replaces MINED

**Decision**: Use `STUMP` as the callback type (matching Arcade's `CallbackStump` constant) instead of the internal `MINED` name.

**Rationale**: Direct alignment with Arcade's `CallbackType` constants. The `STUMP` name also better describes the payload — it carries a STUMP proof, not just a "mined" signal.

### 4. Batched STUMP callbacks become multiple messages

**Decision**: For blocks with many registered txids, publish one `CallbackTopicMessage` per txid (with `STUMP` type) rather than a single batched message with `TxIDs[]` and `StumpRefs[]`.

**Rationale**: The `CallbackMessage` contract has a single `txid` field, not an array. Each callback corresponds to one txid's proof. The subtree worker already iterates over registered txids — publishing individual messages is straightforward. Kafka partitioning by txid ensures ordering per transaction.

**Alternative considered**: Keeping batch messages and having the delivery service fan-out. Rejected because it adds complexity to the delivery service and doesn't match the Arcade contract.

**Trade-off**: More Kafka messages per block, but each is small and Kafka handles high throughput well at scale.

### 5. Topic rename: `stumps` → `callback`, `stumps-dlq` → `callback-dlq`

**Decision**: Rename topics and all config references.

**Rationale**: The topic carries all callback types, not just STUMPs. Clear naming prevents confusion as the system evolves.

## Risks / Trade-offs

- **[Breaking change: topic rename]** → Create new topics before deploying. Old `stumps` topic can be deleted after migration. No dual-consumption needed since this is a coordinated deploy.
- **[Increased message volume from de-batching]** → At scale (millions of txids), individual messages per txid increases Kafka throughput requirements. Mitigated by Kafka's inherent scalability and partition-level parallelism. Monitor consumer lag post-deploy.
- **[STUMP data inlined in Kafka]** → Larger messages. Mitigated by STUMPs being small (< 1KB typical). Monitor Kafka broker disk usage.
- **[Config field rename]** → Deployments with existing config files will break. Mitigated by updating K8s configmaps and documentation in the same release.

## Migration Plan

1. Create `callback` and `callback-dlq` topics in Kafka (all environments).
2. Deploy updated Merkle Service with new topic config.
3. Verify callbacks flow end-to-end with Arcade.
4. Delete old `stumps` and `stumps-dlq` topics.
