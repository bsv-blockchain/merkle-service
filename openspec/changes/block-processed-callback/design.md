## Context

The block processor (`internal/block/processor.go`) processes block announcements by fetching metadata from DataHub, dispatching subtree processing in parallel, and emitting `MINED` callbacks per callback URL group. After all subtrees complete, the processor marks the block in the dedup cache but sends no completion signal to downstream consumers.

Arcade instances receiving `MINED` callbacks with STUMPs cannot tell when all STUMPs for a block have arrived. They need a `BLOCK_PROCESSED` signal to begin BUMP construction.

The registration store is keyed by txid → callback URLs. There is no efficient way to enumerate all unique callback URLs across all registrations. A separate tracking mechanism is needed.

## Goals / Non-Goals

**Goals:**
- Emit a `BLOCK_PROCESSED` callback to all registered callback URLs after a block's subtrees are fully processed
- Track unique callback URLs in a lightweight registry for efficient enumeration
- Reuse the existing Kafka stumps topic and callback delivery pipeline
- Keep the callback payload minimal: status + block hash

**Non-Goals:**
- Guaranteeing ordered delivery of `BLOCK_PROCESSED` after all `MINED` callbacks (Kafka partitioning and consumer parallelism make this impractical; Arcade handles ordering by accumulating STUMPs per block hash)
- Per-txid dedup for `BLOCK_PROCESSED` (it's a per-block event, not per-txid)
- Tracking which Arcade instances care about which blocks (Arcade ignores irrelevant blocks)

## Decisions

### 1. Callback URL Registry as a dedicated Aerospike set

**Decision:** Maintain a separate Aerospike set (`callback-urls`) with a single record using a well-known key (e.g., `"__all_urls__"`) containing a CDT list of unique callback URL strings.

**Rationale:** The registration store is keyed by txid, making it impossible to efficiently scan all unique callback URLs. A dedicated registry avoids full-set scans. Using a single record with a CDT list with `ADD_UNIQUE` flag provides set semantics and atomic reads. The expected cardinality is low (tens to hundreds of Arcade instances, not millions).

**Alternative considered:** Aerospike secondary index scan on the registration set — rejected because secondary indexes on list bins are not efficient, and scanning millions of registration records to extract unique URLs is wasteful for a low-cardinality lookup.

**Alternative considered:** In-memory set built during registration — rejected because it would not survive restarts and requires synchronization across multiple instances.

### 2. Populate registry on registration

**Decision:** When `RegistrationStore.Add()` is called, also add the callback URL to the callback URL registry. This is a fire-and-forget CDT list append with `ADD_UNIQUE|NO_FAIL`.

**Rationale:** Registration is the only entry point for callback URLs. Piggy-backing on the existing registration path ensures the registry stays up-to-date without a separate maintenance loop. The `ADD_UNIQUE` flag prevents duplicates naturally.

### 3. Emit BLOCK_PROCESSED via existing stumps Kafka topic

**Decision:** After all subtrees for a block are processed, read all callback URLs from the registry and publish one `StumpsMessage` per URL with `StatusType=BLOCK_PROCESSED` and the block hash.

**Rationale:** Reuses the existing stumps topic and callback delivery pipeline (retries, DLQ, dedup). No new Kafka topics or consumers needed. The callback delivery service already handles routing by `callbackUrl`.

### 4. Dedup BLOCK_PROCESSED on blockHash instead of txid

**Decision:** For `BLOCK_PROCESSED` messages, use `blockHash` as the dedup key (instead of txid which is empty). The idempotency key becomes `blockHash:BLOCK_PROCESSED`.

**Rationale:** `BLOCK_PROCESSED` is a per-block event, not per-txid. The existing dedup store's `Exists`/`Record` methods accept a txid parameter — we reuse this field with the blockHash value since it serves the same purpose (unique identifier for dedup).

### 5. Skip BLOCK_PROCESSED emission when no registrations found

**Decision:** Only emit `BLOCK_PROCESSED` if at least one subtree in the block had registered txids (i.e., at least one `MINED` callback was emitted). This avoids broadcasting for blocks where no one cares.

**Rationale:** If no registrations matched in any subtree, no Arcade instance is expecting STUMPs for this block, so a completion signal is meaningless. The block processor already tracks whether any registrations were found.

## Risks / Trade-offs

- **Ordering:** `BLOCK_PROCESSED` may arrive at Arcade before all `MINED` callbacks, since messages go through Kafka and may be on different partitions. → Mitigation: Arcade must buffer STUMPs per block and treat `BLOCK_PROCESSED` as "no more STUMPs will come," using a short grace window or reconciliation.

- **Stale callback URLs in registry:** If an Arcade instance is decommissioned, its URL remains in the registry, receiving `BLOCK_PROCESSED` callbacks that will fail and hit the DLQ. → Acceptable for now; a TTL-based cleanup or explicit deregistration can be added later.

- **Single-record contention:** All registrations write to the same `__all_urls__` record. → Acceptable because registration is not on the hot path (it happens when users submit transactions, not during block processing), and CDT operations with `NO_FAIL` are fast.

- **Registry not populated for existing registrations:** Deploying this change will not backfill the registry with URLs from existing registrations. → Mitigation: The next registration from each Arcade instance will populate the registry. Alternatively, a one-time migration script can scan the registration set.
