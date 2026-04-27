## Context

The merkle-service consumes subtree and block announcements from Kafka (sourced from libp2p P2P topics). Multiple P2P nodes broadcast the same subtree/block, resulting in duplicate Kafka messages. Currently:

- **Subtree processor**: Processes every subtree message, emitting SEEN_ON_NETWORK callbacks each time. The seen counter is incremented per message, not per unique subtree, causing premature SEEN_MULTIPLE_NODES thresholds.
- **Block processor**: Processes every block message. Since all nodes announce the same block, MINED callbacks and STUMPs are emitted N times for N nodes.
- **Callback delivery**: No tracking of whether a specific txid/callbackURL/status combination has already been delivered.
- **Kafka consumer**: Uses at-least-once semantics (offset committed only on success). Crashes or rebalances cause legitimate reprocessing.

The system needs deduplication at two layers: (1) message-level dedup to skip already-processed subtrees/blocks, and (2) callback-level dedup to guarantee each txid/callbackURL receives exactly one callback per status type.

## Goals / Non-Goals

**Goals:**
- Deduplicate subtree and block message processing so each unique hash is successfully processed at most once
- Allow retries when processing fails (bad DataHub URL, network error) — only skip after confirmed success
- Ensure each txid/callbackURL pair receives exactly one SEEN_ON_NETWORK, one SEEN_MULTIPLE_NODES, and one MINED callback
- Make the seen counter idempotent — duplicate subtree announcements for the same txid don't inflate the count
- Add idempotency keys to callback deliveries so downstream consumers can detect retries

**Non-Goals:**
- Exactly-once Kafka semantics (transactional producer/consumer) — too complex, not needed with idempotent processing
- Distributed dedup across multiple processor instances — single-instance in-memory dedup is sufficient for MVP; Aerospike-backed dedup can be added later if needed
- Deduplication of the P2P layer itself — we dedup at the Kafka consumer level

## Decisions

### 1. In-memory processed-hash sets for message-level dedup

**Decision**: Use in-memory `map[string]struct{}` with a bounded size (LRU eviction) in both the subtree and block processors to track successfully-processed hashes.

**How it works**:
- Before processing a subtree/block message, check if the hash is in the processed set
- If found → skip (log at debug level), mark Kafka offset as consumed
- If not found → process normally. On success, add to processed set and mark offset
- On failure → do NOT add to set, do NOT mark offset. The message will be retried.

**Alternatives considered**:
- **Aerospike-backed dedup**: More durable but adds latency to every message. The processed set is ephemeral — if the process restarts, re-processing a few messages is acceptable because callback-level dedup (Decision 3) provides the safety net.
- **Kafka consumer group semantics only**: Insufficient because multiple P2P nodes produce duplicate messages with different Kafka offsets.

**Trade-off**: On restart, messages received between the last committed offset and shutdown will be reprocessed. This is acceptable because callback-level dedup prevents duplicate deliveries.

### 2. Idempotent seen counter using Aerospike sets

**Decision**: Change the seen counter from a simple atomic increment to a set-based approach using Aerospike CDT (Collection Data Types). Instead of `count: int`, store `subtrees: list[string]` per txid.

**How it works**:
- `Increment(txid, subtreeID)` uses `ListAppendWithPolicyOp` with `ListWriteFlagsAddUnique|ListWriteFlagsNoFail` to add the subtreeID to the list
- Then reads the list size to get the unique count
- `ThresholdReached` = list size equals threshold AND the append actually added a new element (list grew)

**Why**: The same subtree announced multiple times won't increase the count. Only distinct subtree IDs contribute. This is a single atomic Aerospike operation.

**Alternative considered**:
- **Bloom filter per txid**: More memory-efficient but doesn't provide exact counts and can't tell if an element was already present.

### 3. Aerospike-backed callback delivery dedup

**Decision**: Before delivering a callback, check an Aerospike set keyed by `{txid}:{callbackURL}:{statusType}`. If the record exists, skip delivery. On successful delivery, write the record.

**How it works**:
- Key: composite of txid + callbackURL hash + status (SEEN_ON_NETWORK | SEEN_MULTIPLE_NODES | MINED)
- Before HTTP POST: check if key exists → if yes, skip and ack the Kafka message
- After successful HTTP POST: write the key with a TTL (e.g., 24 hours)
- The TTL ensures old records are cleaned up automatically

**Why Aerospike, not in-memory**: Callback delivery may run across multiple instances and must survive restarts. A missed dedup here means a duplicate callback to the customer.

**Alternative considered**:
- **In-memory only**: Would lose state on restart, allowing duplicate callbacks during the restart window. Unacceptable for the delivery guarantee.
- **Dedup at the stumps producer (before Kafka)**: Moves dedup earlier but doesn't protect against Kafka redelivery or callback delivery restarts.

### 4. Idempotency key in callback HTTP requests

**Decision**: Add an `X-Idempotency-Key` header to callback HTTP POST requests, formatted as `{txid}:{statusType}`. This allows callback receivers to implement their own dedup if needed.

**Why**: Even with our best-effort dedup, network issues could cause a callback to be delivered but the ack lost. The idempotency key lets the receiver safely ignore retries.

## Risks / Trade-offs

- **[In-memory dedup lost on restart]** → Callback-level Aerospike dedup provides the safety net. Brief duplicate processing on restart is acceptable (just wastes compute, no duplicate callbacks).

- **[Aerospike callback dedup adds latency]** → One additional Aerospike read per callback delivery. At ~1ms per read, this is negligible compared to the HTTP POST (~100ms+).

- **[Memory usage for processed-hash sets]** → Bounded by LRU eviction. A 100K-entry map of 64-char hex strings uses ~10MB. Configurable via config.yaml.

- **[Set-based seen counter uses more storage than integer]** → Each txid stores a list of subtree hashes instead of a count. With typical threshold=3, this is ~3 * 64 bytes per txid — negligible.

- **[TTL on callback dedup records]** → If TTL expires and a very late duplicate arrives, it could be redelivered. 24h TTL is generous given blocks are processed within minutes.
