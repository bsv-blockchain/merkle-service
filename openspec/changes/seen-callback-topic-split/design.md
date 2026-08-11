## Context

`internal/callback/delivery.go` runs a single Kafka consumer over
`kafka.callbackTopic`. `handleMessage` is synchronous by design (F-021
durability contract: return nil only after a durable terminal state), so a
partition worker is a strictly serial lane. On the scale-1 Redpanda cluster
`callback` is 1 partition / 3 replicas (`rpk topic list`: `callback  1  3`),
which means the entire service has exactly one such lane.

`callback` is deliberately narrow. `docs/block-processed-completeness.md`
depends on single-partition ordering to guarantee a block's `BLOCK_PROCESSED`
is delivered after all of its `STUMP`s, and widening it is explicitly deferred
until arcade's `expectedSubtreeIndices` completeness check is verified
end-to-end (rollout step 4). So "just add partitions to `callback`" is not
available.

The mixed traffic is the problem, not the width:

- STUMP delivery is ~545 KB per message (`bodyCacheMaxEntries`' comment sizes
  it) and fans out per subscriber per subtree, with a blob fetch and an
  Aerospike dedup round-trip each.
- SEEN callbacks are small batched JSON and are what advances a transaction out
  of `ACCEPTED_BY_NETWORK` in arcade.

Measured 2026-08-10: ACCEPTED→SEEN median ~3.2s, p95 ~11.8s, worst ~55s, with
the arcade ACCEPTED backlog spiking to ~48k during block bursts.

## Goals / Non-Goals

**Goals**

- Remove head-of-line blocking of SEEN callbacks by block-callback traffic.
- Leave the `callback` topic — and therefore the BLOCK_PROCESSED-after-STUMPs
  barrier — bit-for-bit unchanged.
- Ship inert: no behavior change until an operator sets one config key; rollback
  is a config flip, not a revert.

**Non-Goals**

- Widening the `callback` topic (still blocked on step 4 of
  `docs/block-processed-completeness.md`).
- Changing the callback message format, dedup semantics, HTTP bodies, or the
  DLQ topology.
- Splitting STUMP away from BLOCK_PROCESSED — they must stay co-partitioned.
- Any change in arcade.

## Decisions

### Split by callback TYPE onto a second topic, not by widening the existing one

SEEN callbacks and block callbacks have no ordering relationship with each
other, so they do not need to share a topic at all. Giving SEEN its own topic
plus its own consumer group yields an independent set of partition workers.
The `callback` topic keeps exactly the traffic whose ordering matters, at
exactly the width that ordering requires.

**Alternative considered**: widen `callback` and rely on the partition key to
keep each `(subtree, callbackURL)` scope in order. Rejected — that is precisely
the change `docs/block-processed-completeness.md` defers, and it would put the
BLOCK_PROCESSED barrier at risk to fix an unrelated latency problem.

**Alternative considered**: keep one topic and make delivery concurrent within a
partition. Rejected — the synchronous-handler design is what provides the F-021
durability contract and the implicit STUMP → BLOCK_PROCESSED gate; unwinding it
is a much larger, riskier change.

### One helper, `SeenCallbackTopic()`, used by BOTH sides

The failure mode of a topic split is a producer/consumer disagreement: SEEN
callbacks published to a topic nobody consumes, with no error anywhere — every
watched transaction just silently strands at `ACCEPTED_BY_NETWORK`. Making a
single `KafkaConfig.SeenCallbackTopic()` the only expression of "where do SEEN
callbacks live", called from both `internal/subtree/processor.go` and
`internal/callback/delivery.go`, makes that divergence unrepresentable.

The same helper provides the inertness guarantee: empty ⇒ returns
`CallbackTopic` ⇒ producer and consumer both collapse back onto one topic, and
`Init` skips creating the second consumer/producer entirely.

### Retries MUST go back to the SOURCE topic

This is the sharpest edge in the change. `republishForRetry` previously used
one retry producer hard-wired to `CallbackTopic`. Left alone, a failed SEEN
delivery would be republished onto `callback` — so every retried SEEN callback
lands back in the blocked lane, and the fix silently regresses for exactly the
traffic that already failed once.

`kafka.Message` already carries `Topic`, so the source topic is threaded
`handleMessage → processDelivery → scheduleRetryOrDLQ → republishForRetry` and
`retryProducers` is keyed by source topic. `retryProducerFor` falls back to the
`callback` producer for an unknown/empty topic (pre-split behavior), and
`republishForRetry` returns an error rather than acking if no producer resolves
at all — a lost callback is worse than a redelivered one.

**Alternative considered**: give each consumer a handler closure that captures
its own retry producer. Equivalent, but threading the topic keeps one handler
and makes the routing decision explicit and directly testable.

### `TopicPartitions()` keys off the raw field, never the helper

`TopicPartitions()` adds the SEEN topic only when `CallbackSeenTopic != ""`. If
it used `SeenCallbackTopic()` instead, then in fallback mode the helper returns
`CallbackTopic` and the map would silently widen `callback` to 12 partitions —
shattering the ordering barrier as a side effect of a *disabled* feature. This
is the single highest-risk line in the change; it carries an inline comment and
a dedicated test (`callback` must remain ABSENT from the map in both modes).

### Cross-partition SEEN ordering is safe (default 12 partitions)

SEEN messages use `SubtreeHash` in their partition key
(`CallbackTopicMessage.PartitionKey()`), so `SEEN_ON_NETWORK` and
`SEEN_MULTIPLE_NODES` for the same txid can key on different subtrees and land
on different partitions — `SEEN_MULTIPLE_NODES` may therefore arrive first.

Verified safe in arcade: the status lattice is monotonic and the downgrade is
blocked in three independent places.

- `arcade/models/transaction.go:190-206` — `SEEN_ON_NETWORK`'s
  `DisallowedPreviousStatuses()` **contains** `StatusSeenMultipleNodes`, while
  `SEEN_MULTIPLE_NODES`' list does **not** contain `SEEN_ON_NETWORK`. The
  forward step is allowed; the reverse is not.
- `arcade/models/transaction.go:245-255` — `CanTransitionFrom` short-circuits
  `prev == s` (idempotent) and otherwise rejects any disallowed predecessor.
- `arcade/services/api_server/handlers.go:344-353` — `SEEN_ON_NETWORK` and
  `SEEN_MULTIPLE_NODES` funnel into one `applySeenCallback` body; the
  tracker prefilter (`:386-394`) and the post-write gate (`:444-451`) both drop
  a non-superseding update and publish no downstream event.
- Store layer enforces it independently: Pebble
  (`arcade/store/pebble/pebble.go:465-475`, under a per-txid shard lock),
  Postgres (`arcade/store/postgres/postgres.go:490-495` and `:425` — the guard
  is inside the UPDATE's WHERE clause, atomic), Aerospike
  (`arcade/store/aerospike/aerospike.go:433-438`, read-then-CAS on generation).
- Pinned by test: `arcade/models/transaction_test.go:164`
  `{StatusSeenMultipleNodes, StatusSeenOnNetwork, "single-peer downgrade"}` in
  the must-be-rejected table.

`REJECTED` / `DOUBLE_SPEND_ATTEMPTED` do not traverse this path at all — the
merkle callback envelope has exactly four types
(`arcade/models/callback.go:6-15`) and neither is one of them.

Because the ordering is a genuine no-op rather than merely tolerable, the SEEN
topic is defaulted to 12 partitions rather than 1.

### Dedup and the seen counter are not coupled to the topic

- Dedup keys on `(dedupKeyForMessage(msg), callbackURL, string(msg.Type))` —
  the callback TYPE is part of the key and the topic is not
  (`internal/callback/delivery.go`, `processDelivery`). Moving a message to a
  different topic cannot change its dedup identity.
- The SEEN threshold counter is producer-side: `SeenCounter.BatchIncrement` runs
  in the subtree-fetcher before any callback is published
  (`internal/subtree/processor.go`), so `SEEN_MULTIPLE_NODES` is decided before
  the topic is even chosen.

### The DLQ stays shared

One `callback-dlq` for both topics. A dead letter is a dead letter regardless of
which topic it arrived on, and the DLQ is not on any latency path.

## Rollout

Order matters, because the consumer is what creates the topic and the old
consumer is type-agnostic:

1. Deploy `callback-delivery` with `callbackSeenTopic` set. It creates and
   subscribes to the new topic; nothing is published there yet, so it idles.
   (Where topics are GitOps-managed, create the Topic CRD first.)
2. Deploy `subtree-fetcher` with the same value. SEEN callbacks begin flowing to
   the new topic. Any SEEN messages still in flight on `callback` drain through
   the old consumer, which handles every type.
3. Rollback: unset the key and redeploy in the reverse order (producer first).

## Risks

| Risk | Mitigation |
|---|---|
| Producer/consumer topic disagreement | Single `SeenCallbackTopic()` helper used by both; test asserts fallback identity |
| SEEN retries land back on the blocked topic | `retryProducers` keyed by source topic + tests on both republish call sites |
| `TopicPartitions()` accidentally widens `callback` | Keyed off the raw field, inline comment, test asserts `callback` absent in both modes |
| Cross-partition SEEN reorder | Verified monotonic in arcade (evidence above) |
| Broker rejects large SEEN batches on a new topic | Topic CRD sets `max.message.bytes: "10485760"`; a 5000-txid chunk is ~335 KB |
| Enabling producer before consumer | Documented rollout order in `config.yaml`, configmap and PR body |
