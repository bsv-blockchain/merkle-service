# Give SEEN callbacks their own Kafka topic

## Why

`callback.DeliveryService` consumes ONE topic (`kafka.callbackTopic`) and
delivers strictly serially. On the scale-1 Redpanda cluster that topic is
verified live at **1 partition** (`rpk topic list` → `callback  1  3`), so the
consumer group runs exactly **one** in-order partition worker for it.

That single serial lane carries two workloads with wildly different shapes:

| Traffic | Producer | Per-message work |
|---|---|---|
| `SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` | `internal/subtree/processor.go` | small JSON body, one HTTP POST |
| `STUMP` / `BLOCK_PROCESSED` | `internal/block/subtree_worker.go`, `internal/block/processor.go` | ~545 KB body + blob fetch + Aerospike dedup + one HTTP POST **per subscriber per subtree** |

Under sustained load with block processing running, the STUMP fan-out
head-of-line-blocks every SEEN callback queued behind it. Measured live
2026-08-10, downstream in arcade:

- ACCEPTED_BY_NETWORK → SEEN latency: **median ~3.2s, p95 ~11.8s, worst ~55s**
- ACCEPTED backlog spiking to **~48k** transactions during block bursts

Transactions strand at `ACCEPTED_BY_NETWORK` purely because the SEEN callback
that would advance them is stuck behind a block's STUMP flood.

## What Changes

- New **optional** config `kafka.callbackSeenTopic` (env
  `KAFKA_CALLBACK_SEEN_TOPIC`) plus `kafka.callbackSeenPartitions` (default 12).
  Empty ⇒ current single-topic behavior, so the change is **inert until
  configured** and rollback is a config flip.
- New helper `KafkaConfig.SeenCallbackTopic()` (dedicated topic when set, else
  `callbackTopic`). **Both** the producer and the consumer route through this
  one helper so the two sides can never disagree about where SEEN lives.
- The subtree-fetcher's callback producer targets `SeenCallbackTopic()`.
  `internal/block/*` is untouched — STUMP / BLOCK_PROCESSED stay on `callback`,
  so the BLOCK_PROCESSED-after-STUMPs barrier
  (`docs/block-processed-completeness.md`) is preserved exactly.
- `callback.DeliveryService` grows a **second, independent consumer** on the
  SEEN topic with consumer group `<group>-callback-seen`, and **per-source-topic
  retry producers**. A retry now republishes to the topic the message was
  consumed FROM — a single retry producer pinned to `callback` would drag every
  retried SEEN callback back onto the blocked topic and silently restore the bug.
- `TopicPartitions()` maps the SEEN topic **only when `callbackSeenTopic` is
  non-empty**, keyed off the raw field rather than the fallback helper — keying
  it off the helper would silently widen `callback` itself.

## Capabilities

### New Capabilities

_(none — the callback contract, message format, dedup keys and HTTP bodies are
unchanged; only the Kafka transport topology changes)_

### Modified Capabilities

- `unified-callback-topic`: the "all callback events on a single topic"
  requirement is relaxed to an optional type-based split. The unified topic
  remains the default and the fallback; STUMP / BLOCK_PROCESSED are still
  required to share one topic so their ordering barrier holds.

## Impact

- **`internal/config/config.go` / `config.yaml` / `deploy/k8s/configmap.yaml`**:
  `CallbackSeenTopic`, `CallbackSeenPartitions`, `SeenCallbackTopic()`,
  `TopicPartitions()` + `TopicRetention()` coverage, viper defaults and env
  bindings.
- **`internal/subtree/processor.go`**: one-line producer retarget + init log field.
- **`internal/callback/delivery.go`**: `consumers []*kafka.Consumer`,
  `retryProducers map[string]*kafka.Producer`, `retryProducerFor`, source topic
  threaded through `handleMessage → processDelivery → scheduleRetryOrDLQ →
  republishForRetry`, `Start`/`Stop`/`Health` cover both.
- **`internal/kafka/producer.go`**: `Producer.Topic()` accessor so routing is
  assertable in tests.
- **Infra (separate repo, `bsva-infra-flux`)**: a `callback-seen` Topic CRD is
  required before enabling the config, with `max.message.bytes: "10485760"`
  (a 5000-txid batched SEEN chunk is ~335 KB).
- **No changes** to callback message format, dedup keys, HTTP bodies, the DLQ
  topic, or arcade.
