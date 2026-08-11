# unified-callback-topic (delta)

## MODIFIED Requirements

### Requirement: Unified callback Kafka topic

Callback events SHALL by default be published to a single `callback` Kafka
topic (configurable via `kafka.callbackTopic`). The `stumps` topic SHALL no
longer be used for callback delivery.

The system SHALL support an OPTIONAL split in which `SEEN_ON_NETWORK` and
`SEEN_MULTIPLE_NODES` are published to a dedicated topic
(`kafka.callbackSeenTopic`, env `KAFKA_CALLBACK_SEEN_TOPIC`) and consumed by a
second, independent consumer group (`<consumerGroup>-callback-seen`). When
`kafka.callbackSeenTopic` is empty the system SHALL behave exactly as the
unified-topic case. The split exists because `callback` is a single serial
delivery lane: ~545 KB `STUMP` deliveries, fanned out per subscriber per
subtree, otherwise head-of-line-block the small latency-sensitive SEEN
callbacks queued behind them.

`STUMP` and `BLOCK_PROCESSED` SHALL always remain together on
`kafka.callbackTopic`, which SHALL remain at 1 partition, so single-partition
ordering keeps `BLOCK_PROCESSED` behind its `STUMP`s (see
`docs/block-processed-completeness.md`).

Producers and consumers SHALL resolve the SEEN topic through a single shared
accessor (`KafkaConfig.SeenCallbackTopic()`) so the two sides cannot disagree —
a disagreement would publish SEEN callbacks to a topic nobody consumes, with no
error surfaced anywhere.

#### Scenario: Callback topic configured, no SEEN split

- **WHEN** the service starts with `kafka.callbackTopic` set to `callback` and
  `kafka.callbackSeenTopic` empty
- **THEN** all producers SHALL publish callback messages to the `callback`
  topic AND all consumers SHALL consume from the `callback` topic

#### Scenario: SEEN split enabled

- **WHEN** the service starts with `kafka.callbackSeenTopic` set to
  `callback-seen`
- **THEN** the subtree-fetcher SHALL publish `SEEN_ON_NETWORK` and
  `SEEN_MULTIPLE_NODES` to `callback-seen` AND the block services SHALL
  continue publishing `STUMP` and `BLOCK_PROCESSED` to `callback` AND the
  delivery service SHALL run one consumer per topic

#### Scenario: Shared callback topic is never widened

- **WHEN** the partition map used to create and grow topics is computed, with
  or without `kafka.callbackSeenTopic` configured
- **THEN** `kafka.callbackTopic` SHALL be absent from that map (created at 1
  partition) AND `kafka.callbackSeenTopic` SHALL appear in it ONLY when it is
  explicitly configured

#### Scenario: Dead-letter queue topic

- **WHEN** a callback message exceeds max retry attempts
- **THEN** the message SHALL be published to the `callback-dlq` topic
  (configurable via `kafka.callbackDlqTopic`), regardless of which callback
  topic it was consumed from

#### Scenario: A rejected-as-too-large callback is not dead-lettered on the first attempt

- **WHEN** a callback delivery is rejected because its request body exceeded
  the receiver's inbound size limit
- **THEN** the message SHALL be republished for retry while its retry budget
  allows, and SHALL NOT be published to `callback-dlq` before that budget is
  exhausted

## ADDED Requirements

### Requirement: Oversize callback bodies are a distinct, recoverable failure class

A callback delivery rejected because the request body exceeded the receiver's
inbound size limit SHALL be classified as an **oversize** failure — a class
distinct from both a retryable transport failure and a permanent failure.

The system SHALL treat `413 Request Entity Too Large` as oversize and SHALL
NOT treat it as a non-retryable 4xx. Every other non-retryable 4xx
(400, 401, 403, 404, 405, 410, 415, 422) asserts that the request is invalid
and always will be; 413 asserts only that the request is too large for the
receiver's CURRENT configuration, which an operator can change without
altering the message. Conflating the two sent a `STUMP` callback to
`callback-dlq` with zero retry attempts on 2026-08-11 (dev-ovh-1, 1000 TPS),
after which the receiver could never complete the block's expected-STUMP set
and every transaction in that block failed to reach `MINED`.

An oversize failure SHALL take the ordinary retry ladder, so that raising the
receiver's limit heals the affected block with no further operator action. It
SHALL NOT count toward the per-callback-URL circuit breaker: a receiver that
rejects an oversize body is demonstrably reachable, and disabling a
deployment's single registered callback URL would stop delivery of every
callback type for every transaction, not only the oversize one.

Where a receiver's limit is known, the system SHALL support a configured
pre-flight limit (`callback.maxBodyBytes`, env `CALLBACK_MAX_BODY_BYTES`) and
SHALL NOT send a body exceeding it. The default SHALL be 0 (disabled), because
the sender cannot know an arbitrary receiver's limit and a non-zero default
would refuse bodies the receiver would have accepted.

#### Scenario: Receiver rejects a STUMP body as too large

- **WHEN** a `STUMP` callback delivery receives `413 Request Entity Too Large`
  and retry attempts remain
- **THEN** the message SHALL be republished to its source topic with an
  incremented retry count AND SHALL NOT be published to `callback-dlq` AND the
  per-callback-URL circuit breaker SHALL NOT be advanced

#### Scenario: Oversize retries are exhausted

- **WHEN** an oversize callback exhausts its retry budget
- **THEN** it SHALL be published to `callback-dlq` AND the per-callback-URL
  circuit breaker SHALL still NOT be advanced AND the event SHALL be recorded
  under an outcome distinct from an ordinary dead-lettered callback

#### Scenario: Pre-flight limit configured

- **WHEN** `callback.maxBodyBytes` is greater than zero and a built callback
  body exceeds it
- **THEN** the request SHALL NOT be sent AND the delivery SHALL fail as an
  oversize failure

#### Scenario: Pre-flight limit unset

- **WHEN** `callback.maxBodyBytes` is zero
- **THEN** no local size limit SHALL be applied and the body SHALL be sent,
  leaving the receiver to enforce its own limit

### Requirement: Oversize callback failures are observable

An oversize callback failure SHALL be reported at ERROR level naming, at
minimum, the block hash, the subtree index and the size in bytes of the body
that was rejected or refused — the three facts required to identify which
block cannot be finalized and by how much its payload overshot.

The system SHALL expose a metric outcome for oversize rejections, and a
SEPARATE metric outcome for an oversize failure that has exhausted its retry
budget, so that "payloads are approaching the limit" and "a block cannot be
finalized" can be alerted on independently.

The system SHALL additionally warn when a built callback body exceeds a fixed
early-warning threshold, even when that delivery succeeds, so that payload
growth is visible before it becomes a delivery failure.

#### Scenario: Oversize rejection is logged and counted

- **WHEN** a callback delivery fails as oversize
- **THEN** an ERROR SHALL be emitted carrying the block hash, subtree index
  and body size AND the oversize metric outcome SHALL be incremented

#### Scenario: A stranded block is distinguishable

- **WHEN** an oversize callback exhausts its retry budget
- **THEN** a distinct ERROR SHALL state that the block cannot be finalized
  AND a distinct metric outcome SHALL be incremented

#### Scenario: Body size approaching the limit

- **WHEN** a callback body exceeds the early-warning threshold but the
  delivery succeeds
- **THEN** a WARN SHALL be emitted naming the block hash, subtree index and
  body size
