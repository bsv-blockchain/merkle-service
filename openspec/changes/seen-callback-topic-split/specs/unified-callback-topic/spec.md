# unified-callback-topic (delta)

## MODIFIED Requirements

### Requirement: Unified callback Kafka topic

Callback events SHALL by default be published to a single `callback` Kafka
topic (configurable via `kafka.callbackTopic`). The `stumps` topic SHALL no
longer be used for callback delivery.

The system SHALL support an OPTIONAL split in which `SEEN_ON_NETWORK` and
`SEEN_MULTIPLE_NODES` are published to a dedicated topic
(`kafka.callbackSeenTopic`, env `KAFKA_CALLBACK_SEEN_TOPIC`) and consumed by a
second, independent consumer group. When `kafka.callbackSeenTopic` is empty the
system SHALL behave exactly as the unified-topic case.

`STUMP` and `BLOCK_PROCESSED` SHALL always remain together on
`kafka.callbackTopic`, which SHALL remain at 1 partition, so that
single-partition ordering keeps `BLOCK_PROCESSED` behind its `STUMP`s (see
`docs/block-processed-completeness.md`).

Producers and consumers SHALL resolve the SEEN topic through a single shared
accessor (`KafkaConfig.SeenCallbackTopic()`), so the two sides cannot
disagree — a disagreement would publish SEEN callbacks to a topic nobody
consumes, with no error surfaced anywhere.

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
  delivery service SHALL run one consumer per topic, the SEEN one in consumer
  group `<consumerGroup>-callback-seen`

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

## ADDED Requirements

### Requirement: Retries republish to the source topic

A retry-eligible callback SHALL be republished to the SAME Kafka topic it was
consumed from. Republishing a `SEEN_*` retry onto `kafka.callbackTopic` would
place it back behind the `STUMP` / `BLOCK_PROCESSED` traffic the split exists to
escape, silently reintroducing the head-of-line blocking for callbacks that have
already failed once.

If no retry producer can be resolved for the source topic, the delivery service
SHALL return an error so the Kafka offset is NOT committed, rather than acking
and dropping the callback.

#### Scenario: SEEN retry stays on the SEEN topic

- **WHEN** a `SEEN_ON_NETWORK` message consumed from `callback-seen` fails
  delivery with retries remaining
- **THEN** it SHALL be republished to `callback-seen`, and NOT to `callback`

#### Scenario: Block callback retry stays on the callback topic

- **WHEN** a `STUMP` or `BLOCK_PROCESSED` message consumed from `callback` fails
  delivery with retries remaining
- **THEN** it SHALL be republished to `callback`

#### Scenario: Not-yet-due retry bounce

- **WHEN** a message whose `nextRetryAt` is further out than the in-process wait
  cap is consumed
- **THEN** it SHALL be republished, unchanged, to the topic it was consumed from

### Requirement: Cross-partition SEEN ordering is not required

The system SHALL NOT require ordering between `SEEN_ON_NETWORK` and
`SEEN_MULTIPLE_NODES` for the same txid, and the dedicated SEEN topic MAY
therefore be created with more than one partition. The two callbacks may key on
different subtree hashes and so land on different partitions, meaning
`SEEN_MULTIPLE_NODES` MAY be delivered first. The receiver's status lattice is
monotonic and idempotent — a lower status cannot supersede a higher one — so the
reordering is a no-op rather than a regression.

Callback deduplication SHALL remain keyed on `(txid/dedup key, callbackURL,
callback type)` and SHALL NOT depend on the source topic. The SEEN threshold
counter SHALL remain producer-side, evaluated before a callback is published.

#### Scenario: SEEN_MULTIPLE_NODES arrives before SEEN_ON_NETWORK

- **WHEN** both SEEN callbacks for a txid are delivered out of order
- **THEN** the receiver's final state SHALL be `SEEN_MULTIPLE_NODES` and the
  late `SEEN_ON_NETWORK` SHALL be discarded without regressing the status

#### Scenario: Dedup unaffected by the split

- **WHEN** the same callback is delivered from either callback topic
- **THEN** the dedup key SHALL be identical, so moving a callback type between
  topics neither suppresses nor duplicates a delivery
