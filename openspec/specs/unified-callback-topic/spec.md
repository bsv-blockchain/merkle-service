### Requirement: Unified callback Kafka topic
Callback events SHALL by default be published to a single `callback` Kafka topic (configurable via `kafka.callbackTopic`). The `stumps` topic SHALL no longer be used for callback delivery.

The system SHALL support an OPTIONAL split in which `SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` are published to a dedicated topic (`kafka.callbackSeenTopic`, env `KAFKA_CALLBACK_SEEN_TOPIC`) and consumed by a second, independent consumer group (`<consumerGroup>-callback-seen`). When `kafka.callbackSeenTopic` is empty the system SHALL behave exactly as the unified-topic case. The split exists because `callback` is a single serial delivery lane: ~545 KB `STUMP` deliveries, fanned out per subscriber per subtree, otherwise head-of-line-block the small latency-sensitive SEEN callbacks queued behind them.

`STUMP` and `BLOCK_PROCESSED` SHALL always remain together on `kafka.callbackTopic`, which SHALL remain at 1 partition, so single-partition ordering keeps `BLOCK_PROCESSED` behind its `STUMP`s (see `docs/block-processed-completeness.md`).

Producers and consumers SHALL resolve the SEEN topic through a single shared accessor (`KafkaConfig.SeenCallbackTopic()`) so the two sides cannot disagree — a disagreement would publish SEEN callbacks to a topic nobody consumes, with no error surfaced anywhere.

#### Scenario: Callback topic configured, no SEEN split
- **WHEN** the service starts with `kafka.callbackTopic` set to `callback` and `kafka.callbackSeenTopic` empty
- **THEN** all producers SHALL publish callback messages to the `callback` topic AND all consumers SHALL consume from the `callback` topic

#### Scenario: SEEN split enabled
- **WHEN** the service starts with `kafka.callbackSeenTopic` set to `callback-seen`
- **THEN** the subtree-fetcher SHALL publish `SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` to `callback-seen` AND the block services SHALL continue publishing `STUMP` and `BLOCK_PROCESSED` to `callback` AND the delivery service SHALL run one consumer per topic

#### Scenario: Shared callback topic is never widened
- **WHEN** the partition map used to create and grow topics is computed, with or without `kafka.callbackSeenTopic` configured
- **THEN** `kafka.callbackTopic` SHALL be absent from that map (created at 1 partition) AND `kafka.callbackSeenTopic` SHALL appear in it ONLY when it is explicitly configured

#### Scenario: Dead-letter queue topic
- **WHEN** a callback message exceeds max retry attempts
- **THEN** the message SHALL be published to the `callback-dlq` topic (configurable via `kafka.callbackDlqTopic`), regardless of which callback topic it was consumed from

#### Scenario: A rejected-as-too-large callback is not dead-lettered on the first attempt
- **WHEN** a callback delivery is rejected because its request body exceeded the receiver's inbound size limit
- **THEN** the message SHALL be republished for retry while its retry budget allows, and SHALL NOT be published to `callback-dlq` before that budget is exhausted

### Requirement: Oversize callback bodies are a distinct, recoverable failure class
A callback delivery rejected because the request body exceeded the receiver's inbound size limit SHALL be classified as an **oversize** failure — a class distinct from both a retryable transport failure and a permanent failure.

The system SHALL treat `413 Request Entity Too Large` as oversize and SHALL NOT treat it as a non-retryable 4xx. Every other non-retryable 4xx (400, 401, 403, 404, 405, 410, 415, 422) asserts that the request is invalid and always will be; 413 asserts only that the request is too large for the receiver's CURRENT configuration, which an operator can change without altering the message. Conflating the two sent a `STUMP` callback to `callback-dlq` with zero retry attempts on 2026-08-11 (dev-ovh-1, 1000 TPS), after which the receiver could never complete the block's expected-STUMP set and every transaction in that block failed to reach `MINED`.

An oversize failure SHALL take the ordinary retry ladder, so that raising the receiver's limit heals the affected block with no further operator action. It SHALL NOT count toward the per-callback-URL circuit breaker: a receiver that rejects an oversize body is demonstrably reachable, and disabling a deployment's single registered callback URL would stop delivery of every callback type for every transaction, not only the oversize one.

Where a receiver's limit is known, the system SHALL support a configured pre-flight limit (`callback.maxBodyBytes`, env `CALLBACK_MAX_BODY_BYTES`) and SHALL NOT send a body exceeding it. The default SHALL be 0 (disabled), because the sender cannot know an arbitrary receiver's limit and a non-zero default would refuse bodies the receiver would have accepted.

#### Scenario: Receiver rejects a STUMP body as too large
- **WHEN** a `STUMP` callback delivery receives `413 Request Entity Too Large` and retry attempts remain
- **THEN** the message SHALL be republished to its source topic with an incremented retry count AND SHALL NOT be published to `callback-dlq` AND the per-callback-URL circuit breaker SHALL NOT be advanced

#### Scenario: Oversize retries are exhausted
- **WHEN** an oversize callback exhausts its retry budget
- **THEN** it SHALL be published to `callback-dlq` AND the per-callback-URL circuit breaker SHALL still NOT be advanced AND the event SHALL be recorded under an outcome distinct from an ordinary dead-lettered callback

#### Scenario: Pre-flight limit configured
- **WHEN** `callback.maxBodyBytes` is greater than zero and a built callback body exceeds it
- **THEN** the request SHALL NOT be sent AND the delivery SHALL fail as an oversize failure

#### Scenario: Pre-flight limit unset
- **WHEN** `callback.maxBodyBytes` is zero
- **THEN** no local size limit SHALL be applied and the body SHALL be sent, leaving the receiver to enforce its own limit

### Requirement: Oversize callback failures are observable
An oversize callback failure SHALL be reported at ERROR level naming, at minimum, the block hash, the subtree index and the size in bytes of the body that was rejected or refused — the three facts required to identify which block cannot be finalized and by how much its payload overshot.

The system SHALL expose a metric outcome for oversize rejections, and a SEPARATE metric outcome for an oversize failure that has exhausted its retry budget, so that "payloads are approaching the limit" and "a block cannot be finalized" can be alerted on independently.

The system SHALL additionally warn when a built callback body exceeds a fixed early-warning threshold, even when that delivery succeeds, so that payload growth is visible before it becomes a delivery failure.

#### Scenario: Oversize rejection is logged and counted
- **WHEN** a callback delivery fails as oversize
- **THEN** an ERROR SHALL be emitted carrying the block hash, subtree index and body size AND the oversize metric outcome SHALL be incremented

#### Scenario: A stranded block is distinguishable
- **WHEN** an oversize callback exhausts its retry budget
- **THEN** a distinct ERROR SHALL state that the block cannot be finalized AND a distinct metric outcome SHALL be incremented

#### Scenario: Body size approaching the limit
- **WHEN** a callback body exceeds the early-warning threshold but the delivery succeeds
- **THEN** a WARN SHALL be emitted naming the block hash, subtree index and body size

### Requirement: Retries republish to the source topic
A retry-eligible callback SHALL be republished to the SAME Kafka topic it was consumed from. Republishing a `SEEN_*` retry onto `kafka.callbackTopic` would place it back behind the `STUMP` / `BLOCK_PROCESSED` traffic the split exists to escape, silently reintroducing head-of-line blocking for callbacks that have already failed once.

If no retry producer can be resolved for the source topic, the delivery service SHALL return an error so the Kafka offset is NOT committed, rather than acking and dropping the callback.

#### Scenario: SEEN retry stays on the SEEN topic
- **WHEN** a `SEEN_ON_NETWORK` message consumed from `callback-seen` fails delivery with retries remaining
- **THEN** it SHALL be republished to `callback-seen`, and NOT to `callback`

#### Scenario: Block callback retry stays on the callback topic
- **WHEN** a `STUMP` or `BLOCK_PROCESSED` message consumed from `callback` fails delivery with retries remaining
- **THEN** it SHALL be republished to `callback`

#### Scenario: Not-yet-due retry bounce
- **WHEN** a message whose `nextRetryAt` is further out than the in-process wait cap is consumed
- **THEN** it SHALL be republished, unchanged, to the topic it was consumed from

### Requirement: Cross-partition SEEN ordering is not required
The system SHALL NOT require ordering between `SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` for the same txid, and the dedicated SEEN topic MAY therefore be created with more than one partition (default 12, `kafka.callbackSeenPartitions`). The two callbacks may key on different subtree hashes and so land on different partitions, meaning `SEEN_MULTIPLE_NODES` MAY be delivered first. The receiver's status lattice is monotonic and idempotent — a lower status cannot supersede a higher one — so the reordering is a no-op rather than a regression.

Callback deduplication SHALL remain keyed on `(txid/dedup key, callbackURL, callback type)` and SHALL NOT depend on the source topic. The SEEN threshold counter SHALL remain producer-side, evaluated before a callback is published.

#### Scenario: SEEN_MULTIPLE_NODES arrives before SEEN_ON_NETWORK
- **WHEN** both SEEN callbacks for a txid are delivered out of order
- **THEN** the receiver's final state SHALL be `SEEN_MULTIPLE_NODES` and the late `SEEN_ON_NETWORK` SHALL be discarded without regressing the status

#### Scenario: Dedup unaffected by the split
- **WHEN** the same callback is delivered from either callback topic
- **THEN** the dedup key SHALL be identical, so moving a callback type between topics neither suppresses nor duplicates a delivery

### Requirement: CallbackTopicMessage format
Messages on the `callback` topic SHALL use a `CallbackTopicMessage` struct containing all fields from Arcade's `CallbackMessage` (`type`, `txid`, `blockHash`, `subtreeIndex`, `stump`) plus delivery metadata (`callbackURL`, `retryCount`, `nextRetryAt`).

#### Scenario: SEEN_ON_NETWORK message
- **WHEN** a registered txid is found in a subtree
- **THEN** the published message SHALL have `type` set to `SEEN_ON_NETWORK`, `txid` set to the transaction hash, and all other CallbackMessage fields empty

#### Scenario: SEEN_MULTIPLE_NODES message
- **WHEN** a registered txid is seen in subtrees meeting the configured threshold
- **THEN** the published message SHALL have `type` set to `SEEN_MULTIPLE_NODES`, `txid` set to the transaction hash, and all other CallbackMessage fields empty

#### Scenario: STUMP message
- **WHEN** a registered txid is found in a block subtree and the STUMP is built
- **THEN** the published message SHALL have `type` set to `STUMP`, `txid` set to the transaction hash, `blockHash` set to the block hash, `subtreeIndex` set to the subtree's index in the block, and `stump` set to the serialized STUMP binary

#### Scenario: BLOCK_PROCESSED message
- **WHEN** all subtrees in a block have been processed
- **THEN** the published message SHALL have `type` set to `BLOCK_PROCESSED`, `blockHash` set to the block hash, and `txid`, `subtreeIndex`, and `stump` fields empty

### Requirement: Inline STUMP data in Kafka messages
STUMP callback messages SHALL embed the serialized STUMP binary directly in the `stump` field of the Kafka message. The system SHALL NOT use cache references (`StumpRef`/`StumpRefs`) for STUMP delivery.

#### Scenario: STUMP data included in message
- **WHEN** a subtree worker builds a STUMP for a registered txid
- **THEN** the worker SHALL serialize the STUMP and set it as the `stump` field in the `CallbackTopicMessage`

#### Scenario: Delivery service reads STUMP
- **WHEN** the delivery service processes a `STUMP` type message
- **THEN** it SHALL read the `stump` field directly without any cache lookup

### Requirement: HTTP POST body matches CallbackMessage
The callback delivery service SHALL POST a JSON body containing exactly the `CallbackMessage` fields: `type`, `txid` (omitted if empty), `blockHash` (omitted if empty), `subtreeIndex` (omitted if zero), and `stump` (hex-encoded, omitted if empty).

#### Scenario: STUMP callback HTTP body
- **WHEN** delivering a `STUMP` callback
- **THEN** the HTTP body SHALL be `{"type":"STUMP","txid":"<hash>","blockHash":"<hash>","subtreeIndex":<n>,"stump":"<hex>"}`

#### Scenario: SEEN_ON_NETWORK callback HTTP body
- **WHEN** delivering a `SEEN_ON_NETWORK` callback
- **THEN** the HTTP body SHALL be `{"type":"SEEN_ON_NETWORK","txid":"<hash>"}`

#### Scenario: BLOCK_PROCESSED callback HTTP body
- **WHEN** delivering a `BLOCK_PROCESSED` callback
- **THEN** the HTTP body SHALL be `{"type":"BLOCK_PROCESSED","blockHash":"<hash>"}`
