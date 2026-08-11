# Tasks: seen-callback-topic-split

## 1. Configuration

- [x] 1.1 Add `CallbackSeenTopic` (yaml `callbackSeenTopic`, mapstructure
      `callbackseentopic`) and `CallbackSeenPartitions` to `config.KafkaConfig`,
      with `defaultCallbackSeenPartitions = 12`
- [x] 1.2 Add `KafkaConfig.SeenCallbackTopic()` — dedicated topic when set,
      else `CallbackTopic` — documented as the ONLY expression of "where SEEN
      callbacks live", to be used by producer and consumer alike
- [x] 1.3 `TopicPartitions()`: map the SEEN topic ONLY when `CallbackSeenTopic`
      is non-empty, keyed off the raw field (never `SeenCallbackTopic()`, which
      would silently widen `callback` in fallback mode); inline comment on the
      hazard
- [x] 1.4 `TopicRetention()`: add `CallbackSeenTopic` to the 6h work-topic list
- [x] 1.5 Viper defaults (`kafka.callbackseentopic` empty,
      `kafka.callbackseenpartitions` 12) + env bindings
      `KAFKA_CALLBACK_SEEN_TOPIC` / `KAFKA_CALLBACK_SEEN_PARTITIONS`
- [x] 1.6 Scope the "callback stays at 1 partition" rationale comment in
      `config.go` and `config.yaml` explicitly to STUMP / BLOCK_PROCESSED
- [x] 1.7 Document both keys in `config.yaml` (incl. rollout order) and enable
      them in `deploy/k8s/configmap.yaml`

## 2. Producer routing

- [x] 2.1 `internal/subtree/processor.go`: point the callback producer at
      `p.cfg.Kafka.SeenCallbackTopic()`; update the init log field
- [x] 2.2 Leave `internal/block/subtree_worker.go` and
      `internal/block/processor.go` untouched — STUMP / BLOCK_PROCESSED must
      stay on `callback` so their ordering barrier holds

## 3. Consumer

- [x] 3.1 `DeliveryService`: `consumers []*kafka.Consumer` and
      `retryProducers map[string]*kafka.Producer` (keyed by SOURCE topic)
- [x] 3.2 `Init`: keep the existing `callback` consumer with its `nil`
      partitions arg and comment; add a second consumer on
      `SeenCallbackTopic()` with group `<group>-callback-seen` and
      `TopicPartitions()` so EnsureTopics creates it wide. Skip both the second
      consumer and producer when `SeenCallbackTopic() == CallbackTopic`
- [x] 3.3 **Retry routing**: thread `msg.Topic` through
      `handleMessage → processDelivery → scheduleRetryOrDLQ → republishForRetry`;
      add `retryProducerFor` (falls back to the `callback` producer); return an
      error rather than acking when no producer resolves
- [x] 3.4 `Start` / `Stop` / `Health` cover both consumers and all retry
      producers; init log gains the seen topic, its partition count and a
      `callbackSeenSplitEnabled` flag
- [x] 3.5 Add `kafka.Producer.Topic()` so routing is assertable from tests

## 4. Tests

- [x] 4.1 `internal/callback/delivery_seen_topic_test.go`:
      `TestRetryRepublishesToSourceTopic` (SEEN→seen topic, SEEN_MULTIPLE_NODES
      →seen topic, BLOCK_PROCESSED→callback),
      `TestFutureDatedRetryRepublishesToSourceTopic` (the second republish call
      site), `TestRetryProducerFor_FallsBackToCallbackTopic`,
      `TestFallbackMode_SingleTopicBehavesAsBeforeSplit`,
      `TestRepublishForRetry_NoProducerNeverAcks`
- [x] 4.2 `internal/config/topic_partitions_test.go`: keep the existing
      "`callback` is ABSENT" assertion; add seen-absent-when-unset,
      seen-present-only-when-configured (and still no `callback`), explicit and
      non-positive partition counts, `TestKafkaConfig_SeenCallbackTopic`,
      `TestLoad_CallbackSeenTopicDefaultsInert`,
      `TestLoad_CallbackSeenTopicEnvOverride`
- [x] 4.3 `internal/config/topic_retention_test.go`: `callback-seen` gets the
      6h work-topic retention
- [x] 4.4 `internal/subtree/processor_seen_topic_test.go`: SEEN emit publishes
      through a producer bound to the SEEN topic (and not to `callback`);
      fallback mode binds to `callback`
- [x] 4.5 `internal/config/config_test.go`: add the two new env vars to
      `clearConfigEnv`

## 5. OpenSpec

- [x] 5.1 Update `openspec/specs/unified-callback-topic/spec.md` — the unified
      topic becomes the default/fallback, with an optional SEEN split; STUMP and
      BLOCK_PROCESSED still required to share one topic
- [x] 5.2 Add this change folder (proposal / design / tasks / spec delta)

## 6. Validation

- [x] 6.1 `go build ./...`, `go vet ./...`, `go test ./...`
- [x] 6.2 `make lint` (golangci-lint + lint-logfields)
- [x] 6.3 Pre-existing `internal/store` failures (Aerospike `merkle` namespace
      absent in the dev environment) confirmed identical on `origin/main`

## 7. Infra (separate repo — NOT in this PR)

- [ ] 7.1 Add a `callback-seen` Topic CRD to
      `bsva-infra-flux/apps/base/merkle-service/_base/topics.yaml` with
      `max.message.bytes: "10485760"`, before enabling the config
