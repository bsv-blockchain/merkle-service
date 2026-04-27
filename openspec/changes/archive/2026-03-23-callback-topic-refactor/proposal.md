## Why

The callback delivery service currently reads from the `stumps` Kafka topic, which couples it to STUMP-specific message semantics. However, the service needs to deliver four distinct callback types (`SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, `MINED`, `BLOCK_PROCESSED`), and the outbound JSON payload must conform to Arcade's `CallbackMessage` type. Refactoring to a generic `callback` topic with a unified message format ensures all callback events flow through a single, well-defined pipeline and that Arcade receives payloads it can deserialize without translation.

## What Changes

- **BREAKING**: Rename the `stumps` Kafka topic to `callback` (and `stumps-dlq` to `callback-dlq`) across all producers and consumers.
- Refactor `StumpsMessage` to align with Arcade's `CallbackMessage` struct: `type`, `txid`, `blockHash`, `subtreeIndex`, `stump` fields.
- Update all publishers (subtree processor, subtree worker, block processor) to produce messages on the `callback` topic using the new unified message format.
- Update the callback delivery service to consume from the `callback` topic and POST the `CallbackMessage` JSON directly.
- Ensure all four callback types are published with correct payloads:
  - `SEEN_ON_NETWORK`: txid only
  - `SEEN_MULTIPLE_NODES`: txid only
  - `STUMP` (was `MINED`): txid, blockHash, subtreeIndex, stump data
  - `BLOCK_PROCESSED`: blockHash only
- Rename config fields from `stumpsTopic`/`stumpsDlqTopic` to `callbackTopic`/`callbackDlqTopic`.

## Capabilities

### New Capabilities
- `unified-callback-topic`: Defines the generic `callback` Kafka topic schema and the unified `CallbackMessage`-aligned message format used by all producers and consumers.

### Modified Capabilities
- `subtree-processing`: Publishers now produce to `callback` topic with `CallbackMessage` format instead of `StumpsMessage` on `stumps` topic.
- `block-processing`: Subtree worker and block completion now publish to `callback` topic with `CallbackMessage` format; `MINED` becomes `STUMP` type with `subtreeIndex` field.
- `callback-dedup`: Dedup key changes from `txid:callbackURL:statusType` to use the new `CallbackType` values (e.g., `STUMP` instead of `MINED`).
- `callback-batching`: Batched messages use the new `CallbackMessage` format on the `callback` topic.

## Impact

- **Kafka**: Topic rename (`stumps` → `callback`, `stumps-dlq` → `callback-dlq`). Requires topic creation in environments before deploy.
- **Config**: `stumpsTopic` and `stumpsDlqTopic` config keys renamed. Old config files need updating.
- **Internal messages**: `StumpsMessage` struct replaced or refactored to match `CallbackMessage` semantics.
- **Callback delivery HTTP payload**: JSON body changes from ad-hoc format to Arcade's `CallbackMessage` schema.
- **Arcade**: Must be updated (or already expects) the `CallbackMessage` JSON format on its callback endpoint.
- **Affected packages**: `internal/kafka/messages.go`, `internal/callback/delivery.go`, `internal/subtree/processor.go`, `internal/block/subtree_worker.go`, `internal/block/processor.go`, `internal/config/config.go`.
