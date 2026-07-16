# Gate the expected-STUMP set behind `block.emitExpectedStumpSet`

## Why

`docs/block-processed-completeness.md` promised the expected-STUMP set (the
per-(block, callbackURL) subtree-index set attached to `BLOCK_PROCESSED` as
`expectedSubtreeIndices`) behind a config flag, `block.emitExpectedStumpSet`,
default off. The feature actually shipped **always-on** in v0.4.5 (#162) — no
flag was ever implemented, and the openspec main spec never documented the
field. This change reconciles documentation with reality and adds the flag as
an **emergency off-switch** for the feature's Aerospike write load on the MINED
hot path (one batched set-add per subtree with matched URLs), should it ever
need to be shed during an incident.

## What Changes

- New config key `block.emitExpectedStumpSet` (env
  `BLOCK_EMIT_EXPECTED_STUMP_SET`), **default true** — deliberately diverging
  from the doc's original "default off" rollout plan, because the feature has
  been unconditionally live in production since v0.4.5 and its write load is
  already absorbed. Off-switch semantics, not a rollout gate.
- The flag gates **both** the write path (recording subtree indices in
  `handleMessage`) and the attach path (reading the set in
  `emitBlockProcessed`) together. Gating only the write while still attaching
  would ship an empty expected set — "expect zero STUMPs" — reintroducing the
  silent-loss bug the feature exists to fix.
- `docs/block-processed-completeness.md` gains per-step rollout status and a
  corrected "Cost and gating" section; stale references to the removed
  `docs/callback-topic-partition-design.md` in `config.yaml` and
  `internal/config/config.go` now cite `docs/block-processed-completeness.md`.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `block-processing`: `BLOCK_PROCESSED` completeness reporting
  (`expectedSubtreeIndices`) becomes configurable via
  `block.emitExpectedStumpSet` (default on); this change also retroactively
  documents the field itself, which #162 shipped without a spec delta.

## Impact

- **`internal/config/config.go` / `config.yaml`**: `BlockConfig`
  `EmitExpectedStumpSet *bool` (nil ⇒ enabled) +
  `EmitExpectedStumpSetEnabled()`, default `block.emitexpectedstumpset=true`,
  env binding `BLOCK_EMIT_EXPECTED_STUMP_SET`, refreshed kafka
  partition-rationale comments.
- **`internal/block/subtree_worker.go`**: write gate in `handleMessage`,
  attach gate in `emitBlockProcessed` (resolves the store to nil when
  disabled; `emitBlockProcessedCallbacks` signature unchanged — the
  block-processor's coinbase-only path already passes nil).
- **`docs/block-processed-completeness.md`**: gating and rollout-status
  updates.
- **Tests**: `internal/config/config_test.go` (default / env-override /
  zero-value semantics), `internal/block/expected_stump_flag_test.go`
  (flag-off omits the field and never reads or writes the store; flag-unset
  records the index set — closing the write path's pre-existing coverage gap).
  Existing tests pass unmodified.
