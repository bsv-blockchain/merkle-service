# Design: emit-expected-stump-flag

## Context

`internal/block/subtree_worker.go` records, per successfully processed subtree,
the subtree's index into each matched callback URL's expected-STUMP set
(`store.ExpectedStumpStore.AddSubtreeIndex`, one batched Aerospike operation per
subtree) just before decrementing the per-block subtree counter. When the
counter drains to zero, `emitBlockProcessed` reads each URL's set
(`GetSubtreeIndices`) and attaches it to that URL's `BLOCK_PROCESSED` as
`expectedSubtreeIndices` (JSON `omitempty`). The block-processor's
coinbase-only path calls the shared `emitBlockProcessedCallbacks` free function
with a nil store already — no subtrees means no STUMPs and no expected set.

This shipped always-on in v0.4.5 (#162), although
`docs/block-processed-completeness.md` had planned a default-off flag
(`block.emitExpectedStumpSet`). Arcade v0.9.10 consumes the field. The flag is
being added retroactively.

## Goals / Non-Goals

**Goals:**

- An operator off-switch for the expected-STUMP hot-path write load,
  controllable via yaml and env without a rebuild.
- Zero behavior change at defaults, for production `Load()` configs AND for
  zero-valued `BlockConfig` structs built directly in tests.
- Documentation (doc, config comments, openspec) that matches shipped reality.

**Non-Goals:**

- Widening the `callback` topic (still deferred — rollout step 4 of the doc).
- Changing default behavior: the set stays recorded and attached everywhere.
- Wiring changes in `cmd/`: stores stay constructed; gating happens at the two
  call sites.

## Decisions

### 1. Default TRUE, diverging from the doc's original "default off"

The original plan gated a not-yet-consumed field to avoid paying write load
before arcade could use it. That window is gone: the feature has been live and
unconditional since v0.4.5, production has absorbed the load, and arcade
v0.9.10 consumes the field. Defaulting off now would *regress* production
(silent-loss detection would vanish on upgrade). So the flag ships as an
emergency off-switch: default true, explicit false to shed the write load.

### 2. `*bool` with nil-means-enabled, not a plain `bool`

`TestEmitBlockProcessed_AttachesExpectedIndices` (and any directly-constructed
`SubtreeWorkerService`) uses a zero-valued `BlockConfig` without `config.Load()`.
A plain `bool` field would make that zero value mean *disabled*, silently
flipping default semantics for every struct-literal construction and breaking
existing tests, which must remain byte-identical. `EmitExpectedStumpSet *bool`
with `EmitExpectedStumpSetEnabled()` returning `nil || *v` keeps the zero value
on production semantics; `SetDefault("block.emitexpectedstumpset", true)` means
`Load()` always yields a non-nil pointer, and the env binding decodes through
viper's `WeaklyTypedInput` mapstructure pipeline (proven by the env-override
test).

### 3. Write and attach are gated together

Gating only the write while still attaching would ship an **empty** expected
set — semantically "expect zero STUMPs" — so the receiver would finalize
immediately and silently drop any late STUMP: the exact bug the feature fixes,
but now masquerading as a healthy signal. Conversely, writing without attaching
just wastes the writes. Both call sites therefore consult
`EmitExpectedStumpSetEnabled()` and switch in tandem, and the comments at both
sites cross-reference each other.

### 4. Gate the attach by resolving the store to nil in `emitBlockProcessed`

`emitBlockProcessedCallbacks` already treats a nil `expectedStumps` parameter
as "no set to attach" (the coinbase-only processor path passes nil today).
`emitBlockProcessed` resolves `s.expectedStumps` to nil when the flag is off
and delegates — no new function parameter, no signature churn, and the free
function's behavior stays covered by its existing tests.
