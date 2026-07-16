# Tasks: emit-expected-stump-flag

## 1. Configuration

- [x] 1.1 Add `EmitExpectedStumpSet *bool` (mapstructure `emitexpectedstumpset`)
      to `config.BlockConfig` with the default-true divergence rationale and the
      write+attach-together correctness note; add `EmitExpectedStumpSetEnabled()`
      (nil ⇒ enabled)
- [x] 1.2 Default `block.emitexpectedstumpset=true`; env binding
      `BLOCK_EMIT_EXPECTED_STUMP_SET`; document the key in `config.yaml`

## 2. Gating

- [x] 2.1 `handleMessage` write gate: record subtree indices only when
      `s.expectedStumps != nil && s.blockCfg.EmitExpectedStumpSetEnabled()`
- [x] 2.2 `emitBlockProcessed` attach gate: resolve the expected-stump store to
      nil when disabled before delegating to `emitBlockProcessedCallbacks`
      (suppresses the read AND the field — never an empty set)

## 3. Docs and comments

- [x] 3.1 `docs/block-processed-completeness.md`: rewrite "Cost and gating"
      (flag exists, default on, off-switch semantics, both paths gated
      together); mark per-step rollout status (1 shipped v0.4.5, 2 shipped
      arcade v0.9.10 with skip-grace in progress, 3 overtaken by events,
      4 still deferred)
- [x] 3.2 Refresh the stale `docs/callback-topic-partition-design.md`
      references in `internal/config/config.go` and `config.yaml` to cite
      `docs/block-processed-completeness.md`

## 4. Tests (TDD: red before each implementation step)

- [x] 4.1 Config: `TestLoad_EmitExpectedStumpSet_DefaultTrue`,
      `TestLoad_EmitExpectedStumpSet_EnvOverride` (false disables, true
      enables), `TestBlockConfig_EmitExpectedStumpSetEnabled_ZeroValue`
      (nil means enabled)
- [x] 4.2 Block: `TestEmitBlockProcessed_FlagOff_OmitsExpectedIndices` (no
      `expectedSubtreeIndices` key in the raw payload, zero `GetSubtreeIndices`
      reads), `TestHandleMessage_FlagOff_SkipsAddSubtreeIndex` (zero writes,
      STUMP still published, counter still decremented),
      `TestHandleMessage_FlagUnset_RecordsSubtreeIndex` (default-on write path,
      closing its pre-existing coverage gap)

## 5. Validation

- [x] 5.1 Existing tests pass unmodified (`expected_stump_emit_test.go`,
      `subtree_worker_test.go`, `expected_stump_integration_test.go`
      byte-identical to origin/main)
- [x] 5.2 `go build ./...`, `go test ./... -count=1`, `go test -race` on
      internal/config + internal/block, `make lint` all green
