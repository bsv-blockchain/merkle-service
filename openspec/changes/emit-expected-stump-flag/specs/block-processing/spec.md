# block-processing (delta)

## ADDED Requirements

### Requirement: Attach expected-STUMP index set to BLOCK_PROCESSED (configurable)

The subtree worker SHALL record, per (block, callbackURL), the set of subtree
indices that produced a STUMP for that URL, and SHALL attach that set to the
URL's `BLOCK_PROCESSED` message as `expectedSubtreeIndices` (JSON `omitempty`),
so the receiver can detect a missing STUMP (STUMPs are sparse, so a receiver
cannot otherwise distinguish a legitimately-absent STUMP from a lost one).
Recording MUST happen before the per-block subtree counter is decremented so
the set is complete when the counter drains to zero and the emit path reads it.

The behavior SHALL be controlled by `block.emitExpectedStumpSet` (env
`BLOCK_EMIT_EXPECTED_STUMP_SET`), default **true**; an unset value (including a
zero-valued config struct) MUST behave as enabled. Disabling MUST suppress both
the recording and the attach together: emitting `BLOCK_PROCESSED` with an
expected set that was not being recorded would ship an empty set ("expect zero
STUMPs") and silently disable missing-STUMP detection.

#### Scenario: Flag enabled or unset attaches the recorded set

- **WHEN** `block.emitExpectedStumpSet` is true or unset and a block's subtrees
  produce STUMPs for a callback URL
- **THEN** each successful subtree records its index for that URL exactly once
  (idempotent under re-drives), and the URL's `BLOCK_PROCESSED` message carries
  those indices as `expectedSubtreeIndices`

#### Scenario: Flag disabled records nothing and omits the field

- **WHEN** `block.emitExpectedStumpSet` is explicitly false and a block is
  processed
- **THEN** no subtree indices are recorded, the expected-stump store is not
  read at emit time, and `BLOCK_PROCESSED` is still emitted without an
  `expectedSubtreeIndices` key — indistinguishable from a pre-feature payload,
  never an empty set
