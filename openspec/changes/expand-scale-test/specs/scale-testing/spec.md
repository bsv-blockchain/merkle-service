## ADDED Requirements

### Requirement: Mega-scale fixture generation
The fixture generator SHALL support generating test data with 1,000,000 transactions across 244 subtrees of 4,096 txids each, and 100 Arcade instances registering 10,000 txids each.

#### Scenario: Generate mega fixtures
- **WHEN** the generator is invoked with `--instances 100 --txids-per-instance 10000 --subtrees 244 --txids-per-subtree 4096`
- **THEN** it SHALL produce a valid `testdata-mega/` directory with `txids.bin` (32MB), 244 subtree files, and a manifest with all mappings verified

### Requirement: Phase-level metrics collection
The scale test harness SHALL measure and report distinct timing phases: block processing duration and callback delivery duration.

#### Scenario: Block processing timing
- **WHEN** a block is injected and all MINED callbacks begin arriving
- **THEN** the report SHALL show the time from block injection (T0) to first MINED callback received (T1) as "block processing time"

#### Scenario: Callback delivery timing
- **WHEN** MINED callbacks are being delivered
- **THEN** the report SHALL show the time from first MINED callback (T1) to last MINED callback (T2) as "callback delivery spread"

#### Scenario: Per-instance latency distribution
- **WHEN** all callbacks are received
- **THEN** the report SHALL include P50, P95, and P99 per-instance callback latency statistics

### Requirement: Parallel registration pre-loading
The test harness SHALL pre-load 1,000,000 txid registrations using parallel workers to complete within a reasonable time (<3 minutes).

#### Scenario: Parallel pre-loading
- **WHEN** 100 Arcade instances with 10,000 txids each are pre-loaded
- **THEN** the pre-loading SHALL use a configurable worker pool (default 10) to parallelize across instances

### Requirement: Mega-scale end-to-end verification
All existing verification checks (completeness, no duplicates, STUMP validity, BLOCK_PROCESSED delivery, no spurious callbacks) SHALL pass at the 1M transaction scale.

#### Scenario: Full verification at mega scale
- **WHEN** `TestScaleMega` completes
- **THEN** all verification functions SHALL confirm: every registered txid received a valid MINED callback with correct STUMP data, no duplicates, exactly one BLOCK_PROCESSED per instance, and no spurious callbacks
