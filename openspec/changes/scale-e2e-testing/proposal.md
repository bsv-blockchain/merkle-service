## Why

The merkle service targets blocks with millions of transactions and hundreds of subtrees, but all existing tests operate at trivial scale (1-3 txids, 1 callback server). We need to validate correctness, completeness, and performance characteristics under realistic load before deploying to production — specifically, that every registered transaction receives its MINED callback with a valid STUMP, that BLOCK_PROCESSED is broadcast to all callback URLs, and that no callbacks are lost or duplicated.

## What Changes

- Add a scale-focused end-to-end test harness under `test/scale/` that simulates 50 Arcade instances, each registering 1,000 transactions, against blocks containing 50,000 transactions across 50 subtrees of 1,024 transactions each.
- Pre-generate and store deterministic test fixtures (transaction hashes, subtree data) so tests load data rather than regenerating it each run.
- Stand up 50 concurrent callback HTTP servers (one per simulated Arcade instance) on a port range, each collecting and verifying received callbacks.
- Inject test data directly via Kafka (block messages, subtree data) to exercise the block processor → subtree processor → callback delivery pipeline without requiring libp2p.
- Build a robust verification layer that checks:
  - Every registered txid received exactly one MINED callback with non-empty STUMP data.
  - Every callback URL received exactly one BLOCK_PROCESSED callback per block.
  - STUMP data is valid BRC-0074 binary (decodeable, correct block height).
  - No unexpected or duplicate callbacks were delivered.
- Collect and report performance metrics: end-to-end latency (block injected → last callback delivered), callback throughput, Kafka consumer lag, per-phase timing.

## Capabilities

### New Capabilities
- `scale-e2e-testing`: Test harness, fixture generation, callback verification, and metrics collection for scale end-to-end testing of the merkle service pipeline.

### Modified Capabilities

_(none — this is a test-only change with no modifications to existing service behavior)_

## Impact

- **New files**: `test/scale/` directory with test harness, fixture generator, callback servers, and verification logic; `test/scale/testdata/` with pre-generated fixtures.
- **Dependencies**: Uses existing `internal/kafka`, `internal/store`, `internal/stump`, `internal/block`, `internal/callback` packages. No new external dependencies.
- **Infrastructure**: Requires local Kafka and Aerospike (same as existing e2e tests). Designed for 64GB RAM machines.
- **Build tags**: Uses `//go:build scale` to avoid running in normal CI.
