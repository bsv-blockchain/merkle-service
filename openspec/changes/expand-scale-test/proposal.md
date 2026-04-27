## Why

The current scale test (50 Arcade instances × 1,000 txids = 50,000 total across 50 subtrees) completes in ~2 seconds and doesn't stress the system enough to reveal bottlenecks. Real-world blocks contain millions of transactions across hundreds of subtrees, and we need to validate performance at 20× the current scale with precise phase-level timing to identify where latency accumulates.

## What Changes

- Generate a new "mega" fixture set: 1,000,000 block transactions across ~244 subtrees of 4,096 txids each, with 100 Arcade instances each registering 10,000 transactions
- Add a new `TestScaleMega` test variant that uses these fixtures
- Expand the metrics report with distinct phase timings: block processing duration (T0→subtrees-fetched→stumps-produced) and callback delivery duration (first-callback→last-callback), plus per-subtree and per-instance breakdowns
- Update the fixture generator to support the new parameters and format subtree filenames with 3 digits (for >99 subtrees)
- Add a `make mega-scale-test` target for running the expanded test

## Capabilities

### New Capabilities

_(none — this extends the existing scale test infrastructure rather than introducing new system capabilities)_

### Modified Capabilities

_(no spec-level behavior changes — this is a test infrastructure expansion)_

## Impact

- **`test/scale/cmd/generate-fixtures/`**: Update to support 1M txids, 244 subtrees, 100 instances; fix subtree filename format for 3-digit indices
- **`test/scale/testdata/`**: Current 50K fixtures remain; new `testdata-mega/` directory with ~32MB txids.bin, 244 subtree files (~128KB each), and a larger manifest
- **`test/scale/scale_test.go`**: New `TestScaleMega` variant parameterized for 100 instances / 1M txids
- **`test/scale/fixtures.go`**: Update `loadAllFixtures` to accept a configurable testdata directory; adjust subtree filename parsing for 3-digit indices
- **`test/scale/metrics.go`**: Expand `MetricsReport` with block-processing vs. callback-delivery phase timings, per-subtree processing times, and throughput breakdowns
- **`test/scale/setup.go`**: Optimize preloading for 1M registrations (batch writes, progress logging)
- **`Makefile`**: New `mega-scale-test` target
- **Memory**: ~32MB for txid data + ~31MB for subtree data + Aerospike/Kafka overhead; well within 64GB
