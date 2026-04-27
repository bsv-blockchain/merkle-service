## Context

The existing scale test (`test/scale/`) validates the full Merkle Service pipeline with 50 Arcade instances × 1,000 txids = 50,000 total transactions across 50 subtrees. It completes in ~2 seconds. We need a 20× expansion to 1,000,000 block transactions across ~244 subtrees with 100 Arcade instances registering 10,000 txids each, plus granular phase-level metrics to identify bottlenecks.

Current infrastructure: deterministic fixture generator, CallbackFleet (N HTTP servers), mock DataHub, Aerospike pre-loading, Kafka-based block injection, and verification suite. All reusable with parameterization changes.

## Goals / Non-Goals

**Goals:**
- Test with 1,000,000 transactions in a block, 244 subtrees × 4,096 txids, 100 Arcade instances × 10,000 txids
- Measure block processing time (fetch metadata → fetch subtrees → produce stumps) separately from callback delivery time (stumps produced → all callbacks delivered)
- Keep the existing 50K test intact as a fast smoke/regression test
- Stay within 64GB RAM on a single developer machine

**Non-Goals:**
- Changing production code behavior — this is purely test infrastructure
- Distributed test execution across multiple machines
- Testing libp2p transport (Kafka-only, matching existing pattern)
- Benchmarking Aerospike or Kafka themselves (we measure the application layer)

## Decisions

### 1. Separate fixture directory (`testdata-mega/`)

Keep the existing `testdata/` (50K txids) for fast tests. Generate a new `testdata-mega/` with 1M-scale fixtures. Both share the same format and loader code.

**Rationale**: Avoids breaking the existing smoke test. The 50K fixtures are small enough to commit; the mega fixtures at ~32MB for txids.bin are borderline but still reasonable for a test repo. Alternative was runtime generation, but that adds 10+ seconds of CPU time per run.

### 2. Parameterize fixture loading by directory

Change `loadAllFixtures()` to accept a `dir string` parameter instead of using the hardcoded `testdataDir` constant. Each test variant passes its fixture directory.

**Rationale**: Minimal change, allows unlimited fixture sets. Alternative was embedding config in manifest.json, but that's over-engineering for test code.

### 3. Three-digit subtree filenames (`%03d.bin`)

With 244 subtrees, two-digit formatting breaks. Switch to `%03d.bin` for the mega fixtures. The existing `%02d.bin` format stays for the 50-subtree set (backward compatible).

**Rationale**: The loader already gets subtree indices from the manifest, so it just needs to format filenames from the index. We'll make the format string configurable based on subtree count.

### 4. Phase-aware metrics with instrumentation hooks

Add timing instrumentation to capture:
- **T_block_start**: Block message consumed from Kafka
- **T_subtrees_done**: All subtree STUMPs produced (last stump message published to stumps topic)
- **T_first_callback**: First MINED callback received by any server
- **T_last_mined**: Last MINED callback received
- **T_all_bp**: All BLOCK_PROCESSED callbacks received

Since we can't easily instrument the internal pipeline without modifying production code, we'll measure from the test harness:
- **Block processing time**: T0 (inject) → T_first_callback (proxy for "stumps started arriving at delivery layer")
- **Callback delivery spread**: T_first_callback → T_last_mined (measures delivery fan-out)
- **Total pipeline time**: T0 → T_all_bp

**Rationale**: The callback servers already track first/last timestamps. We add per-subtree tracking by correlating stump blockHash+subtreeIndex in MINED payloads. This avoids any production code changes.

### 5. Batch registration pre-loading

With 1M registrations (100 instances × 10,000 txids), sequential `Add()` calls will be slow. Use goroutines with a worker pool (10 workers) to parallelize pre-loading across instances.

**Rationale**: Pre-loading 1M records sequentially at ~1ms/record = 16 minutes. With 10 parallel workers across 100 instances = ~1.5 minutes. Alternative was Aerospike batch writes, but the current `RegistrationStore.Add()` API is per-record.

### 6. Enhanced metrics report

Expand the ASCII report with:
- Phase timing breakdown (block processing vs. callback delivery)
- Throughput: txids/sec for both phases
- Per-instance callback latency histogram (P50, P95, P99)
- Total data volume (bytes sent/received)

**Rationale**: The user specifically wants to identify where bottlenecks exist. Phase breakdown isolates "is the bottleneck in STUMP computation or in HTTP delivery?"

## Risks / Trade-offs

- **[Fixture file size]** 32MB txids.bin + 244×128KB subtrees ≈ 63MB committed to repo → Acceptable for test data; can add to `.gitignore` and regenerate via `make generate-mega-fixtures` if size becomes a concern
- **[Port exhaustion]** 100 callback servers on ports 19000-19099 → Well within OS limits; existing fleet pattern handles this cleanly
- **[Aerospike memory]** 1M registrations × ~200 bytes each ≈ 200MB in Aerospike → Fine for local testing with 64GB RAM
- **[Test duration]** Full mega test may take 2-5 minutes → Acceptable for scale testing; separate make target isolates from CI
- **[Pre-load time]** 1M registrations could take minutes → Mitigated by parallel worker pool (Decision 5)
