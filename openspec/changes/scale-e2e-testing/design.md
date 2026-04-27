## Context

The merkle service has existing integration tests (`internal/e2e/e2e_test.go`) that validate basic callback flows with 1-3 transactions and single callback servers. These use Kafka + Aerospike on localhost with `//go:build integration`.

Production targets blocks with millions of transactions across hundreds of subtrees. We need to validate the full pipeline at a meaningful fraction of production scale: 50 simulated Arcade instances × 1,000 txids = 50,000 registered transactions, with blocks of 50,000 transactions across 50 subtrees of 1,024 each.

The machine constraint is 64GB RAM. With 50 callback servers and Kafka/Aerospike running locally, memory is the primary bottleneck. Each subtree's raw data is 1,024 × 32 bytes = 32KB, and 50 subtrees = 1.6MB — trivially small. The callback payload per MINED message is ~2-5KB (STUMP data + JSON). Total expected data in flight is well under 1GB.

## Goals / Non-Goals

**Goals:**
- Validate that all 50,000 registered transactions receive exactly one MINED callback with valid STUMP data after block processing.
- Validate all 50 callback URLs receive exactly one BLOCK_PROCESSED callback per block.
- Detect lost, duplicated, or corrupt callbacks at scale.
- Measure end-to-end latency and per-phase timing to identify bottlenecks.
- Make the test deterministic and reproducible by using pre-generated fixtures.
- Run within 64GB RAM on a single machine.

**Non-Goals:**
- Testing libp2p networking (bypassed — inject directly to Kafka).
- Multi-node Kafka or Aerospike clusters (single localhost instances).
- Sustained throughput testing (this is a single-block correctness + timing test).
- Testing block processor's DataHub integration (subtrees are pre-stored in blob store).

## Decisions

### 1. Test location: `test/scale/` with `//go:build scale` tag

**Decision**: Place scale tests in `test/scale/` with a `scale` build tag, separate from existing `internal/e2e/`.

**Rationale**: Scale tests take significantly longer (minutes), require more memory, and are not appropriate for regular CI. The `test/` top-level directory keeps them distinct from unit and integration tests. The `scale` build tag ensures they never run accidentally.

**Alternative considered**: Adding to `internal/e2e/` with the existing `integration` tag. Rejected because scale tests have different resource requirements and running time.

### 2. Pre-generated deterministic fixtures stored as binary files

**Decision**: Create a one-time fixture generator (`test/scale/cmd/generate-fixtures/main.go`) that produces:
- `testdata/txids.bin` — 50,000 deterministic 32-byte transaction hashes.
- `testdata/subtrees/00.bin` through `testdata/subtrees/49.bin` — raw subtree data (concatenated 32-byte hashes) matching DataHub format.
- `testdata/manifest.json` — mapping of arcade instance index → assigned txid range, subtree assignments, expected callback counts.

Transaction hashes are derived from `SHA256(seed || index)` for determinism. The first 50,000 hashes are distributed across 50 subtrees of 1,024, with each Arcade instance's 1,000 txids scattered across subtrees to simulate realistic distribution.

**Rationale**: Loading pre-generated data eliminates ~10-30 seconds of SHA256 computation per run, makes tests fully reproducible, and allows inspection of fixture data for debugging.

**Alternative considered**: Generate in `TestMain`. Rejected because generation is slow and debugging is harder without stable fixtures.

### 3. Fifty callback servers on sequential ports

**Decision**: Start 50 `net/http` servers on `127.0.0.1:19000-19049`, one per simulated Arcade instance. Each server runs a `callbackCollector` that records all received payloads in a thread-safe slice.

**Rationale**: Using real HTTP servers (not httptest) gives us real port numbers that work with the callback delivery service, which needs routable URLs. The port range 19000-19049 is high enough to avoid conflicts. Each server is lightweight (~2MB overhead including buffers).

**Alternative considered**: `httptest.NewServer` for each. Rejected because httptest servers use random ports that are harder to pre-compute for fixture generation, and the ephemeral port approach makes debugging harder.

### 4. Pipeline: Kafka injection bypassing libp2p and DataHub

**Decision**: The test injects data at two points:
1. **Aerospike**: Pre-load all 50,000 txid→callbackURL registrations and the callback URL registry.
2. **Blob store**: Pre-load all 50 subtree binary files into the subtree store.
3. **Kafka block topic**: Publish one `BlockMessage` to trigger the block processor.

The block processor fetches subtrees from blob store (skipping DataHub), processes registrations, builds STUMPs, publishes MINED + BLOCK_PROCESSED messages to the stumps topic, and the callback delivery service delivers them to the 50 callback servers.

**Rationale**: This exercises the critical path (block processing → STUMP building → callback delivery) without external DataHub or libp2p dependencies, matching the user's requirement to leverage Kafka messages directly.

### 5. Verification: Completeness, correctness, and no duplicates

**Decision**: After the block is processed, the test waits for all expected callbacks with a deadline, then verifies:
- **Completeness**: Every registered txid appears in exactly one MINED callback's `txids` array across the callbacks received by its Arcade instance's server. Every callback URL received exactly one BLOCK_PROCESSED callback.
- **Correctness**: Every MINED callback contains non-empty `stumpData` that decodes as valid BRC-0074 STUMP binary (correct block height, parseable levels). The `blockHash` matches the test block.
- **No duplicates**: No txid appears in MINED callbacks more than once per callback URL. No BLOCK_PROCESSED callback is received more than once per URL.

**Rationale**: These three checks together ensure the pipeline is lossless and correct at scale. STUMP validation catches serialization bugs that wouldn't surface at small scale.

### 6. Metrics collection via timing checkpoints

**Decision**: Instrument the test with `time.Now()` checkpoints at key phases:
- T0: Block message published to Kafka.
- T1: First MINED callback received.
- T2: Last MINED callback received (all 50,000 txids accounted for).
- T3: All BLOCK_PROCESSED callbacks received.
- Per-callback-server: count of MINED messages, total txids, bytes received.

Report as a summary table at test completion. No external metrics infrastructure required.

**Rationale**: Simple timing is sufficient for identifying bottlenecks. Adding Prometheus/pprof would add complexity without proportional value for a local test.

**Alternative considered**: pprof CPU/memory profiling. Can be added separately by running with `-cpuprofile`/`-memprofile` flags; doesn't need to be built into the test.

### 7. Subtree-to-Arcade distribution strategy

**Decision**: Distribute txids such that each Arcade instance's 1,000 txids are spread across multiple subtrees (roughly 20 per subtree). This means each subtree contains txids from ~20 different Arcade instances, and each Arcade's callback server will receive multiple MINED callbacks (one per subtree that contains its txids).

Specifically: Arcade `i` gets txids at global indices `[i*1000, (i+1)*1000)`. Since subtrees are 1,024 txids each at contiguous ranges, an Arcade's txids span roughly 1-2 subtrees if contiguous, which is unrealistic. Instead, use a shuffled assignment: txid at global index `j` is assigned to subtree `j % 50`, placing Arcade `i`'s 1,000 txids across all 50 subtrees (~20 per subtree).

**Rationale**: This creates a realistic pattern where each Arcade receives MINED callbacks from many subtrees, exercising the grouping and STUMP building logic under fan-out conditions.

## Risks / Trade-offs

- **[Risk] Kafka/Aerospike not running** → Test skips gracefully with clear error message, same pattern as existing e2e tests. Build tag `scale` prevents accidental execution.
- **[Risk] 50 HTTP servers exhaust file descriptors** → 50 servers with modest connection counts are well within default ulimits (typically 1024+). Add a preflight check.
- **[Risk] Test flakiness from timing** → Use generous timeouts (5 minutes for full pipeline). Verification is count-based, not time-based, so slow runs still pass.
- **[Risk] Fixture drift after code changes** → Fixtures are pure data (hashes, raw bytes). Only need regeneration if subtree format or hash derivation changes, which is extremely rare.
- **[Trade-off] Single block only** → Testing one block at this scale validates correctness. Multi-block sustained throughput testing is a separate concern and non-goal.

## Open Questions

_(none — design is straightforward given the existing pipeline architecture)_
