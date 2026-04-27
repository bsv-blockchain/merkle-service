## 1. Project Structure and Scaffolding

- [x] 1.1 Create `test/scale/` directory with `scale_test.go` using `//go:build scale` tag and package `scale`
- [x] 1.2 Create `test/scale/testdata/` directory for storing generated fixtures
- [x] 1.3 Create `test/scale/cmd/generate-fixtures/main.go` scaffold with command-line flags for seed, instance count, txids-per-instance, subtree count, txids-per-subtree

## 2. Fixture Generator — Transaction Hashes

- [x] 2.1 Implement deterministic txid generation: `SHA256(seed || big-endian uint64 index)` producing 50,000 unique 32-byte hashes
- [x] 2.2 Write txids to `testdata/txids.bin` as concatenated 32-byte hashes (50,000 × 32 = 1,600,000 bytes)
- [x] 2.3 Add a `loadTxids(path string) ([][]byte, error)` helper in `test/scale/fixtures.go` that reads and parses the binary txid file

## 3. Fixture Generator — Subtree Data

- [x] 3.1 Implement subtree assignment: txid at global index `j` goes to subtree `j % numSubtrees`, producing 50 groups of 1,000 txids
- [x] 3.2 Write each subtree's raw binary data to `testdata/subtrees/XX.bin` (1,000 × 32 bytes each) in DataHub concatenated-hash format
- [x] 3.3 Compute and store each subtree's root hash (SHA256 of raw data) for use as the subtree identifier
- [x] 3.4 Add a `loadSubtree(path string) ([]byte, error)` helper that reads a single subtree binary file

## 4. Fixture Generator — Manifest

- [x] 4.1 Define `Manifest` struct: seed, block hash, block height, arcade instances (index, port, txid global indices, callback URL), subtrees (index, hash, txid global indices)
- [x] 4.2 Implement Arcade-to-txid assignment: instance `i` gets global indices `[i*1000, (i+1)*1000)`, callback URL is `http://127.0.0.1:{19000+i}/callback`
- [x] 4.3 Write manifest to `testdata/manifest.json` with all mappings
- [x] 4.4 Add `loadManifest(path string) (*Manifest, error)` helper
- [x] 4.5 Add a verification step in the generator that confirms: all 50,000 txids are assigned to exactly one subtree and one Arcade instance

## 5. Fixture Generator — Run and Commit

- [x] 5.1 Wire up the `main()` function in `cmd/generate-fixtures/` to call all generators in sequence with default parameters (seed=42, 50 instances, 1000 txids/instance, 50 subtrees, 1000 txids/subtree)
- [x] 5.2 Run the generator and commit the output `testdata/` files to the repo
- [x] 5.3 Add a test `TestFixturesLoadable` in `scale_test.go` that loads all fixture files and validates basic counts (50,000 txids, 50 subtrees × 1,000, manifest integrity)

## 6. Callback Server Fleet

- [x] 6.1 Create `test/scale/callback_server.go` with a `CallbackServer` struct holding: port, `sync.Mutex`-protected slices for received MINED payloads, BLOCK_PROCESSED payloads, raw bytes, and timing data (first/last callback timestamps)
- [x] 6.2 Implement `NewCallbackServer(port int)` that creates an `http.Server` on `127.0.0.1:{port}` with a handler that records payloads and timestamps
- [x] 6.3 Implement `Start()` / `Stop()` lifecycle for each callback server
- [x] 6.4 Create `CallbackFleet` struct that manages 50 `CallbackServer` instances, with `StartAll()` / `StopAll()` / `GetServer(index int)` methods
- [x] 6.5 Add a test `TestCallbackFleetStartStop` that starts all 50 servers, sends one HTTP POST to each, verifies receipt, and stops all servers

## 7. Registration Pre-loading

- [x] 7.1 Create `test/scale/setup.go` with `preloadRegistrations(manifest, asClient, regSetName)` that batch-loads all 50,000 txid→callbackURL registrations into Aerospike
- [x] 7.2 Create `preloadCallbackURLRegistry(manifest, asClient, registrySetName)` that adds all 50 callback URLs to the CallbackURLRegistry
- [x] 7.3 Create `preloadSubtrees(manifest, subtreeStore)` that loads all 50 subtree binary files into the blob-backed subtree store
- [x] 7.4 Add verification: after pre-loading, spot-check 10 txids with `BatchGet()` and verify registrations exist

## 8. Block Injection and Pipeline Orchestration

- [x] 8.1 Create `test/scale/pipeline.go` with `injectBlock(manifest, blockTopic, producer)` that publishes a `BlockMessage` with the test block hash, height, and a mock DataHub URL
- [x] 8.2 Set up a mock DataHub HTTP server that returns block metadata (subtree hashes from manifest) for the `/block/{hash}/json` endpoint
- [x] 8.3 Wire up the full pipeline in `TestScaleEndToEnd`: create Aerospike client, stores, Kafka producers/consumers, block processor, callback delivery service
- [x] 8.4 Add proper cleanup with `t.Cleanup()` for all resources (Kafka consumers, producers, Aerospike client, HTTP servers, callback fleet)

## 9. Verification Layer

- [x] 9.1 Create `test/scale/verify.go` with `waitForAllCallbacks(fleet, manifest, timeout)` that blocks until all expected MINED and BLOCK_PROCESSED callbacks are received or timeout
- [x] 9.2 Implement `verifyMinedCompleteness(fleet, manifest)`: for each Arcade instance, collect all txids from MINED callbacks and assert they exactly match the instance's registered txids
- [x] 9.3 Implement `verifyMinedNoDuplicates(fleet, manifest)`: for each Arcade instance, assert no txid appears more than once across all MINED callbacks
- [x] 9.4 Implement `verifyStumpValidity(fleet, manifest)`: for each MINED callback, base64-decode stumpData and verify it parses as valid BRC-0074 binary with correct block height
- [x] 9.5 Implement `verifyBlockProcessed(fleet, manifest)`: assert each callback server received exactly one BLOCK_PROCESSED callback with correct block hash
- [x] 9.6 Implement `verifyNoSpuriousCallbacks(fleet, manifest)`: after a drain period, assert total callbacks = expected MINED + BLOCK_PROCESSED count with no extras
- [x] 9.7 Create `runAllVerifications(t, fleet, manifest)` that calls all verify functions and reports failures per-category

## 10. Metrics and Reporting

- [x] 10.1 Create `test/scale/metrics.go` with `MetricsReport` struct holding: T0 (block injected), T1 (first MINED), T2 (last MINED), T3 (all BLOCK_PROCESSED), per-server stats
- [x] 10.2 Implement `collectMetrics(fleet, t0)` that gathers timing data from the callback fleet's first/last timestamps
- [x] 10.3 Implement `printReport(report)` that outputs a formatted summary table: phase timings, per-server callback counts, total txids, bytes received, overall pass/fail
- [x] 10.4 Wire metrics collection into `TestScaleEndToEnd` between pipeline completion and verification

## 11. Main Test Function

- [x] 11.1 Implement `TestScaleEndToEnd` orchestrating the full flow: load fixtures → start fleet → pre-load registrations → start pipeline services → inject block → wait for callbacks → collect metrics → verify → report
- [x] 11.2 Add `TestMain` for the scale package with environment checks (Kafka reachable, Aerospike reachable, sufficient ulimits)
- [x] 11.3 Add a shorter `TestScaleSmoke` variant with 5 instances × 100 txids × 5 subtrees for faster iteration during development
- [x] 11.4 Run `TestScaleSmoke` to validate the full pipeline works, then run `TestScaleEndToEnd` with full parameters
