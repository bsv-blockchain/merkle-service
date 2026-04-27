## 1. Parameterize Fixture Loading

- [x] 1.1 Update `loadAllFixtures()` in `test/scale/fixtures.go` to accept a `dir string` parameter instead of using the hardcoded `testdataDir` constant
- [x] 1.2 Update all callers of `loadAllFixtures()` in `scale_test.go` to pass the fixture directory
- [x] 1.3 Update subtree filename formatting in `loadAllFixtures()` to dynamically choose `%02d` vs `%03d` based on subtree count (>99 uses 3 digits)

## 2. Update Fixture Generator for Mega Scale

- [x] 2.1 Update `writeSubtrees()` in `generate.go` to use `%03d.bin` format when subtree count exceeds 99
- [x] 2.2 Validate the generator works with mega parameters: `--instances 100 --txids-per-instance 10000 --subtrees 250 --txids-per-subtree 4000 --out testdata-mega`
- [x] 2.3 Run the generator and commit the `testdata-mega/` fixture files to the repo

## 3. Parallel Registration Pre-loading

- [x] 3.1 Refactor `preloadRegistrations()` in `setup.go` to use a worker pool of N goroutines (default 10), each processing a subset of arcade instances
- [x] 3.2 Add progress logging every 10 instances (or configurable interval) during parallel pre-loading
- [x] 3.3 Add a `preloadRegistrationsParallel()` function that distributes arcade instances across workers and collects errors

## 4. Enhanced Metrics Report

- [x] 4.1 Add phase timing fields to `MetricsReport` in `metrics.go`: `BlockProcessingTime` (T0→T1), `CallbackDeliverySpread` (T1→T2), `TotalPipelineTime` (T0→T3)
- [x] 4.2 Compute per-instance callback latency (time from T0 to each server's last callback) and calculate P50, P95, P99 percentiles
- [x] 4.3 Add a `PhaseBreakdown` section to `printReport()` showing block processing vs callback delivery times and their percentage of total
- [x] 4.4 Add throughput metrics: txids/sec for block processing phase and txids/sec for callback delivery phase
- [x] 4.5 Add total data volume summary (total bytes received across all callback servers)

## 5. Test Variants

- [x] 5.1 Add `TestScaleMega` in `scale_test.go` that calls `runScaleTest()` with `testdata-mega` directory, 100 instances, and a 10-minute timeout
- [x] 5.2 Update `runScaleTest()` to accept the fixture directory as a parameter and pass it through to `loadAllFixtures()`
- [x] 5.3 Update `TestScaleSmoke` and `TestScaleEndToEnd` to pass their fixture directory explicitly

## 6. Makefile Target

- [x] 6.1 Add `mega-scale-test` target to Makefile: `go test -tags scale -v -count=1 -timeout 15m -run TestScaleMega ./test/scale/`
- [x] 6.2 Add `generate-mega-fixtures` target that runs the fixture generator with mega parameters

## 7. Verification and Run

- [x] 7.1 Run `TestScaleSmoke` to confirm existing tests still pass with parameterized fixture loading
- [x] 7.2 Run `TestScaleMega` with full parameters and verify all checks pass
- [x] 7.3 Verify the metrics report shows phase-level timing breakdown and percentile latencies
