## 1. retryutil extraction (zero behavior change)

- [x] 1.1 Create `internal/retryutil` with `Backoff(baseMs, attempt int, maxBackoff time.Duration) time.Duration` (exponential, 1-based attempt, 0 when baseMs<=0, shift-overflow guard → maxBackoff), `Wait(ctx, d) error` (ctx-aware timer, d<=0 → nil), and `IsDiskFull(err) bool` (ENOSPC via errors.Is + "no space left on device"/"disk quota exceeded" text match); unit tests for schedule/cap/overflow/disable, Wait cancellation, and the IsDiskFull matrix
- [x] 1.2 Refactor `internal/subtree/processor.go` `retryBackoff`/`waitBackoff`/`isDiskFull` into one-line wrappers over retryutil, keeping `subtreeRetryBackoffCap = 30s` local; `processor_backpressure_test.go` passes UNMODIFIED

## 2. Configuration

- [x] 2.1 Add `RetryBackoffBaseMs` (mapstructure `retrybackoffbasems`) and `NotFoundMaxAttempts` (`notfoundmaxattempts`) to `config.BlockConfig` with incident-referencing doc comments
- [x] 2.2 Defaults `block.retrybackoffbasems=1000`, `block.notfoundmaxattempts=3`; env bindings `BLOCK_RETRY_BACKOFF_BASE_MS`, `BLOCK_NOT_FOUND_MAX_ATTEMPTS`; document both in `config.yaml`

## 3. Metrics

- [ ] 3.1 New `internal/metrics/block.go`: `SubtreeWorkMessagesTotal` CounterVec (`merkle_subtree_work_messages_total`, label `outcome`) + `IncSubtreeWork(outcome)`, reusing the existing outcome label constants
- [ ] 3.2 Increment `retried` / `dlq` on the worker's two existing `handleTransientFailure` branches, with tests pinning the deltas

## 4. Worker retry path

- [ ] 4.1 `workerRetryBackoffCap = 5s` const + `retryBackoff(attempt)` / `notFoundMaxAttempts()` helpers on `SubtreeWorkerService`; log both new settings from `Init`
- [ ] 4.2 Park branch at the top of `handleTransientFailure`: `retryutil.IsDiskFull(cause)` → WARN + `parked_disk_full` + throttle wait + return error (no AttemptCount bump, no decrement, no DLQ)
- [ ] 4.3 DLQ condition gains `notFoundExhausted` (`errors.Is(cause, datahub.ErrNotFound) && nextAttempt >= notFoundMaxAttempts()`) as an independent clause alongside `maxAttempts` and `isPermanentFetchErr`; add a `reason` field to the DLQ log; branch body (decrement-before-DLQ) unchanged and sleep-free
- [ ] 4.4 Retry branch waits `retryutil.Wait(ctx, retryBackoff(nextAttempt))` BEFORE the AttemptCount bump/encode/publish; interrupted wait returns an error without publishing; add `backoffMs` to the retry WARN log

## 5. Tests

- [ ] 5.1 `internal/block/subtree_worker_backpressure_test.go`: backoff schedule pins the 5s cap; wall-clock backoff-before-retry; ctx-cancel abort; ENOSPC + quota-text parking (no DLQ, no decrement, AttemptCount unchanged, metric delta); e2e disk-full park via `stubStumpStore{putErr: enospcErr()}`; not-found reduced-budget DLQ + below-budget retry + budget capped by maxAttempts
- [ ] 5.2 Existing worker tests pass unmodified (zero-value config disables backoff); `go build ./...`, `go test ./... -count=1`, `-race` on internal/kafka + internal/block + internal/subtree, `golangci-lint run` all green
