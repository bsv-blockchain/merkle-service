# Harden the subtree-worker retry path

## Why

During the 2026-07-15 scale-ovh incident the subtree-worker's retry path amplified a
DataHub-404 storm roughly 10×: `handleTransientFailure` re-publishes failed work items to
`subtree-work` **immediately** — no backoff — so the full `block.maxAttempts` budget (10)
burns in milliseconds per item, each attempt re-fanning load onto the already-failing
dependency. The subtree-fetcher gained an exponential retry backoff for exactly this
failure shape (`subtree.retryBackoffBaseMs`, dev-ovh-1 ENOSPC incident); the worker never
did.

Two sibling gaps live in the same function:

1. **Disk-full dead-letters.** A full blob store (STUMP `Put` returning ENOSPC) is
   treated as an ordinary transient failure, so after the retry budget it lands on
   `subtree-work-dlq` — which has no replay. The fetcher got ENOSPC "parking" (ride out
   the outage under Kafka retention, never DLQ) in v0.4.6; the worker was deferred.
2. **Late 404s are permanent but retried forever.** Teranode's asset cache prunes
   subtree data after ~2h, so a DataHub 404 on an old work item can never succeed —
   yet it burns the full 10-attempt budget like any other blip.

## What Changes

- Extract the fetcher's backoff/wait/disk-full helpers into a new `internal/retryutil`
  package (zero behavior change for the fetcher) so the two retry paths cannot silently
  fork — especially `IsDiskFull`'s errno-plus-text matching.
- `handleTransientFailure` in the subtree-worker:
  - waits an exponential, context-aware backoff before every retry republish
    (`block.retryBackoffBaseMs`, default 1000ms, capped at **5s** — see design.md for
    why not the fetcher's 30s);
  - **parks** work items on disk-full: WARN + metric + throttle, no AttemptCount bump,
    no counter decrement, no DLQ — the unacked message rides out the outage under
    Kafka retention;
  - routes DataHub 404s to the DLQ after a reduced budget
    (`block.notFoundMaxAttempts`, default 3), hard-ceilinged by `block.maxAttempts`.
- New counter `merkle_subtree_work_messages_total{outcome}` (retried / dlq /
  parked_disk_full) — the per-outcome visibility the incident lacked.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `block-processing`: subtree-work retry hand-offs gain backoff, disk-full parking, and
  a bounded not-found budget.

## Impact

- **`internal/retryutil`** (new): `Backoff`, `Wait`, `IsDiskFull` lifted from
  `internal/subtree/processor.go`; the fetcher keeps thin wrappers so its behavior and
  tests are byte-for-byte unchanged.
- **`internal/config/config.go` / `config.yaml`**: `block.retryBackoffBaseMs`
  (`BLOCK_RETRY_BACKOFF_BASE_MS`), `block.notFoundMaxAttempts`
  (`BLOCK_NOT_FOUND_MAX_ATTEMPTS`).
- **`internal/metrics/block.go`** (new): `merkle_subtree_work_messages_total`.
- **`internal/block/subtree_worker.go`**: `handleTransientFailure` gains the park
  branch, the not-found DLQ clause, and the pre-publish backoff wait.
- **Operational**: retries for a failing item now span real time (≈37s across a
  10-attempt life at defaults, plus per-republish backlog traversal), so
  `subtree-work` consumer lag during dependency outages is expected and healthy —
  see design.md for the counter-TTL runbook caveat.
