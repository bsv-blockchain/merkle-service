# Blob-store age sweeper for orphaned subtree blobs

## Why

A subtree blob only receives a delete-at-height schedule when its subtree-work item
completes (the worker re-stores/schedules at `blockHeight + dahOffset`). Any blob whose
work item never completes — queue trimmed during incident recovery, item parked
long-term, service crash between store and schedule — has **no DAH entry and is never
deleted**. Failed writes on a full disk additionally leave zero-byte litter.

Observed on dev-ovh-1 (15 Jul 2026): the subtree store grew from ~2% to **100% of a
1TiB volume in ~3 hours** under reprocess-storm conditions — 46,197 files, of which
39,477 were older than 2h (orphans; steady state should hold ~2 blocks ≈ 26GB) and
13,433 were zero-byte ENOSPC artifacts. A full store then cascades: fetcher stores park,
prune scheduling itself needs disk, and the fetcher's peer-health tracker gets poisoned
by local write failures.

The existing orphan sweeper (v0.4.x, issue #177) could not contain this: its 24h default
age is 8× slower than the observed fill rate, it swept **every** file older than the
threshold — including STUMP blobs, which callback-delivery reads at delivery time with
retry windows up to ~1h (`subtree.stumpDahOffset=6` exists for exactly that window) —
and it ran in every process that mounts the volume, a sweep herd on shared CephFS.

Aggressive GC is safe **for subtree blobs only**: they are a cache. DataHub re-serves
subtree data (~2h window), and the worker's miss path ("subtree not in blob store,
fetching from DataHub") is battle-tested — it carried an entire block at 100% miss rate.
Worst case for a swept blob is one re-fetch.

## What Changes

- Replace `SweepOrphans` with `SweepOlderThan(maxAge) (files, bytes, err)` on
  `FileBlobStore`: removes **only** top-level 64-lowercase-hex files (subtree blobs)
  older than maxAge, plus zero-byte files older than ~5min anywhere outside `.dah/`
  (ENOSPC litter). STUMP blobs (`stump/` namespace) and `.dah/` bookkeeping are never
  touched. Keys matching neither rule are left alone — leak, don't risk live data.
- Rewire the sweeper to run in the **block-processor only** (single replica; it already
  executes DAH prunes on the shared volume), started from `cmd/block-processor` via
  `store.StartAgeSweeperFromConfig(registry.Blob, ...)`: one sweep at startup, then
  every interval; INFO log per sweep only when something was removed. The store
  factories no longer start a per-process sweeper.
- Config: `blobStore.sweepMaxAgeSec` (default **1800** ≈ 3 blocks, replaces
  `blobStore.orphanMaxAgeSec`; nonzero values below 600 rejected at startup) and
  `blobStore.sweepIntervalSec` (default **300**, was 3600; 0 disables). New env
  bindings `BLOB_STORE_SWEEP_MAX_AGE_SEC` / `BLOB_STORE_SWEEP_INTERVAL_SEC`.
- New counters `merkle_blobstore_swept_files_total` and
  `merkle_blobstore_swept_bytes_total`, updated on every sweep pass.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `subtree-processing`: the subtree blob store gains a bounded-size guarantee — an age
  sweeper that reclaims orphaned subtree blobs and zero-byte litter without ever
  touching STUMP blobs or DAH bookkeeping.

## Impact

- **`internal/store/file_blob_sweep.go`**: `SweepOlderThan`, `StartAgeSweeper`,
  `StartAgeSweeperFromConfig` (replacing `SweepOrphans`, `StartOrphanSweeper`,
  `StartBlobSweeperFromConfig`).
- **`internal/store/registry.go` / `factory.go` / `sql/sql.go`**: `Registry.Blob`
  exposes the raw blob store for the block-processor's sweeper wiring; per-process
  sweeper start removed from both backends.
- **`cmd/block-processor/main.go`**: starts the sweeper after registry construction,
  stops it on shutdown.
- **`internal/config/config.go` / `config.yaml`**: `blobStore.sweepMaxAgeSec` replaces
  `blobStore.orphanMaxAgeSec`; new defaults, env bindings, and the ≥600s age floor.
- **`internal/metrics/blobstore.go`** (new): the two sweep counters.
- **Operational**: deployments that pinned `blobStore.orphanMaxAgeSec` must move to
  `blobStore.sweepMaxAgeSec` (unknown YAML keys are ignored, so stale configs silently
  fall back to the new, safer defaults). DAH remains the precise prompt-delete path;
  the sweeper guarantees a bounded store regardless of what DAH missed.
