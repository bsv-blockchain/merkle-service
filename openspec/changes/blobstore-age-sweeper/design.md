# Design: blob-store age sweeper

## Context

`FileBlobStore` (internal/store/file_blob.go) is the durable backend for two blob
families sharing one volume, plus its own bookkeeping:

- **Subtree blobs** — `StoreSubtree(id, ...)` writes the raw key, and every producer
  passes a content-addressed sha256 hex hash (`verifySubtreeContentAddress` parses it
  via `chainhash.NewHashFromStr`). On disk: **top-level files named with 64 lowercase
  hex characters**.
- **STUMP blobs** — `stumpStore.Put` prefixes every key with `stumpKeyPrefix = "stump/"`
  (internal/store/stump_store.go), explicitly "so they cannot collide with subtree keys
  when the same BlobStore backs both". On disk: files under the **`stump/`
  subdirectory**.
- **DAH manifests** — `<root>/.dah/<height>/<owner>.list`; `resolvePath` rejects any
  blob key that would land inside `.dah/`, so it is bookkeeping-only space.

Deletion is event-driven (delete-at-height manifests, pruned by
`SetCurrentBlockHeight`), but a subtree blob only gets a schedule when its subtree-work
item completes. Incomplete items (trimmed queues, long parking, crashes) orphan blobs
forever — dev-ovh-1 filled 1TiB in ~3h (issue #188).

## Goals / Non-Goals

**Goals:**

- The subtree blob store is bounded in size regardless of what the DAH path missed.
- STUMP blobs are never age-swept: callback-delivery fetches them at delivery time with
  retry windows up to ~1h (`stumpDahOffset=6` exists for that window).
- Zero-byte ENOSPC litter (13,433 files in the incident) is reaped quickly.
- Exactly one sweeper per shared volume (no CephFS walk herd).
- Sweep visibility: counters for files/bytes reclaimed.

**Non-Goals:**

- Sweeping `.dah/` manifests: a schedule outliving its blob is a no-op fire, and
  manifests are tiny; deleting them risks unscheduling live blobs. (Issue #188 floated
  reaping stale manifests; deliberately dropped here — wrong risk/reward.)
- A general TTL/GC framework for the blob store; DAH remains the precise prompt path.
- Sweeping memory stores (single-process, lost on restart anyway).

## Decisions

### 1. Discriminator: key namespace, not DAH-schedule lookup

The sweeper deletes a file only when it is **directly at the store root AND named with
exactly 64 lowercase hex characters** — the shape every subtree key has by construction
(content addressing) and no other on-disk artifact can have: STUMPs are namespaced under
`stump/` (enforced at the only STUMP write site), and `.dah/` is rejected key space.
Anything else (uppercase hex, wrong length, other names) is left alone: an unknown file
leaks to operators rather than risking live data.

The alternative — "delete only keys with no pending DAH schedule" — was rejected:
manifests are per-writer append-only files keyed by height, so answering "is key K
scheduled?" means parsing every manifest on every sweep, and a torn append (already
tolerated by the DAH design) would make the answer wrong exactly when it matters.
Age (mtime) plus the namespace split is sufficient: every *completed* subtree gets a DAH
schedule that fires well before the sweep age, so the sweeper only ever fires on data
the pipeline abandoned.

### 2. Zero-byte litter: reaped everywhere outside `.dah/`, after 5 minutes

A zero-byte blob is never valid data — subtree payloads and STUMPs are non-empty by
construction — but `os.WriteFile` creates-then-writes, so a file is legitimately empty
for the instant a write is in flight. A fixed `zeroByteMaxAge = 5min` grace (orders of
magnitude above any write latency, orders of magnitude below the incident's litter age)
separates the two. This applies under `stump/` too: a zero-byte STUMP would otherwise
be *served* as empty bytes by `Get` (which only maps ENOENT to not-found), poisoning
callback delivery; deleting it converts that to the well-handled `ErrStumpNotFound`.
`.dah/` stays untouched even for empty manifests — torn-manifest handling belongs to
the DAH machinery.

### 3. Block-processor is the only sweeper

The previous sweeper ran wherever the store was constructed — every replica of every
service walking the same shared CephFS tree each interval. The block-processor is a
single replica, already executes the DAH prunes on this volume (it is where
`SetCurrentBlockHeight` fires), and mounts the same blob store through the registry. The
registry now exposes `Blob BlobStore`, and `cmd/block-processor` starts
`StartAgeSweeperFromConfig` right after registry construction: one sweep immediately
(deterministic recovery after a restart mid-incident), then every `sweepIntervalSec`.
Store factories no longer start sweepers.

Concurrency posture is unchanged from the old sweeper: no lock taken. Blob files are
content-addressed (concurrent re-store recreates the key), POSIX keeps deleted files
readable through open descriptors, `GetSubtree`'s miss path re-fetches, and the only
mutex-guarded state (`.dah/`) is skipped entirely. `FileBlobStore` keeps no per-key
in-memory state, so there is nothing to reconcile after a delete.

### 4. Config: rename to `sweepMaxAgeSec`, tighten defaults, floor at 600s

`orphanMaxAgeSec` (86400) becomes `sweepMaxAgeSec` (1800 ≈ 3 blocks at ~10min cadence)
— the rename is deliberate: the semantics changed from "sweep everything" to "sweep
subtree blobs only", and a silently-inherited 24h value would defeat the fix.
`sweepIntervalSec` drops 3600 → 300 so orphan buildup between sweeps stays a small
fraction of steady state. Load() rejects nonzero `sweepMaxAgeSec < 600`: mtime-based
deletion below one block interval could remove the in-flight block's blobs while STUMPs
are still being assembled from them. 0 disables (either key), preserving an operator
escape hatch. Old configs carrying `orphanMaxAgeSec` are ignored by viper and fall back
to the new defaults — safe in the conservative direction for STUMPs (never swept) and
the aggressive direction for orphans (which is the point).

### 5. Metrics: two counters, no duration gauge

`merkle_blobstore_swept_files_total` / `merkle_blobstore_swept_bytes_total`, updated on
every pass (zero deltas keep the series alive on healthy stores). Per-sweep duration is
logged, not gauged — internal/metrics has no last-run-duration gauge pattern, and the
INFO log (emitted only when files > 0, to keep healthy stores quiet) carries it.

## Risks / Trade-offs

- **A never-mined subtree announced >30min before its block**: its blob is swept and
  the worker re-fetches from DataHub once at block time. Accepted — that miss path
  carried a full block at 100% miss rate during the incident.
- **Non-lowercase or non-standard subtree keys would never be swept.** No such producer
  exists (`chainhash` hex is lowercase); if one appears, blobs leak visibly (the
  incident dashboards now watch the volume) rather than being wrongly deleted.
- **Sweeper down = block-processor down.** Then no blocks are processed either, so the
  dominant blob producer (block-path re-stores) is also down; announcement-time writes
  continue but at a rate the volume tolerates until the single replica reschedules.
