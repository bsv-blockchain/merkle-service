## 1. Configuration

- [x] 1.1 Replace `OrphanMaxAgeSec` with `SweepMaxAgeSec` (mapstructure `sweepmaxagesec`)
  in `config.BlobStoreConfig`; re-document `SweepIntervalSec` (0 disables) with
  incident-referencing doc comments
- [x] 1.2 Defaults `blobstore.sweepintervalsec=300`, `blobstore.sweepmaxagesec=1800`;
  env bindings `BLOB_STORE_SWEEP_INTERVAL_SEC`, `BLOB_STORE_SWEEP_MAX_AGE_SEC`;
  document both in `config.yaml` with the dev-ovh-1 rationale
- [x] 1.3 Load() validation: reject negative `sweepIntervalSec`; reject nonzero
  `sweepMaxAgeSec < 600` (the one-block-interval floor); tests for defaults, env
  overrides, and the validation table

## 2. Metrics

- [x] 2.1 New `internal/metrics/blobstore.go`: `merkle_blobstore_swept_files_total` and
  `merkle_blobstore_swept_bytes_total` counters + `AddBlobSweep(files, bytes)`

## 3. Sweeper

- [x] 3.1 `isSubtreeBlobName` (64 lowercase hex) + `SweepOlderThan(maxAge) (files int,
  bytes int64, err error)` on `FileBlobStore`: top-level subtree blobs older than
  maxAge removed; zero-byte files older than 5min removed anywhere outside `.dah/`;
  STUMP (`stump/`) blobs and `.dah/` never touched; per-file errors non-fatal;
  replaces `SweepOrphans`
- [x] 3.2 `StartAgeSweeper(interval, maxAge, logger)`: immediate sweep, then ticker;
  metrics updated every pass; INFO log (files/bytes/maxAge/duration) only when
  files > 0; idempotent stop; interval clamp guard; replaces `StartOrphanSweeper`
- [x] 3.3 `StartAgeSweeperFromConfig(blob, cfg, logger)`: no-op for memory stores,
  interval 0, or max age 0; replaces `StartBlobSweeperFromConfig`

## 4. Wiring (block-processor only)

- [x] 4.1 `store.Registry` gains `Blob BlobStore`; set in the Aerospike and SQL
  factories; per-process sweeper start removed from both
- [x] 4.2 `cmd/block-processor/main.go` starts `StartAgeSweeperFromConfig(registry.Blob,
  cfg.BlobStore, logger)` after registry construction and stops it on shutdown

## 5. Tests

- [x] 5.1 `internal/store/file_blob_sweep_test.go` rewritten for the new contract: old
  subtree blob swept with bytes accounted; fresh blob kept; aged STUMP never swept;
  uppercase/63-char/non-hex names never swept; `.dah/` manifests untouched and still
  firing; zero-byte litter reaped at 10min under a 30min maxAge (including under
  `stump/`), fresh zero-byte kept, zero-byte `.dah/` file kept; runner sweeps
  immediately with metric deltas and idempotent stop; from-config helper covers the
  enabled / interval-0 / max-age-0 / memory-store cases
- [x] 5.2 Gates: `go build ./...`, `go test ./... -count=1`, `go test -race` on
  internal/store + internal/config + internal/metrics, `make lint`
