## ADDED Requirements

### Requirement: Subtree blob store is bounded by an age sweeper
The service SHALL run a periodic age sweeper against file-backed blob stores, in the
block-processor only, that deletes top-level 64-lowercase-hex blob files (subtree
blobs) whose modification time is older than `blobStore.sweepMaxAgeSec` (default 1800),
and zero-byte files older than ~5 minutes anywhere outside `.dah/`. The sweeper MUST
NOT touch STUMP blobs (the `stump/` namespace) at any age, MUST NOT touch `.dah/`
bookkeeping, and MUST NOT delete files whose names do not match the subtree key shape.
The sweep runs once at startup and then every `blobStore.sweepIntervalSec` (default
300); either key set to 0 disables the sweeper. A nonzero `blobStore.sweepMaxAgeSec`
below 600 MUST be rejected at startup. Files and bytes reclaimed SHALL be exposed via
`merkle_blobstore_swept_files_total` and `merkle_blobstore_swept_bytes_total`.

#### Scenario: Orphaned subtree blob is reclaimed
- **WHEN** a top-level 64-lowercase-hex blob file's mtime is older than sweepMaxAgeSec because its subtree-work item never completed (no delete-at-height was ever scheduled)
- **THEN** the sweeper removes it and accounts its size in the swept files/bytes counters, and a later request for that subtree falls through to the DataHub re-fetch path

#### Scenario: STUMP blobs survive any age
- **WHEN** a blob under the stump/ namespace is older than sweepMaxAgeSec
- **THEN** the sweeper leaves it in place, because callback-delivery reads STUMPs at delivery time with retry windows up to ~1h

#### Scenario: DAH bookkeeping survives sweeping
- **WHEN** a .dah/ manifest file is older than sweepMaxAgeSec
- **THEN** the sweeper leaves it in place and the schedule still fires when its height is reached

#### Scenario: Zero-byte litter is reaped early
- **WHEN** a zero-byte blob file outside .dah/ is older than ~5 minutes but younger than sweepMaxAgeSec
- **THEN** the sweeper removes it, because failed writes (ENOSPC) leave empty files that are never valid data

#### Scenario: Sweeper disabled by configuration
- **WHEN** blobStore.sweepIntervalSec or blobStore.sweepMaxAgeSec is 0
- **THEN** no sweeper runs and no blob files are age-deleted

#### Scenario: Unsafe sweep age is rejected at startup
- **WHEN** blobStore.sweepMaxAgeSec is nonzero and below 600
- **THEN** configuration loading fails, because a sweep age under one block interval could delete the in-flight block's subtree blobs
