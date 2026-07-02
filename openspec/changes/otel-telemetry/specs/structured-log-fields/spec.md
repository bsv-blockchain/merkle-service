## ADDED Requirements

### Requirement: Canonical snake_case log-field keys
The system SHALL log well-known identifiers under a fixed set of snake_case keys, shared with the arcade repository's own log-field canon, so the same identifier is always spelled the same way across both services: `txid`, `txids`, `txid_count`, `txids_truncated`, `block_hash`, `block_height`, `subtree_hash`, `subtree_index`, `callback_url`, `datahub_url`, `peer_id`, `request_id`, `trace_id`, `span_id`. Call sites SHALL use the typed constructors in `internal/logfields` rather than bare string keys.

#### Scenario: A list and a count of the same identifier use distinct keys
- **WHEN** the system logs both a list of txids and a count of txids on related log records
- **THEN** the list SHALL be logged under `txids` and the count SHALL be logged under `txid_count`, never the same key for both

#### Scenario: Truncated txid lists are marked
- **WHEN** a `txids` list logged on a record was capped rather than complete
- **THEN** the record SHALL also carry `txids_truncated: true`

### Requirement: Pre-canon log-field keys are banned outside wire formats
The system SHALL fail its build (`make lint-logfields`) if a pre-canon log key literal (e.g. `blockHash`, `subtreeHash`, `subtreeID`, `subtreeIndex`, `callbackUrl`, `callbackURL`, `blockHeight`, `peerID`, `peerId`, `requestId`, `dataHubUrl`) appears in non-test Go source under `internal/`, `cmd/`, or `tools/`, except within struct tags (`json:`, `yaml:`, `mapstructure:`) or `FormValue` reads, which are wire formats and must not be renamed to match the log-field canon.

#### Scenario: Banned key literal in a log call fails lint
- **WHEN** a non-test Go source file under `internal/`, `cmd/`, or `tools/` contains a banned pre-canon key literal outside a struct tag or `FormValue` read
- **THEN** `make lint-logfields` SHALL fail

#### Scenario: Struct tags and form fields are exempt
- **WHEN** a banned key literal appears only within a `json:`, `yaml:`, or `mapstructure:` struct tag, or as a `FormValue` argument
- **THEN** `make lint-logfields` SHALL pass

### Requirement: Registration acceptance is logged with txid and callback URL
The `/watch` registration endpoint SHALL log an Info-level `"registration accepted"` record carrying the registered `txid` and `callback_url` for every successful registration.

#### Scenario: Successful registration is logged
- **WHEN** a transaction registration via `/watch` succeeds
- **THEN** the system SHALL log `"registration accepted"` with the `txid` and `callback_url` fields set to the registered values

### Requirement: SEEN callback batches log matched txids up to a configurable cap
After a SEEN_ON_NETWORK or SEEN_MULTIPLE_NODES callback batch is durably published, the system SHALL log an Info-level `"seen callback batch published"` record carrying `subtree_hash`, `callback_url`, and `txid_count`; when the configured `subtree.seenTxidLogMax` (default 1000, env `SUBTREE_SEEN_TXID_LOG_MAX`) is greater than zero, the record SHALL also carry the batch's `txids`, capped at that limit, with `txids_truncated` set to `true` when the batch exceeded the cap and `false` otherwise; when `subtree.seenTxidLogMax` is `0`, the `txids` field SHALL be omitted entirely and only the count SHALL be logged.

#### Scenario: Batch under the cap logs full txid list
- **WHEN** a SEEN_ON_NETWORK or SEEN_MULTIPLE_NODES batch of size less than or equal to `subtree.seenTxidLogMax` is published
- **THEN** the log record SHALL include all matched txids under `txids` and `txids_truncated: false`

#### Scenario: Batch exceeding the cap logs a truncated txid list
- **WHEN** a batch of size greater than `subtree.seenTxidLogMax` is published
- **THEN** the log record SHALL include only the first `subtree.seenTxidLogMax` txids under `txids` and `txids_truncated: true`
- **AND** `txid_count` SHALL reflect the full (untruncated) batch size

#### Scenario: seenTxidLogMax of zero logs counts only
- **WHEN** `subtree.seenTxidLogMax` is `0`
- **THEN** the log record SHALL include `txid_count` but SHALL NOT include a `txids` field

#### Scenario: A failed publish is not logged as success
- **WHEN** publishing a SEEN batch fails
- **THEN** the system SHALL NOT log `"seen callback batch published"` for that batch

### Requirement: block_hash continuity across the block-processing pipeline
The system SHALL log `block_hash` under the canonical key at every stage of block processing — p2p-client receipt, block-processor dispatch, subtree-worker STUMP building, subtree-counter bookkeeping, and BLOCK_PROCESSED callback delivery — so a single `block_hash` search reconstructs a block's full processing lifecycle.

#### Scenario: A block hash is searchable across every processing stage
- **WHEN** a block is received, processed, and its BLOCK_PROCESSED callback delivered
- **THEN** a search for that block's `block_hash` value SHALL return log records from the p2p-client, block-processor, subtree-worker, and callback-delivery stages, all under the same `block_hash` key
