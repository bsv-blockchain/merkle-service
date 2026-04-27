## ADDED Requirements

### Requirement: Deterministic fixture generation
The system SHALL provide a fixture generator that produces deterministic test data for scale testing. Fixtures SHALL include 50,000 transaction hashes derived from a seed, 50 subtree binary files in DataHub raw format (1,024 × 32 bytes each), and a manifest mapping Arcade instances to their assigned txids and expected callbacks.

#### Scenario: Fixture generation produces stable output
- **WHEN** the fixture generator is run twice with the same seed
- **THEN** the output files are byte-identical

#### Scenario: Fixture data matches DataHub raw format
- **WHEN** a generated subtree binary file is passed to `datahub.ParseRawNodes()`
- **THEN** it returns exactly 1,024 valid nodes without error

#### Scenario: Manifest describes complete distribution
- **WHEN** the manifest is loaded
- **THEN** every one of the 50,000 txids is assigned to exactly one subtree and exactly one Arcade instance, and the union of all Arcade instance txid sets equals the full set of 50,000 txids

### Requirement: Callback server fleet
The system SHALL start 50 HTTP callback servers (one per simulated Arcade instance) that collect received callback payloads in a thread-safe manner.

#### Scenario: All 50 servers start successfully
- **WHEN** the test harness starts the callback server fleet
- **THEN** 50 HTTP servers are listening on ports 19000 through 19049 and each responds to HTTP POST requests

#### Scenario: Servers handle concurrent callbacks
- **WHEN** multiple MINED callbacks arrive simultaneously at the same server
- **THEN** all callbacks are recorded without data races or lost payloads

### Requirement: Registration pre-loading
The system SHALL pre-load all txid→callbackURL registrations into Aerospike and all callback URLs into the callback URL registry before triggering block processing.

#### Scenario: All registrations are queryable
- **WHEN** 50,000 registrations are pre-loaded (1,000 txids × 50 Arcade instances)
- **THEN** `regStore.BatchGet()` for any txid returns the correct callback URL

#### Scenario: Callback URL registry contains all URLs
- **WHEN** registrations are pre-loaded
- **THEN** `urlRegistry.GetAll()` returns all 50 callback URLs

### Requirement: MINED callback completeness
After block processing completes, every registered txid SHALL have been delivered in exactly one MINED callback to its registered callback URL.

#### Scenario: All 50,000 txids receive MINED callbacks
- **WHEN** a block containing 50,000 transactions across 50 subtrees is processed
- **THEN** each of the 50 callback servers has received MINED callbacks whose combined `txids` arrays contain exactly the 1,000 txids registered to that server

#### Scenario: No duplicate MINED deliveries
- **WHEN** all MINED callbacks are collected
- **THEN** no txid appears more than once across all MINED callbacks received by a single callback server

### Requirement: MINED callback correctness
Each MINED callback SHALL include valid STUMP data and the correct block hash.

#### Scenario: STUMP data is valid BRC-0074 binary
- **WHEN** a MINED callback is received with `stumpData`
- **THEN** the base64-decoded stump data is parseable as a BRC-0074 STUMP with the correct block height

#### Scenario: Block hash matches
- **WHEN** a MINED callback is received
- **THEN** the `blockHash` field matches the hash of the test block

### Requirement: BLOCK_PROCESSED callback completeness
After block processing completes, every registered callback URL SHALL receive exactly one BLOCK_PROCESSED callback.

#### Scenario: All 50 callback URLs receive BLOCK_PROCESSED
- **WHEN** a block with registered txids is processed
- **THEN** each of the 50 callback servers receives exactly one BLOCK_PROCESSED callback with the correct block hash

#### Scenario: No duplicate BLOCK_PROCESSED deliveries
- **WHEN** all BLOCK_PROCESSED callbacks are collected
- **THEN** no callback server received more than one BLOCK_PROCESSED callback for the same block

### Requirement: No unexpected callbacks
The test SHALL verify that no callbacks beyond the expected MINED and BLOCK_PROCESSED callbacks are received.

#### Scenario: No spurious callbacks
- **WHEN** all expected callbacks have been received and a drain period elapses
- **THEN** no additional callbacks are received by any server

### Requirement: Performance metrics reporting
The test SHALL collect and report timing metrics for the end-to-end pipeline.

#### Scenario: Timing checkpoints are reported
- **WHEN** the test completes
- **THEN** the test output includes: time from block injection to first MINED callback, time to last MINED callback, time to all BLOCK_PROCESSED callbacks, total wall clock time, and per-server callback counts

#### Scenario: Per-server statistics are reported
- **WHEN** the test completes
- **THEN** the test output includes for each callback server: number of MINED callbacks received, total txids received, total bytes received
