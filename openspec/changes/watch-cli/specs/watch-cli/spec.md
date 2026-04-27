## ADDED Requirements

### Requirement: Register a single txid via CLI
The `watch` CLI SHALL accept `--txid`, `--callback`, and `--url` flags and register the txid by sending a single `POST /watch` request to the API. The txid SHALL be validated as a 64-character hex string before sending.

#### Scenario: Successful single registration
- **WHEN** `watch --url http://host:8080 --txid <valid-txid> --callback http://cb.example/` is run AND the API returns 200
- **THEN** the CLI prints `OK <txid>` and exits with code 0

#### Scenario: Invalid txid format
- **WHEN** `--txid` is not a 64-character hex string
- **THEN** the CLI prints an error and exits with code 2 without sending any HTTP request

#### Scenario: API returns error
- **WHEN** the API returns a non-200 status
- **THEN** the CLI prints `FAIL <txid>: <error>` and exits with code 1

### Requirement: Register txids in bulk from a file
The `watch` CLI SHALL accept `--file <path>` to read txids from a newline-delimited file. Blank lines and lines starting with `#` SHALL be skipped. All txids SHALL be validated before any requests are sent.

#### Scenario: Successful bulk registration
- **WHEN** `watch --url http://host:8080 --file txids.txt --callback http://cb.example/` is run AND all registrations succeed
- **THEN** the CLI prints `OK <txid>` for each and exits with code 0

#### Scenario: File contains invalid txid
- **WHEN** the file contains a line that is not a valid 64-char hex txid
- **THEN** the CLI reports the invalid line and exits with code 2 without sending any requests

#### Scenario: Partial failure in bulk
- **WHEN** N of M registrations fail
- **THEN** the CLI prints `FAIL <txid>: <reason>` for each failure and exits with code 1

#### Scenario: Read from stdin
- **WHEN** `--file -` is specified
- **THEN** the CLI reads txids from stdin

### Requirement: Concurrent bulk registration
The `watch` CLI SHALL send bulk registration requests concurrently up to the limit set by `--concurrency` (default 10).

#### Scenario: Concurrency limits parallel requests
- **WHEN** `--concurrency 5` is set and 100 txids are registered
- **THEN** at most 5 HTTP requests are in-flight simultaneously

### Requirement: Configurable HTTP timeout
The `watch` CLI SHALL apply a per-request HTTP timeout controlled by `--timeout` (default 10s). If a request exceeds the timeout it SHALL be reported as a failure.

#### Scenario: Request times out
- **WHEN** the API does not respond within the configured timeout
- **THEN** the CLI prints `FAIL <txid>: request timeout` and counts the txid as failed

### Requirement: Quiet output mode
The `watch` CLI SHALL support `--quiet` to suppress per-txid output and print only a final summary line `registered N/M`.

#### Scenario: Quiet mode suppresses per-txid output
- **WHEN** `--quiet` is set and all registrations succeed
- **THEN** the CLI prints `registered M/M` and no per-txid lines

### Requirement: Required flags and help
The `watch` CLI SHALL require `--url` and `--callback` flags. If either is missing, it SHALL print usage and exit with code 2. `--help` SHALL print all available flags with descriptions and defaults.

#### Scenario: Missing required flag
- **WHEN** `--url` or `--callback` is omitted
- **THEN** the CLI prints usage information and exits with code 2

#### Scenario: Help flag
- **WHEN** `--help` is passed
- **THEN** the CLI prints all flags with descriptions and exits with code 0
