## MODIFIED Requirements

### Requirement: Callback delivery deduplication
The callback delivery system SHALL check whether a specific txid/callbackURL/statusType combination has already been successfully delivered before attempting HTTP delivery. If already delivered, the message SHALL be acknowledged without making an HTTP request. Dedup checks and records SHALL operate correctly under concurrent delivery from multiple worker goroutines.

#### Scenario: First delivery of a callback
- **WHEN** a stumps message arrives for txid T, callbackURL U, and statusType S AND no prior successful delivery exists for (T, U, S)
- **THEN** the system SHALL deliver the HTTP POST to the callback URL AND record the successful delivery in Aerospike

#### Scenario: Duplicate callback message
- **WHEN** a stumps message arrives for txid T, callbackURL U, and statusType S AND a prior successful delivery exists for (T, U, S)
- **THEN** the system SHALL skip the HTTP POST AND acknowledge the Kafka message

#### Scenario: Failed delivery followed by retry
- **WHEN** a stumps message arrives for txid T, callbackURL U, statusType S AND the previous delivery attempt failed (no success record exists)
- **THEN** the system SHALL attempt delivery again

#### Scenario: Concurrent dedup safety
- **WHEN** two worker goroutines simultaneously check dedup for the same (T, U, S) key AND neither finds an existing record
- **THEN** both workers MAY deliver the callback (at-most one extra delivery) AND both SHALL record success, with no data corruption or panics

## ADDED Requirements

### Requirement: Concurrent callback delivery
The callback delivery service SHALL process Kafka messages concurrently using a configurable worker pool, decoupling message consumption from HTTP delivery.

#### Scenario: Worker pool initialization
- **WHEN** the delivery service starts with `callback.deliveryWorkers` set to N
- **THEN** the service SHALL spawn N worker goroutines that concurrently process callback deliveries

#### Scenario: Default worker count
- **WHEN** `callback.deliveryWorkers` is not configured
- **THEN** the service SHALL default to 64 delivery workers

#### Scenario: Backpressure from slow endpoints
- **WHEN** all N workers are blocked waiting on slow HTTP responses AND the internal dispatch channel is full
- **THEN** the Kafka consumer SHALL pause consuming new messages until a worker becomes available (backpressure)

#### Scenario: Graceful shutdown with in-flight deliveries
- **WHEN** the delivery service receives a stop signal AND there are in-flight HTTP deliveries
- **THEN** the service SHALL wait for all in-flight deliveries to complete (up to a configurable timeout) before shutting down

### Requirement: Tuned HTTP connection pooling
The delivery service SHALL configure its HTTP client transport for high-throughput delivery to many endpoints.

#### Scenario: Connection reuse
- **WHEN** a worker delivers a callback to endpoint E and the connection is idle
- **THEN** the HTTP client SHALL reuse the idle connection for subsequent deliveries to E (up to `MaxIdleConnsPerHost`)

#### Scenario: Connection limits
- **WHEN** `callback.maxConnsPerHost` is configured
- **THEN** the HTTP client SHALL NOT open more than that many concurrent connections to any single endpoint

#### Scenario: Default transport settings
- **WHEN** no explicit HTTP transport configuration is provided
- **THEN** the HTTP client SHALL use MaxIdleConnsPerHost=16, MaxConnsPerHost=32, and disable compression for small payloads
