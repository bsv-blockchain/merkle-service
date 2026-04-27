## ADDED Requirements

### Requirement: Register transaction with callback URL
The system SHALL expose a REST API endpoint `POST /watch` that accepts a transaction ID (txid) and a callback URL. The registration SHALL be stored in Aerospike as a mapping from txid to a set of callback URLs. Multiple callback URLs SHALL be supported per txid using Aerospike CDT set operations.

> Traces to: requirements.md — "api-server: registers transactions/callbacks for businesses, stores in aerospike"

#### Scenario: Register a new transaction
- **WHEN** a client sends `POST /watch` with a valid txid and callback URL
- **THEN** the system creates a new Aerospike record with the txid as key and the callback URL in a set
- **THEN** the system responds with HTTP 200 and a confirmation body

#### Scenario: Register additional callback for existing transaction
- **WHEN** a client sends `POST /watch` with a txid that already has registered callbacks and a new callback URL
- **THEN** the system adds the new callback URL to the existing set for that txid using Aerospike CDT set append
- **THEN** the system responds with HTTP 200

#### Scenario: Idempotent duplicate registration
- **WHEN** a client sends `POST /watch` with a txid and callback URL that is already registered
- **THEN** the system does not create a duplicate entry (set semantics ensure uniqueness)
- **THEN** the system responds with HTTP 200

### Requirement: Validate registration requests
The system SHALL validate all incoming `/watch` requests before persisting to Aerospike. Invalid requests SHALL be rejected with appropriate HTTP error codes.

> Traces to: requirements.md — "api-server: registers transactions/callbacks for businesses"

#### Scenario: Missing txid
- **WHEN** a client sends `POST /watch` without a txid
- **THEN** the system responds with HTTP 400 and an error message indicating txid is required

#### Scenario: Invalid txid format
- **WHEN** a client sends `POST /watch` with a txid that is not a valid 64-character hex string
- **THEN** the system responds with HTTP 400 and an error message indicating invalid txid format

#### Scenario: Missing callback URL
- **WHEN** a client sends `POST /watch` without a callback URL
- **THEN** the system responds with HTTP 400 and an error message indicating callback URL is required

#### Scenario: Invalid callback URL
- **WHEN** a client sends `POST /watch` with a callback URL that is not a valid HTTP/HTTPS URL
- **THEN** the system responds with HTTP 400 and an error message indicating invalid callback URL

### Requirement: Health check endpoint
The system SHALL expose a health check endpoint that reports the readiness of the API server and its Aerospike connection.

> Traces to: requirements.md — "Follow the same daemon/service pattern that Teranode uses"

#### Scenario: Healthy API server
- **WHEN** a client sends `GET /health`
- **THEN** the system responds with HTTP 200 and a JSON body indicating healthy status including Aerospike connectivity
