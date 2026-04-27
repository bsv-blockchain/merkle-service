## ADDED Requirements

### Requirement: Dashboard serves a web UI on a configurable port
The debug dashboard SHALL start an HTTP server on a configurable port (default 9900) that serves the dashboard web interface and callback receiver.

#### Scenario: Dashboard starts with default port
- **WHEN** the dashboard is started without specifying a port
- **THEN** it listens on port 9900

#### Scenario: Dashboard starts with custom port
- **WHEN** the dashboard is started with `-port 8888`
- **THEN** it listens on port 8888

### Requirement: Dashboard displays a home page with navigation
The dashboard SHALL display a home page at `/` with links to the registrations view and the callback log.

#### Scenario: Home page loads
- **WHEN** a user navigates to `/`
- **THEN** the page displays the dashboard title, navigation links, and a summary of tracked txids and received callbacks

### Requirement: User can register a txid through the dashboard
The dashboard SHALL provide a form to register a txid with the merkle-service API. The callback URL SHALL be auto-populated with the dashboard's own callback receiver endpoint.

#### Scenario: Register a new txid
- **WHEN** a user enters a 64-character hex txid in the registration form and submits
- **THEN** the dashboard sends a POST to the merkle-service `/watch` endpoint with the txid and the dashboard's callback URL, adds the txid to its local tracking list, and displays a success message

#### Scenario: Register with invalid txid
- **WHEN** a user submits a txid that is not exactly 64 hex characters
- **THEN** the dashboard displays a validation error without calling the merkle-service API

### Requirement: User can look up a txid's registration status
The dashboard SHALL allow looking up any txid against Aerospike to see its registered callback URLs.

#### Scenario: Look up a registered txid
- **WHEN** a user enters a txid in the lookup form and it has registrations in Aerospike
- **THEN** the dashboard displays all callback URLs associated with that txid

#### Scenario: Look up an unregistered txid
- **WHEN** a user enters a txid that has no registrations in Aerospike
- **THEN** the dashboard displays "No registrations found" for that txid

### Requirement: Dashboard displays all tracked txids
The dashboard SHALL display a table of all txids that have been registered through its UI or manually looked up, showing their callback URLs from Aerospike.

#### Scenario: View tracked registrations
- **WHEN** a user navigates to the registrations page
- **THEN** the page lists all tracked txids with their registered callback URLs and registration time

### Requirement: Callback receiver stores incoming callbacks
The dashboard SHALL expose a `POST /callbacks/receive` endpoint that accepts callback payloads (matching the merkle-service callback format) and stores them in memory.

#### Scenario: Receive a SEEN_ON_NETWORK callback
- **WHEN** the merkle-service delivers a callback POST to `/callbacks/receive` with status `SEEN_ON_NETWORK`
- **THEN** the dashboard stores the full payload with a timestamp and returns HTTP 200

#### Scenario: Receive a MINED callback
- **WHEN** the merkle-service delivers a callback POST to `/callbacks/receive` with status `MINED` and stumpData
- **THEN** the dashboard stores the full payload including stumpData with a timestamp and returns HTTP 200

#### Scenario: Callback store capacity limit
- **WHEN** the in-memory callback store has reached its maximum capacity (default 1000)
- **THEN** the oldest callback entry is evicted to make room for the new one

### Requirement: Dashboard displays received callbacks
The dashboard SHALL display a log of all received callbacks in reverse chronological order.

#### Scenario: View callback log
- **WHEN** a user navigates to the callbacks page
- **THEN** the page displays all received callbacks showing timestamp, txid, status type, and payload details

#### Scenario: Empty callback log
- **WHEN** no callbacks have been received yet
- **THEN** the page displays "No callbacks received yet"

### Requirement: Dashboard connects to Aerospike using merkle-service config
The dashboard SHALL reuse the merkle-service configuration system (config file, environment variables) to connect to Aerospike.

#### Scenario: Dashboard reads config from environment
- **WHEN** the dashboard starts with `AEROSPIKE_HOST=aerospike.local`
- **THEN** it connects to the Aerospike instance at `aerospike.local`

#### Scenario: Dashboard reads config from config file
- **WHEN** the dashboard starts with `CONFIG_FILE=./config.yaml`
- **THEN** it reads Aerospike connection settings from the config file

### Requirement: Dashboard is self-contained in tools/debug-dashboard
All dashboard code, templates, and assets SHALL be contained within the `tools/debug-dashboard/` directory.

#### Scenario: Run the dashboard
- **WHEN** a developer runs `go run ./tools/debug-dashboard` from the repository root
- **THEN** the dashboard starts without requiring any additional build steps or file setup
