## ADDED Requirements

### Requirement: Dashboard served at root
The API server SHALL serve an HTML dashboard page at `GET /`. The page SHALL be a self-contained HTML document with inline CSS and JavaScript, embedded in the Go binary using the `embed` package.

#### Scenario: Access dashboard
- **WHEN** a user sends a `GET /` request to the API server
- **THEN** the server responds with `200 OK` and `Content-Type: text/html` containing the dashboard page

### Requirement: Endpoint documentation
The dashboard page SHALL display documentation for all API server endpoints, including method, path, request body format, and response format for each endpoint.

#### Scenario: View endpoint documentation
- **WHEN** the dashboard page loads
- **THEN** the user sees documentation for `POST /watch`, `GET /health`, and `GET /api/lookup/{txid}` endpoints with their request/response schemas

### Requirement: Transaction registration form
The dashboard SHALL provide a form to register a transaction ID with a callback URL. The form SHALL submit the registration via a `fetch()` call to the API server's `POST /watch` endpoint and display the result without a full page reload.

#### Scenario: Register a txid via dashboard
- **WHEN** a user enters a valid 64-character hex txid and a valid callback URL and submits the registration form
- **THEN** the dashboard sends a `POST /watch` request with `{"txid": "<txid>", "callbackUrl": "<url>"}` and displays a success message

#### Scenario: Registration validation error
- **WHEN** a user submits the registration form with an invalid txid or empty callback URL
- **THEN** the dashboard displays the error response from the API server without reloading the page

### Requirement: Transaction lookup
The dashboard SHALL provide a form to look up a transaction ID. The lookup SHALL query the API server's `GET /api/lookup/{txid}` endpoint and display the registered callback URLs.

#### Scenario: Lookup a registered txid
- **WHEN** a user enters a txid and submits the lookup form
- **THEN** the dashboard sends a `GET /api/lookup/{txid}` request and displays the list of registered callback URLs

#### Scenario: Lookup an unregistered txid
- **WHEN** a user looks up a txid that has no registrations
- **THEN** the dashboard displays a message indicating no registrations were found

### Requirement: Lookup API endpoint
The API server SHALL expose a `GET /api/lookup/{txid}` endpoint that returns the registered callback URLs for a given transaction ID from Aerospike.

#### Scenario: Lookup existing registration
- **WHEN** a client sends `GET /api/lookup/{txid}` for a txid with registered callback URLs
- **THEN** the server responds with `200 OK` and `{"txid": "<txid>", "callbackUrls": ["url1", "url2"]}`

#### Scenario: Lookup non-existent registration
- **WHEN** a client sends `GET /api/lookup/{txid}` for a txid with no registrations
- **THEN** the server responds with `200 OK` and `{"txid": "<txid>", "callbackUrls": []}`

#### Scenario: Lookup with invalid txid
- **WHEN** a client sends `GET /api/lookup/{txid}` where txid is not a valid 64-character hex string
- **THEN** the server responds with `400 Bad Request` and `{"error": "invalid txid format"}`

### Requirement: Dashboard styling
The dashboard SHALL use the same dark theme styling (GitHub-inspired, #0d1117 background) as the existing debug-dashboard to maintain visual consistency.

#### Scenario: Visual consistency
- **WHEN** the dashboard page renders
- **THEN** it uses a dark background (#0d1117), light text (#c9d1d9), and card-based layout matching the debug-dashboard's visual style
