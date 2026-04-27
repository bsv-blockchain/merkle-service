## Why

The API server currently exposes only `/watch` and `/health` endpoints with no discoverability or diagnostic UI. In production, operators need a way to verify the service is working, inspect registered data, and test endpoints without deploying the separate debug-dashboard tool (which connects directly to Aerospike). By embedding a dashboard HTML page into the API server itself, we get a self-documenting, self-diagnostic production endpoint that loads all data through the API server's own endpoints rather than direct database access.

## What Changes

- Add a default HTML page served at `GET /` on the API server that documents all available endpoints and provides interactive diagnostic functionality
- Add new JSON API endpoints to the API server to support dashboard data needs:
  - `GET /api/lookup/{txid}` — look up registered callback URLs for a txid
  - `GET /api/registrations` — list recent registrations (query parameter for txids)
- The HTML dashboard is a single-page application that calls API server endpoints via `fetch()` — no server-side template rendering, no in-memory stores on the server
- Embed the HTML as a static file using Go's `embed` package
- The dashboard replicates the debug-dashboard's core functionality: registration form, txid lookup, endpoint documentation

## Capabilities

### New Capabilities
- `api-dashboard`: Embedded HTML dashboard served by the API server at `/` that documents endpoints and provides interactive registration/lookup functionality, loading all data from API endpoints

### Modified Capabilities

## Impact

- **Code**: `internal/api/` — new handlers, routes, and embedded HTML asset
- **APIs**: New `GET /`, `GET /api/lookup/{txid}` endpoints added to the API server
- **Dependencies**: No new dependencies — uses Go `embed` and existing chi router
- **Systems**: No infrastructure changes — the dashboard is served by the existing API server process
