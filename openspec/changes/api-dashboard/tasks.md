## 1. Lookup API Endpoint

- [x] 1.1 Add `handleLookup` handler to `internal/api/handlers.go` that accepts `GET /api/lookup/{txid}`, validates the txid format, queries `regStore.Get(txid)`, and returns JSON `{"txid": "...", "callbackUrls": [...]}` or appropriate error responses
- [x] 1.2 Register the `GET /api/lookup/{txid}` route in `internal/api/server.go` Init method
- [x] 1.3 Add `LookupResponse` struct to `internal/api/handlers.go` with `Txid` and `CallbackUrls` fields

## 2. Dashboard HTML

- [x] 2.1 Create `internal/api/dashboard.html` — single self-contained HTML file with inline CSS (dark theme matching debug-dashboard) and inline JavaScript
- [x] 2.2 Add endpoint documentation section to dashboard listing `POST /watch`, `GET /health`, and `GET /api/lookup/{txid}` with request/response schemas
- [x] 2.3 Add registration form section with txid input (64 hex chars) and callback URL input, submitting via `fetch()` to `POST /watch`
- [x] 2.4 Add lookup form section with txid input, querying via `fetch()` to `GET /api/lookup/{txid}` and displaying callback URLs
- [x] 2.5 Add response display areas for registration results (success/error) and lookup results (URL list or "no registrations found")
- [x] 2.6 Style the dashboard with the same dark GitHub theme (#0d1117 background, #c9d1d9 text, card-based layout, badge styles) from the debug-dashboard's `layout.html`

## 3. Embed and Serve Dashboard

- [x] 3.1 Add `//go:embed dashboard.html` directive in `internal/api/server.go` (or a new `embed.go` file) to embed the HTML file
- [x] 3.2 Add `handleDashboard` handler that serves the embedded HTML with `Content-Type: text/html`
- [x] 3.3 Register `GET /` route in `internal/api/server.go` Init method pointing to `handleDashboard`

## 4. Tests for Lookup Endpoint

- [x] 4.1 Add test for `GET /api/lookup/{txid}` with a valid txid that has registrations — verify 200 response with correct JSON structure
- [x] 4.2 Add test for `GET /api/lookup/{txid}` with a valid txid that has no registrations — verify 200 response with empty `callbackUrls` array
- [x] 4.3 Add test for `GET /api/lookup/{txid}` with an invalid txid — verify 400 response with error message
- [x] 4.4 Add test for `GET /` — verify 200 response with `Content-Type: text/html`

## 5. Integration Verification

- [x] 5.1 Verify the API server builds and starts with the embedded dashboard (`go build ./cmd/api-server/...`)
- [ ] 5.2 Manually verify dashboard loads at `http://localhost:<port>/` and all interactive forms work against the running API server
