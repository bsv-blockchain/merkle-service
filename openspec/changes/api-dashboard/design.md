## Context

The API server (`internal/api/`) currently serves two endpoints: `POST /watch` (register txid with callback URL) and `GET /health`. The debug-dashboard (`tools/debug-dashboard/`) is a separate Go binary that provides a web UI for registration, lookup, and callback inspection — but it connects directly to Aerospike and uses in-memory stores (CallbackStore, TxidTracker) for session state.

In production, operators need diagnostic visibility without deploying a separate tool. The API server should serve a self-contained HTML dashboard at its root that uses only the API server's own HTTP endpoints.

## Goals / Non-Goals

**Goals:**
- Serve a single-page HTML dashboard at `GET /` from the API server
- Add JSON API endpoints for txid lookup so the dashboard can query data
- Dashboard loads all data via `fetch()` calls to the API server — no direct DB access from the client
- Replicate core debug-dashboard functionality: register txid, lookup txid, view endpoint documentation
- Zero new dependencies — use Go `embed`, existing chi router

**Non-Goals:**
- Callback receiver/storage — the API server does not receive callbacks (that's the client's responsibility)
- Real-time callback display — without a callback receiver, there's no callback feed to show
- STUMP visualizer — this is client-side JS and can be added later if needed
- Session state or in-memory tracking — the dashboard is stateless, querying Aerospike on demand
- Authentication or access control for the dashboard

## Decisions

### 1. Single embedded HTML file with inline JS/CSS

**Decision**: Embed one `dashboard.html` file using `//go:embed` with all CSS and JavaScript inline.

**Rationale**: The debug-dashboard uses Go templates with server-side rendering. For the API server dashboard, a single self-contained HTML file with client-side `fetch()` calls is simpler — no template engine needed, no server-side session state, and the file can be served as a static asset. One file keeps the embed simple and avoids needing to serve a directory of assets.

**Alternative considered**: Go templates like the debug-dashboard. Rejected because it would require adding template parsing, page data structs, and server-side state to the API server, which should remain a thin API layer.

### 2. New API endpoints under `/api/` prefix

**Decision**: Add lookup endpoint at `GET /api/lookup/{txid}` to query Aerospike for registered callback URLs for a given txid.

**Rationale**: The dashboard needs to query registration data. Rather than having the HTML page call Aerospike directly (impossible from browser), we expose a JSON endpoint. The `/api/` prefix separates dashboard-support endpoints from the existing top-level routes (`/watch`, `/health`). The existing `POST /watch` endpoint already handles registration, so no new registration endpoint is needed.

**Alternative considered**: Adding a batch lookup endpoint. Deferred — single txid lookup covers the primary use case.

### 3. Stateless dashboard

**Decision**: The dashboard maintains no server-side state. Each lookup queries Aerospike directly. There is no txid tracker or callback store in the API server.

**Rationale**: The API server follows the Teranode service pattern (Init/Start/Stop/Health). Adding in-memory stores would complicate lifecycle management and is unnecessary — the source of truth is Aerospike. The debug-dashboard's in-memory stores exist because it's a development tool; the production dashboard should query the real data store.

### 4. Dashboard documents all API endpoints

**Decision**: The HTML page includes a static section documenting all available API endpoints (method, path, request/response format) alongside the interactive forms.

**Rationale**: This makes the API server self-documenting. When an operator hits the root URL, they immediately see what the service offers and can interact with it.

## Risks / Trade-offs

- **[Risk] Dashboard adds surface area to production API** → The dashboard is read-only (lookup) plus the existing `/watch` endpoint. No new write operations are introduced. The dashboard HTML is a static embed with no template injection risk.
- **[Risk] Large HTML file bloats binary** → The debug-dashboard templates total ~770 lines. A single-page dashboard will be similar or smaller. Impact on binary size is negligible.
- **[Trade-off] No callback viewing** → Unlike the debug-dashboard, the API server dashboard cannot display received callbacks since the API server is not a callback receiver. This is acceptable — callback inspection remains a debug-dashboard concern.
