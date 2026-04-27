## Context

The merkle-service currently has no observability tooling beyond the `/health` endpoint. Developers debugging registration flows, callback delivery, or P2P message handling must manually query Aerospike and tail Kafka topics. There's also no easy way to simulate the "business user" side — you need a running Arcade instance or a manual curl loop to receive callbacks.

The registration store only supports lookup by known txid(s) — there's no scan/list-all capability in Aerospike's CDT-based design. This constrains what the dashboard can show natively.

## Goals / Non-Goals

**Goals:**
- Provide a single `go run` command that starts a debugging web UI
- Allow developers to register test txids (via the merkle-service API) with the dashboard's own callback URL
- Capture and display all received callbacks in real-time
- Look up registration details for specific txids
- Keep the tool completely isolated in `tools/debug-dashboard/`

**Non-Goals:**
- Production monitoring or alerting — this is a developer tool
- Scanning all Aerospike registrations (not supported by the store API)
- Kafka topic inspection — the dashboard focuses on the HTTP API and callback layer
- Persistent storage of received callbacks — in-memory only, resets on restart

## Decisions

### 1. Single binary in `tools/debug-dashboard/`

**Choice**: A standalone `main.go` in `tools/debug-dashboard/` using Go's standard library `net/http` with embedded HTML templates.

**Rationale**: No external web framework needed. The tool should be trivially runnable with `go run ./tools/debug-dashboard` from the repo root. Using Go's `embed` package for HTML keeps it as a single directory with no build step. It can import `internal/` packages since it's within the same module.

**Alternative considered**: Separate Go module in `tools/debug-dashboard/`. Rejected because it would need to duplicate or vendor the store/config packages.

### 2. In-memory callback store with mutex protection

**Choice**: Store received callbacks in a `[]CallbackEntry` protected by `sync.RWMutex`, with a configurable max capacity (default 1000).

**Rationale**: This is a debugging tool — persistence isn't needed. In-memory storage is simple, fast, and avoids additional dependencies. The capacity limit prevents unbounded memory growth during extended debugging sessions.

### 3. Txid tracking via local registry

**Choice**: Maintain a local in-memory list of txids registered through the dashboard. For each, allow lookup against Aerospike via the existing `RegistrationStore.Get()` method.

**Rationale**: Since there's no scan-all API in the registration store, the dashboard tracks txids it knows about: those registered through its own UI plus any manually entered for lookup. This provides a practical view without requiring Aerospike schema changes.

### 4. HTML-rendered server-side pages (no JS framework)

**Choice**: Server-rendered HTML using Go's `html/template` package with embedded templates. Simple forms for actions, page refreshes for updates.

**Rationale**: Minimal complexity for a developer tool. No npm, no build pipeline, no JS bundling. A developer can read and modify the templates directly. HTMX or similar could be added later if needed, but server-rendered HTML is sufficient for v1.

### 5. Dual-port architecture

**Choice**: The dashboard listens on a single configurable port (default 9900) serving both the dashboard UI and the callback receiver endpoint.

**Rationale**: A single port simplifies setup. The callback URL is `http://<host>:<port>/callbacks/receive`. When registering a txid, the dashboard auto-fills this URL. The same server serves the dashboard pages and receives callbacks.

**Alternative considered**: Two separate ports (one for UI, one for callbacks). Rejected as unnecessary complexity for a debugging tool.

## Risks / Trade-offs

- **[No scan-all for registrations]** → The dashboard can only show txids it knows about (registered through its UI or manually looked up). Mitigation: clear UX showing this is a lookup tool, not a full inventory.
- **[In-memory only]** → Callback history is lost on restart. Mitigation: acceptable for a debugging tool; add a "clear" button for manual reset.
- **[Imports internal packages]** → The tool depends on `internal/store` and `internal/config`, coupling it to the main codebase. Mitigation: this is intentional — it needs to read from the same Aerospike instance. Being in the same module makes this natural.
