## Why

Debugging the merkle-service requires manually querying Aerospike and tailing Kafka topics to understand what's happening. There's no easy way to see registered transactions, their callback URLs, or what callbacks have been delivered. Developers also need a way to simulate the "business user" side (Arcade) — registering txids and receiving callbacks — without running Arcade itself.

## What Changes

- Add a standalone `tools/debug-dashboard/` directory containing a self-contained Go binary
- The dashboard provides an HTTP server with two roles:
  1. **Dashboard UI**: A web interface showing registered transactions, their callback statuses, and received callback messages
  2. **Callback receiver**: Exposes an endpoint that can be used as the `callbackUrl` when registering txids via `/watch`, storing all received callback payloads for inspection
- The tool connects directly to Aerospike to read registrations (by txid lookup)
- The tool provides a simple form to register new txids via the merkle-service API, automatically using the dashboard's own callback receiver URL
- All received callbacks are stored in-memory and displayed in the dashboard
- Completely self-contained — no modifications to existing merkle-service code

## Capabilities

### New Capabilities
- `debug-dashboard`: Standalone debugging tool with web UI for viewing registrations, receiving callbacks, and registering test transactions

### Modified Capabilities

## Impact

- **New directory**: `tools/debug-dashboard/` — isolated from the main codebase
- **Dependencies**: Uses the existing `internal/store` and `internal/config` packages for Aerospike connectivity
- **No changes to existing code** — purely additive tooling
