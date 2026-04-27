## Why

The debug dashboard currently hardcodes its own callback URL when registering transactions with the merkle-service. Users need to register transactions with custom callback URLs to test how the merkle-service delivers callbacks to different endpoints (e.g., a local Arcade instance, a staging server, or another debugging tool).

## What Changes

- Allow the registration form to accept a custom callback URL instead of only using the dashboard's built-in receiver
- Default the callback URL field to the dashboard's own receiver but make it editable
- Support registering multiple callback URLs for the same txid (the merkle-service supports this)
- Track which callback URLs were registered per txid in the dashboard's local state

## Capabilities

### New Capabilities

None — this enhances the existing debug dashboard, no new spec needed.

### Modified Capabilities

- `debug-dashboard`: Registration form gains an editable callback URL field and support for multiple callback URLs per txid

## Impact

- `tools/debug-dashboard/handlers.go` — `handleRegister` reads callback URL from form instead of hardcoding
- `tools/debug-dashboard/templates/home.html` — Registration form gets editable callback URL input
- `tools/debug-dashboard/templates/registrations.html` — May need to show multiple callback URLs per txid
- `tools/debug-dashboard/store.go` — No changes expected (already supports multiple callback URLs per txid)
