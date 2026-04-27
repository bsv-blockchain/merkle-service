## Why

The STUMP visualizer currently only exists in the standalone debug dashboard (`tools/debug-dashboard`), which runs on a separate port and requires a separate deployment. Operators and developers using the main merkle-service API dashboard (`/` on port 8080) cannot visualize STUMP data without switching to the debug tool. Adding the visualizer to the main UI makes it accessible to anyone with access to the API server.

## What Changes

- Add a new "STUMP Visualizer" tab to the main API dashboard at `internal/api/dashboard.html`.
- Port the client-side BRC-0074 BUMP decoder and SVG tree renderer from the debug dashboard's `stump.html` into the main dashboard.
- Support both base64 and hexadecimal input formats (auto-detected). The callback system now sends STUMP data as hex (Arcade's `HexBytes`), so hex support is essential.
- The visualizer is purely client-side (JavaScript) — no new API endpoints or backend changes needed.

## Capabilities

### New Capabilities
_(none — no new spec-level capability; this reuses the existing stump-visualizer behavior)_

### Modified Capabilities
- `stump-visualizer`: The STUMP visualizer is now also accessible as a tab in the main API dashboard, in addition to its existing `/stump` route in the debug dashboard.

## Impact

- **Main dashboard HTML**: `internal/api/dashboard.html` gains a new tab with ~500 lines of JavaScript (decoder + renderer). File size increases but remains a single self-contained HTML file.
- **No backend changes**: No new routes, handlers, or Go code changes.
- **No breaking changes**: Existing tabs (Endpoints, Register, Lookup) are unchanged.
