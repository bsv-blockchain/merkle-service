## 1. Add STUMP Tab to Main Dashboard

- [x] 1.1 Add "STUMP" tab button to the tab navigation in `internal/api/dashboard.html`
- [x] 1.2 Add tab content section with textarea input, Decode button, and Load Example button
- [x] 1.3 Add empty containers for SVG tree, metadata panel, detail panel, and legend

## 2. Port JavaScript

- [x] 2.1 Port the BRC-0074 decoder functions (`decodeBase64`, `decodeHex`, `readCompactSize`, `readHash`, `decodeBUMP`) from `tools/debug-dashboard/templates/stump.html`, adding hex decoding support
- [x] 2.1a Add auto-detection function that determines whether input is hex or base64 and decodes accordingly
- [x] 2.2 Port the tree layout engine (`computeTreeLayout`) and SVG renderer (`renderTree`)
- [x] 2.3 Port the interactive features: hover tooltips, node click detail panel, proof path highlighting
- [x] 2.4 Port the metadata display function (`showMetadata`)
- [x] 2.5 Add the example test vector and Load Example button handler
- [x] 2.6 Wire up the Decode button to trigger decode -> layout -> render -> metadata

## 3. Styling

- [x] 3.1 Style the STUMP tab content to match the existing dashboard dark theme
- [x] 3.2 Add SVG node colors (green/blue/gray) and glow filter for proof path highlighting
- [x] 3.3 Style the legend, metadata panel, and detail panel consistently with existing dashboard sections

## 4. Update Debug Dashboard

- [x] 4.1 Add hex input auto-detection to `tools/debug-dashboard/templates/stump.html` so it also supports hex-encoded STUMP data

## 5. Verification

- [x] 5.1 Verify `go build ./...` succeeds (dashboard.html is embedded)
- [x] 5.2 Verify the Load Example button renders the test vector tree correctly
- [x] 5.3 Verify tab switching works between all four tabs (Endpoints, Register, Lookup, STUMP)
- [x] 5.4 Verify hex input decodes correctly in both main dashboard and debug dashboard
