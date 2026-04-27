## Why

The debug dashboard currently has no way to inspect STUMP (Subtree Unified Merkle Path) data. When debugging callback delivery issues or verifying that merkle proofs are correctly constructed, operators must manually decode base64 binary data. A visual tree representation makes it immediately clear whether a STUMP is valid and which transactions it contains proofs for.

## What Changes

- Add a new "STUMP Visualizer" page to the debug dashboard at `/stump`
- Accept base64-encoded STUMP binary data as input
- Decode the BRC-0074 BUMP binary format into structured data
- Render an interactive merkle tree visualization showing all path levels, leaves, sibling hashes, and proof paths
- Highlight registered txids (flag=0x02), sibling hashes (flag=0x00), and duplicate nodes (flag=0x01) with distinct colors
- Display metadata: block height, tree height, leaf counts per level
- Allow clicking/hovering on nodes to see full hash values
- Client-side only (JavaScript) — no server-side STUMP processing needed

## Capabilities

### New Capabilities
- `stump-visualizer`: Client-side STUMP decoder and merkle tree visualization for the debug dashboard. Covers base64 input, BRC-0074 binary parsing, tree layout rendering, and proof path highlighting.

### Modified Capabilities

(none)

## Impact

- New files in `tools/debug-dashboard/`: template (`templates/stump.html`), handler, route registration
- New navigation link in `templates/layout.html`
- JavaScript-heavy page (departure from current server-rendered-only pattern, but appropriate for interactive visualization)
- No new Go dependencies — STUMP decoding happens in client-side JavaScript
