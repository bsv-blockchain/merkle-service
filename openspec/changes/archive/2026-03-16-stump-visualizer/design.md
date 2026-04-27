## Context

The debug dashboard (`tools/debug-dashboard/`) is a Go server-rendered HTML application using embedded templates. It currently has pages for registration management and callback inspection. There is no tooling for inspecting STUMP/BUMP binary data — the BRC-0074 merkle proof format used by the merkle service.

The BSV showcase at `bitcoin-sv.github.io/showcase-merkle-paths/` provides a reference for how merkle path visualizations should look: a tree diagram with colored nodes showing the proof path from leaf to root, with hash details available on interaction.

## Goals / Non-Goals

**Goals:**
- Decode base64-encoded BRC-0074 BUMP binary into structured JSON in the browser
- Render a visual merkle tree showing all levels and nodes from the STUMP
- Distinguish node types visually: registered txids (green), sibling hashes (blue), duplicate nodes (gray)
- Show proof paths: trace from each txid leaf up through the tree to the root
- Display metadata: block height, tree height, node counts
- Show full 64-char hex hashes on hover/click

**Non-Goals:**
- Server-side STUMP decoding (keep it client-side JS for simplicity)
- Proof verification (computing and comparing the root) — visualization only
- Supporting BUMP merging or editing
- Mobile-optimized layout

## Decisions

### 1. Client-side JavaScript for decoding and rendering

**Decision:** All STUMP parsing and tree rendering happens in client-side JavaScript embedded in the template.

**Rationale:** The debug dashboard is a developer tool. Keeping decoding in JS means no new Go dependencies, no round-trip to the server for each decode, and instant feedback as the user pastes data. The existing dashboard is server-rendered, but this page is inherently interactive.

**Alternative considered:** Server-side Go decoder with JSON API — rejected because it adds unnecessary complexity and the STUMP decoder is straightforward to implement in JS.

### 2. SVG-based tree rendering

**Decision:** Render the merkle tree as an SVG element with positioned nodes and connecting lines.

**Rationale:** SVG provides precise layout control, scales cleanly, supports hover/click events natively, and doesn't require a canvas drawing library. The tree structure maps naturally to positioned SVG elements. For typical subtrees (up to ~16 leaves / 4-5 levels), SVG performance is a non-issue.

**Alternative considered:** HTML/CSS flexbox tree — harder to draw connecting lines between parent and child nodes. Canvas — overkill for this scale and loses DOM event handling.

### 3. Top-down tree layout (root at top)

**Decision:** Draw the tree with the root at the top and leaves at the bottom, consistent with standard CS tree visualizations and the BSV showcase reference.

**Rationale:** Most developers expect top-down tree layouts. The BUMP format stores levels bottom-up (level 0 = leaves), but the visual display should be top-down.

### 4. Color scheme matching dashboard dark theme

**Decision:** Use the existing dashboard color palette:
- Green (`#3fb950`) for registered txid nodes (flag=0x02)
- Blue (`#58a6ff`) for sibling hash nodes (flag=0x00)
- Gray (`#484f58`) for duplicate nodes (flag=0x01)
- Dimmed (`#30363d`) for "implied" nodes not included in the STUMP
- Connecting lines in subdued gray

### 5. Single-page template with inline JS

**Decision:** One new template file (`templates/stump.html`) with embedded JavaScript — no external JS files or build tools.

**Rationale:** Matches the existing dashboard pattern of self-contained templates. The JS is ~200-300 lines for decoding + rendering, small enough to inline. Avoids introducing a build step or static file serving.

## Risks / Trade-offs

- **Large trees:** Subtrees with thousands of leaves would produce very wide SVGs. → Mitigation: Add horizontal scroll and limit display to ~64 leaves with a warning for larger trees. In practice, STUMPs only contain a sparse subset of the tree (registered txids + proof paths), so even large subtrees have compact STUMPs.

- **JS decoding fidelity:** Client-side CompactSize/VarInt parsing must exactly match the Go encoder. → Mitigation: Use the same BRC-0074 spec, include test vectors in the template's comments that match known-good Go encoder output.

- **No verification:** The visualizer shows the tree but doesn't verify the proof computes the correct root. → Acceptable for a debug tool; verification is a future enhancement.
