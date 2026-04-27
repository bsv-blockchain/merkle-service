## 1. Template and Route Scaffolding

- [x] 1.1 Create `templates/stump.html` with basic `{{define "content"}}` block containing an input textarea, decode button, metadata panel placeholder, and empty SVG container, styled with existing dashboard CSS classes
- [x] 1.2 Add `stump.html` to the `pages` slice in `parseTemplates()` in `main.go` so it is parsed with the layout
- [x] 1.3 Add `GET /stump` route in `main.go` pointing to a new `handleStump` handler
- [x] 1.4 Add `handleStump` method to `Handlers` that renders `stump.html` with a pageData titled "STUMP Visualizer"
- [x] 1.5 Add "STUMP Visualizer" navigation link to `templates/layout.html` with active-state highlighting for Title "STUMP Visualizer"

## 2. BRC-0074 Binary Decoder (Client-Side JS)

- [x] 2.1 Implement `decodeBase64` helper that converts base64 string to Uint8Array, with error handling for invalid base64
- [x] 2.2 Implement `readCompactSize(data, offset)` function returning `{value, bytesRead}` matching Bitcoin CompactSize VarInt encoding (<253 = 1 byte, 0xFD = 2-byte LE, 0xFE = 4-byte LE, 0xFF = 8-byte LE)
- [x] 2.3 Implement `readHash(data, offset)` function that reads 32 bytes and returns the hex string (in internal byte order, not reversed)
- [x] 2.4 Implement main `decodeBUMP(uint8Array)` function that parses: block height (CompactSize), tree height (1 byte), then for each level: nLeaves (CompactSize), then for each leaf: offset (CompactSize), flag (1 byte), and hash (32 bytes if flag != 0x01)
- [x] 2.5 Return structured object: `{ blockHeight, treeHeight, levels: [{ levelIndex, leaves: [{ offset, flag, hash }] }] }` where flag 0x00=hash, 0x01=duplicate, 0x02=txid
- [x] 2.6 Add error handling in decodeBUMP for: unexpected end of data, invalid flag values, and return descriptive error messages with the byte offset where parsing failed

## 3. Decode Trigger and Error Display

- [x] 3.1 Wire the decode button click to read the textarea value, call `decodeBase64` then `decodeBUMP`, and store the result
- [x] 3.2 Display decode errors in a styled error div (using `.flash-error` class) below the input area
- [x] 3.3 Clear previous tree and metadata when decode is triggered (reset state)
- [x] 3.4 Handle empty input gracefully: show a prompt message, do not attempt decode

## 4. Metadata Display

- [x] 4.1 After successful decode, populate a metadata panel showing: block height, tree height, total node count
- [x] 4.2 Show per-level breakdown: level index, node count, and count by type (txid/hash/duplicate) using colored badges matching the node colors

## 5. SVG Tree Layout Engine

- [x] 5.1 Implement `computeTreeLayout(decodedBUMP)` that calculates x,y positions for each node: root at top (level = treeHeight), leaves at bottom (level 0), with horizontal spacing based on node offset within each level
- [x] 5.2 Calculate SVG dimensions: width based on max offset at leaf level × horizontal spacing, height based on treeHeight × vertical spacing, with padding
- [x] 5.3 For each node in the decoded BUMP, compute `{ x, y, level, offset, flag, hash }` layout entries
- [x] 5.4 Compute parent-child connecting lines: for each node at level L with offset O, its parent is at level L+1, offset floor(O/2); draw a line between their centers

## 6. SVG Tree Rendering

- [x] 6.1 Create the SVG element with computed dimensions and set it as content of the tree container div
- [x] 6.2 Render connecting lines as SVG `<line>` elements in subdued gray (#30363d) drawn before nodes so nodes appear on top
- [x] 6.3 Render each node as an SVG `<circle>` (or `<rect>` with rounded corners) at its computed position, filled with the appropriate color: green (#3fb950) for flag=0x02, blue (#58a6ff) for flag=0x00, gray (#484f58) for flag=0x01
- [x] 6.4 Add truncated hash labels (first 8 chars) as SVG `<text>` elements below each node
- [x] 6.5 Wrap the SVG in a horizontally scrollable container (`overflow-x: auto`) for wide trees

## 7. Node Interaction — Hover and Click

- [x] 7.1 Add mouseover/mouseout event handlers to each node SVG element that show/hide a tooltip with the full 64-char hash, level, offset, and flag type name
- [x] 7.2 Style the tooltip as a dark floating div positioned near the cursor, matching dashboard theme
- [x] 7.3 Add click handler to each node that populates a detail panel below the tree with: full hash, level, offset, flag type, and whether it is a txid (flag=0x02)
- [x] 7.4 Highlight the clicked node with a brighter border/outline to indicate selection

## 8. Proof Path Highlighting

- [x] 8.1 When a green (txid, flag=0x02) node is clicked, compute the proof path: starting from that leaf, at each level find the sibling node (offset XOR 1), and trace up to the root
- [x] 8.2 Visually highlight the proof path by adding a glow effect (e.g. brighter stroke, drop shadow filter) to the selected node and all nodes/lines along its path to the root
- [x] 8.3 Clicking the same txid node again or clicking a non-txid node clears the proof path highlight and returns to default coloring
- [x] 8.4 When switching between txid selections, clear the previous highlight before applying the new one

## 9. Page Styling and Polish

- [x] 9.1 Style the input area: full-width textarea with monospace font, placeholder text "Paste base64-encoded STUMP data here...", matching `.card` container style
- [x] 9.2 Style the metadata panel as a `.card` with a grid/flex layout for the stats and per-level breakdown
- [x] 9.3 Style the detail panel (shown on node click) as a `.card` below the tree
- [x] 9.4 Add level labels (0, 1, 2, ...) along the left side of the SVG tree for orientation
- [x] 9.5 Add a color legend showing what green, blue, and gray nodes represent

## 10. Testing

- [x] 10.1 Add a Go test in `tools/debug-dashboard/` that verifies the `/stump` route returns HTTP 200 and contains expected HTML elements (textarea, decode button)
- [x] 10.2 Add a test that verifies the STUMP visualizer page is included in the template parsing (stump.html appears in the pages list)
- [x] 10.3 Embed a known-good base64 STUMP test vector as a JS comment in `stump.html` (generated from the Go encoder in `internal/stump/builder_test.go`) for manual testing reference
