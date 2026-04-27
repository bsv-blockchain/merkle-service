## ADDED Requirements

### Requirement: STUMP base64 input and decoding
The system SHALL accept base64-encoded BRC-0074 BUMP binary data as text input and decode it entirely in client-side JavaScript. The decoded result SHALL be a structured representation containing block height, tree height, and per-level leaf entries with offset, flags, and hash.

#### Scenario: Valid base64 STUMP is decoded
- **WHEN** a user pastes valid base64-encoded STUMP data into the input field and triggers decode
- **THEN** the system decodes the binary using BRC-0074 CompactSize VarInt format, extracts block height, tree height, and all level entries, and displays the structured result

#### Scenario: Invalid base64 input shows error
- **WHEN** a user pastes data that is not valid base64 or does not conform to BRC-0074 BUMP format
- **THEN** the system displays a clear error message describing what went wrong (e.g., "invalid base64", "unexpected end of data at level 2")

#### Scenario: Empty input is handled
- **WHEN** the input field is empty and the user triggers decode
- **THEN** the system displays a prompt to paste STUMP data and does not render a tree

### Requirement: Merkle tree SVG visualization
The system SHALL render the decoded STUMP as a top-down SVG merkle tree with the root at the top and leaves at the bottom. Each node in the STUMP SHALL be rendered as a positioned SVG element with connecting lines to its parent.

#### Scenario: Tree renders with correct structure
- **WHEN** a valid STUMP is decoded with tree height H
- **THEN** the system renders an SVG with H+1 rows (level 0 at bottom through level H at top), nodes positioned at their correct offset within each level, and lines connecting each node to its parent

#### Scenario: Tree is scrollable for wide trees
- **WHEN** a decoded STUMP has more leaves than fit in the visible viewport
- **THEN** the SVG container allows horizontal scrolling to view all nodes

### Requirement: Node type color coding
The system SHALL visually distinguish node types using colors: green (#3fb950) for registered txid nodes (flag=0x02), blue (#58a6ff) for sibling hash nodes (flag=0x00), and gray (#484f58) for duplicate nodes (flag=0x01).

#### Scenario: Registered txid nodes are green
- **WHEN** a STUMP is decoded containing nodes with flag=0x02 (txid)
- **THEN** those nodes are rendered in green (#3fb950) in the SVG tree

#### Scenario: Sibling hash nodes are blue
- **WHEN** a STUMP is decoded containing nodes with flag=0x00 (hash, not txid)
- **THEN** those nodes are rendered in blue (#58a6ff) in the SVG tree

#### Scenario: Duplicate nodes are gray
- **WHEN** a STUMP is decoded containing nodes with flag=0x01 (duplicate)
- **THEN** those nodes are rendered in gray (#484f58) in the SVG tree

### Requirement: Hash detail on interaction
The system SHALL display the full 64-character hex hash of a node when the user hovers over or clicks on it.

#### Scenario: Hover shows full hash
- **WHEN** a user hovers over a node in the SVG tree
- **THEN** a tooltip or detail panel displays the full 64-character hex hash and the node's level, offset, and flag type

#### Scenario: Click selects node and shows details
- **WHEN** a user clicks on a node in the SVG tree
- **THEN** a detail panel below or beside the tree displays the node's full hash, level, offset, flag type, and whether it is a txid

### Requirement: Metadata display
The system SHALL display decoded STUMP metadata including block height, tree height, and the count of nodes per level.

#### Scenario: Metadata panel shows after decode
- **WHEN** a valid STUMP is decoded
- **THEN** the system displays block height, tree height (number of levels), total node count, and a per-level breakdown of node counts by type (txid, hash, duplicate)

### Requirement: Dashboard integration
The STUMP visualizer SHALL be accessible as a page within the debug dashboard at the `/stump` route, with a navigation link added to the existing dashboard layout.

#### Scenario: Navigation link exists
- **WHEN** a user views any page in the debug dashboard
- **THEN** the navigation includes a link to "STUMP Visualizer" pointing to `/stump`

#### Scenario: Page renders at /stump
- **WHEN** a user navigates to `/stump` in the debug dashboard
- **THEN** the STUMP visualizer page loads with the input field, decode button, and empty tree area, styled consistently with the dashboard dark theme

### Requirement: Proof path highlighting
The system SHALL allow the user to select a registered txid and visually highlight the proof path from that leaf up through the tree, showing which nodes are used at each level to compute the root.

#### Scenario: Selecting a txid highlights its path
- **WHEN** a user clicks on a registered txid node (green) in the tree
- **THEN** the system highlights the proof path: the selected node and each sibling/parent node used at successive levels up to the root, using a brighter outline or glow effect

#### Scenario: Deselecting clears the highlight
- **WHEN** a user clicks on the same txid node again or clicks elsewhere
- **THEN** the proof path highlighting is removed and the tree returns to default coloring
