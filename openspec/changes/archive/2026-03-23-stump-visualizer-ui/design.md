## Context

The main API dashboard (`internal/api/dashboard.html`) is a single embedded HTML file served at `/` by the API server. It uses tab-based navigation with pure client-side JavaScript. The debug dashboard's STUMP visualizer (`tools/debug-dashboard/templates/stump.html`) contains a complete client-side BRC-0074 decoder, SVG tree renderer, and interactive proof path highlighting — all in ~500 lines of JavaScript with no backend dependencies.

## Goals / Non-Goals

**Goals:**
- Add a "STUMP Visualizer" tab to the main dashboard with the same functionality as the debug dashboard version.
- Keep it self-contained in the single HTML file (no external JS dependencies).
- Match the existing dark theme styling of the main dashboard.

**Non-Goals:**
- Refactoring the debug dashboard to share code with the main dashboard (they serve different audiences).
- Adding new API endpoints for server-side STUMP decoding.
- Changing the existing dashboard tabs or layout.

## Decisions

### 1. Copy JavaScript directly into the dashboard HTML

**Decision**: Port the STUMP decoder and SVG renderer JavaScript from `stump.html` into the main dashboard's `<script>` section, adapting it to work within the tab system.

**Rationale**: The main dashboard is a single self-contained HTML file with no build system. Inlining the JavaScript keeps the same pattern. The ~500 lines of STUMP code is manageable within the file.

### 2. Add as a fourth tab

**Decision**: Add "STUMP" as a new tab after the existing Endpoints/Register/Lookup tabs, using the same tab switching mechanism.

**Rationale**: Follows the established UI pattern. No structural changes to the dashboard.

### 3. Include the example test vector

**Decision**: Include the "Load Example" button with the same test vector from the debug dashboard, so users can immediately see what the visualizer does.

**Rationale**: Essential for discoverability — users seeing the tab for the first time need to understand what it does without having actual STUMP data handy.

## Risks / Trade-offs

- **[HTML file size increases]** → Acceptable trade-off. The file goes from ~300 lines to ~800 lines, still well within reason for a single embedded dashboard.
