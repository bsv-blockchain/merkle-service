## MODIFIED Requirements

### Requirement: Dashboard integration
The STUMP visualizer SHALL be accessible as a page within the debug dashboard at the `/stump` route, AND as a tab within the main API dashboard served at `/`. Both instances SHALL provide identical visualization functionality.

#### Scenario: Navigation link exists in debug dashboard
- **WHEN** a user views any page in the debug dashboard
- **THEN** the navigation includes a link to "STUMP Visualizer" pointing to `/stump`

#### Scenario: Page renders at /stump in debug dashboard
- **WHEN** a user navigates to `/stump` in the debug dashboard
- **THEN** the STUMP visualizer page loads with the input field, decode button, and empty tree area, styled consistently with the dashboard dark theme

#### Scenario: Tab exists in main API dashboard
- **WHEN** a user views the main API dashboard at `/`
- **THEN** the tab navigation includes a "STUMP" tab alongside Endpoints, Register, and Lookup

#### Scenario: STUMP tab renders visualizer in main dashboard
- **WHEN** a user clicks the "STUMP" tab in the main API dashboard
- **THEN** the STUMP visualizer loads with an input field, Decode button, Load Example button, and empty tree area, providing the same decode, render, and interaction functionality as the debug dashboard version

## ADDED Requirements

### Requirement: Hexadecimal input support
The STUMP visualizer SHALL accept both base64-encoded and hexadecimal-encoded STUMP data. The format SHALL be auto-detected from the input content.

#### Scenario: Hex input is decoded
- **WHEN** a user pastes a hexadecimal string (containing only characters 0-9, a-f, A-F)
- **THEN** the system decodes it as hex and visualizes the STUMP tree

#### Scenario: Base64 input is decoded
- **WHEN** a user pastes a base64 string (containing characters A-Z, a-z, 0-9, +, /, =)
- **THEN** the system decodes it as base64 and visualizes the STUMP tree

#### Scenario: Auto-detection distinguishes formats
- **WHEN** the input contains characters outside the hex range (such as +, /, =, or g-z uppercase)
- **THEN** the system treats the input as base64
