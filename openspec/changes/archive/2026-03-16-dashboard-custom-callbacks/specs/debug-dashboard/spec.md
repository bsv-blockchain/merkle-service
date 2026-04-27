## MODIFIED Requirements

### Requirement: Registration form callback URL
The registration form SHALL allow users to specify a custom callback URL when registering a transaction. The callback URL field SHALL default to the dashboard's own callback receiver URL but MUST be editable.

#### Scenario: Register with default callback URL
- **WHEN** user submits the registration form without modifying the callback URL field
- **THEN** the dashboard registers the txid with the merkle-service using the dashboard's own callback receiver URL

#### Scenario: Register with custom callback URL
- **WHEN** user edits the callback URL field to a custom URL and submits the form
- **THEN** the dashboard registers the txid with the merkle-service using the user-provided callback URL

#### Scenario: Custom callback URL validation
- **WHEN** user submits the registration form with an empty callback URL
- **THEN** the dashboard displays an error message indicating a callback URL is required

#### Scenario: Custom callback URL tracked locally
- **WHEN** user registers a txid with a custom callback URL
- **THEN** the txid tracker stores the custom callback URL (not the dashboard's default)

### Requirement: Multiple callback URLs per txid
The registrations page SHALL display all registered callback URLs for each tracked txid.

#### Scenario: Register same txid with different callback URLs
- **WHEN** user registers the same txid twice with different callback URLs
- **THEN** both callback URLs are stored and displayed on the registrations page for that txid
