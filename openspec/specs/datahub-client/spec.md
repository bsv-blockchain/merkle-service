## ADDED Requirements

### Requirement: Fetch subtree from DataHub
The DataHub client SHALL fetch binary subtree data from a DataHub endpoint and return a parsed `*subtree.Subtree` object. The client MUST construct the URL as `{dataHubURL}/subtree/{hash}` and deserialize the response using `go-subtree.NewSubtreeFromReader`.

#### Scenario: Successful subtree fetch
- **WHEN** the client fetches a subtree with a valid hash from a reachable DataHub
- **THEN** the client returns a parsed `*subtree.Subtree` with Nodes containing TxIDs

#### Scenario: DataHub unreachable
- **WHEN** the client fetches a subtree and the DataHub is unreachable
- **THEN** the client retries up to the configured maximum and returns an error if all retries fail

#### Scenario: Subtree not found
- **WHEN** the client fetches a subtree hash that does not exist on the DataHub
- **THEN** the client returns an error indicating the subtree was not found (HTTP 404)

### Requirement: Fetch block metadata from DataHub
The DataHub client SHALL fetch block metadata in JSON format from a DataHub endpoint and return structured block data including the list of subtree hashes. The client MUST construct the URL as `{dataHubURL}/block/{hash}/json`.

#### Scenario: Successful block metadata fetch
- **WHEN** the client fetches block metadata with a valid hash from a reachable DataHub
- **THEN** the client returns block metadata including height, header, and an ordered list of subtree hashes

#### Scenario: Block not found
- **WHEN** the client fetches a block hash that does not exist on the DataHub
- **THEN** the client returns an error indicating the block was not found

### Requirement: Configurable HTTP client
The DataHub client SHALL use configurable timeouts for HTTP requests. The client MUST accept a context for cancellation.

#### Scenario: Request timeout
- **WHEN** a DataHub request exceeds the configured timeout
- **THEN** the request is cancelled and an error is returned
