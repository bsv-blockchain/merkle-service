## MODIFIED Requirements

### Requirement: Fetch block metadata from DataHub
The DataHub client SHALL fetch block metadata in binary format from a DataHub endpoint and return structured block data including the block height and the ordered list of subtree hashes. The client MUST construct the URL as `{dataHubURL}/block/{hash}` and deserialize the response using `ParseBinaryBlockMetadata`.

#### Scenario: Successful block metadata fetch
- **WHEN** the client fetches block metadata with a valid hash from a reachable DataHub
- **THEN** the client returns block metadata including height and an ordered list of subtree hashes

#### Scenario: Block not found
- **WHEN** the client fetches a block hash that does not exist on the DataHub
- **THEN** the client returns an error indicating the block was not found (HTTP 404)

#### Scenario: Binary response too short
- **WHEN** the DataHub returns a binary response shorter than 8 bytes
- **THEN** the client returns a parse error

#### Scenario: Binary response with invalid subtree data length
- **WHEN** the DataHub returns a binary response where the subtree data portion is not a multiple of 32 bytes
- **THEN** the client returns a parse error

## ADDED Requirements

### Requirement: Parse binary block metadata
The DataHub client SHALL provide a `ParseBinaryBlockMetadata(data []byte) (*BlockMetadata, error)` function that decodes the Teranode binary block format: 4-byte uint32 LE block height, 4-byte uint32 LE subtree count, followed by N×32-byte subtree hashes.

#### Scenario: Valid binary block data
- **WHEN** `ParseBinaryBlockMetadata` receives a valid binary block payload with N subtrees
- **THEN** it returns a `*BlockMetadata` with the correct height and a slice of N hex-encoded subtree hash strings

#### Scenario: Empty subtree list
- **WHEN** `ParseBinaryBlockMetadata` receives a payload with subtree count = 0
- **THEN** it returns a `*BlockMetadata` with an empty `Subtrees` slice

#### Scenario: Data length mismatch
- **WHEN** `ParseBinaryBlockMetadata` receives data where the actual subtree bytes do not match the declared subtree count
- **THEN** it returns an error
