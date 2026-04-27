## Why

Some Teranode peers only expose `/api/v1/block/<hash>` (binary format) and do not expose `/api/v1/block/<hash>/json`. The block processor currently calls the JSON endpoint exclusively, making it incompatible with peers that lack it. Switching to the binary endpoint increases peer compatibility and aligns the block fetch path with the already-binary subtree fetch path.

## What Changes

- Replace `FetchBlockMetadata` in `internal/datahub/client.go` to call `/block/{hash}` instead of `/block/{hash}/json`
- Add `ParseBinaryBlockMetadata(data []byte) (*BlockMetadata, error)` to parse the binary response format into the existing `BlockMetadata` struct
- Update `client_test.go` to test binary block fetch and parsing
- Remove JSON unmarshal path for block metadata (no backward compatibility needed — JSON endpoint was never the intended long-term path)

## Capabilities

### New Capabilities
- none

### Modified Capabilities
- `datahub-client`: The block metadata fetch requirement changes from JSON (`/block/{hash}/json`) to binary (`/block/{hash}`), with a new binary parsing requirement

## Impact

- `internal/datahub/client.go` — `FetchBlockMetadata` and new `ParseBinaryBlockMetadata`
- `internal/datahub/client_test.go` — updated tests
- No changes to callers (`internal/block/processor.go`) — `BlockMetadata` struct and `FetchBlockMetadata` signature are unchanged
- No Kafka message format changes, no config changes
