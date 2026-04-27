## Context

The block processor fetches block metadata (height + list of subtree hashes) from a Teranode DataHub endpoint. The current implementation calls `/block/{hash}/json`, which is not exposed by all Teranode peers. The binary endpoint `/block/{hash}` is universally available and already the pattern used for subtree data. The `FetchBlockMetadata` function in `internal/datahub/client.go` is the only call site.

## Goals / Non-Goals

**Goals:**
- Replace the JSON block endpoint with the binary block endpoint
- Add a `ParseBinaryBlockMetadata` function to decode the binary response
- Keep `BlockMetadata` struct and `FetchBlockMetadata` signature unchanged (no caller changes)

**Non-Goals:**
- Changing how subtree data is fetched (already binary)
- Supporting both endpoints simultaneously (fallback logic adds complexity for no benefit)
- Changing the `BlockMetadata` fields (height and subtrees are sufficient; header/tx count are optional)

## Decisions

### Binary wire format

The Teranode DataHub binary block response encodes:
- 4 bytes: block height as `uint32` little-endian
- 4 bytes: subtree count as `uint32` little-endian
- N × 32 bytes: subtree hashes in raw bytes (same byte order as the subtree endpoint)

This is consistent with the subtree binary format (concatenated 32-byte hashes) and the existing `ParseRawNodes` pattern. The `TransactionCount` field in `BlockMetadata` is not encoded in the binary response and will default to 0; callers do not use it after the block processor was refactored to use the subtree counter.

**Alternative considered**: Keep JSON with a fallback to binary on 404. Rejected because it adds retry-on-fallback latency on every block fetch from binary-only peers, complicates testing, and the JSON endpoint provides no data the binary endpoint does not.

### No change to BlockMetadata struct

`BlockHeader` and `TransactionCount` fields become zero-valued when using the binary endpoint. The block processor only reads `meta.Subtrees` and `meta.Height`, so this is safe. If callers ever need header data they would need to parse the raw block header separately.

## Risks / Trade-offs

- [Binary format assumption] If Teranode's actual binary block format differs from the documented layout above → `ParseBinaryBlockMetadata` will return corrupt data. **Mitigation**: Verify against a live node or Teranode source before shipping; add a length validation guard (`len(data) >= 8` and `(len(data)-8) % 32 == 0`).
- [TransactionCount lost] Any future code reading `meta.TransactionCount` will get 0. **Mitigation**: None needed until a caller requires it — document the field as unused.

## Open Questions

- **Binary format confirmation**: The 4+4+N×32 layout above should be verified against the Teranode DataHub source or API documentation before the task is implemented.
