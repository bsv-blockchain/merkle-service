## Why

The merkle-service receives P2P announcements for subtrees and blocks but cannot process them end-to-end. When we switched to `go-teranode-p2p-client`, we discovered that P2P messages are announcements containing a hash and a DataHub URL — not the full subtree/block data. The subtree processor, block processor, and block subtree processor all have TODO placeholders where DataHub fetching and processing logic needs to be implemented.

Without this change, the service cannot:
- Fetch and store subtree data when announced
- Check which announced transactions are registered and issue SEEN_ON_NETWORK callbacks
- Process block announcements to build STUMPs and deliver MINED callbacks
- Track cross-subtree transaction appearances for SEEN_MULTIPLE_NODES callbacks

## What Changes

- **DataHub HTTP client**: New internal package to fetch subtree and block data from DataHub URLs (GET `/api/v1/subtree/{hash}` for binary subtree, GET `/api/v1/block/{hash}/json` for block metadata with subtree list)
- **Subtree processor**: Fetch full subtree data from DataHub, parse with `go-subtree`, store binary data to disk, check registrations (with cache), increment seen counters, emit SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES callbacks via Kafka stumps topic
- **Block processor**: Fetch block metadata from DataHub to get the list of subtree hashes, spawn block subtree processors for each subtree
- **Block subtree processor**: For each subtree in a block, fetch subtree data (or retrieve from local store), find registered txids, build STUMPs using `stump.Build()`, emit MINED callbacks with encoded STUMP data, update registration TTLs

## Capabilities

### New Capabilities
- `datahub-client`: HTTP client for fetching subtree and block data from Teranode DataHub endpoints

### Modified Capabilities
- `subtree-processing`: Subtree processor gains DataHub fetching, registration checking, seen-counter tracking, and SEEN_ON_NETWORK/SEEN_MULTIPLE_NODES callback emission
- `block-processing`: Block processor gains DataHub fetching for block metadata and subtree-level STUMP building with MINED callback emission

## Impact

- `internal/datahub/` — New package with DataHub HTTP client
- `internal/subtree/processor.go` — Full implementation replacing TODO
- `internal/block/processor.go` — Full implementation replacing TODO
- `internal/block/subtree_processor.go` — Full re-implementation
- `go.mod` — May need `go-subtree` dependency if not already present
- Existing stores (registration, seen counter, subtree/blob) and STUMP builder are already implemented — this change wires them together
