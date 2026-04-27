## Context

The merkle-service receives subtree and block announcements via P2P (published to Kafka). Each announcement contains a hash and a DataHub URL. The service needs to fetch full data from the DataHub, process it, and emit callbacks. All downstream infrastructure exists: registration store (Aerospike), seen counter store, STUMP builder, callback delivery service, subtree blob store. The missing piece is the DataHub HTTP client and the processing logic that connects announcements to callbacks.

Teranode's DataHub exposes:
- `GET /api/v1/subtree/{hash}` — binary subtree data (deserializable via `go-subtree.NewSubtreeFromReader`)
- `GET /api/v1/block/{hash}/json` — block metadata including `subtrees` array of hashes
- `GET /api/v1/block/{hash}/subtrees/json` — subtree metadata for a block

## Goals / Non-Goals

**Goals:**
- Fetch subtree data from DataHub and store to local blob store on announcement
- Check registered txids against subtree contents and emit SEEN_ON_NETWORK callbacks
- Track cross-subtree appearances and emit SEEN_MULTIPLE_NODES when threshold reached
- Fetch block metadata from DataHub to get subtree list
- For each subtree in a block, build STUMPs for registered txids and emit MINED callbacks
- Update registration TTLs after block processing

**Non-Goals:**
- Fetching full transaction data (SubtreeData) — we only need the subtree merkle structure (TxIDs as leaf hashes)
- Handling DataHub authentication or signatures
- Block reorganization / orphan handling (future work)
- Fetching subtree_data (raw tx bytes) — not needed for merkle proof construction

## Decisions

### 1. DataHub client as internal package (`internal/datahub/`)

Create a thin HTTP client that wraps `net/http` with configurable timeout and retry. Two methods: `FetchSubtree(ctx, url, hash)` returns `*subtree.Subtree`, `FetchBlock(ctx, url, hash)` returns block metadata with subtree hashes.

**Rationale:** Keeps DataHub interaction isolated and testable. The client accepts a base URL from the announcement message's DataHubURL field.

**Alternative:** Inline HTTP calls in processors. Rejected — harder to test, duplicated retry logic.

### 2. Binary format for subtree fetching

Fetch subtrees in binary format (`/api/v1/subtree/{hash}`) and deserialize with `go-subtree.NewSubtreeFromReader`. This gives us TxIDs (as `SubtreeNode.Hash`) and the merkle tree structure needed for STUMP building.

**Rationale:** Binary is more compact than JSON. The `go-subtree` package provides ready-made deserialization. We also store the raw binary to the blob store for later block processing.

**Alternative:** JSON format with pagination. Rejected — requires multiple requests for large subtrees, and we need the full tree for merkle proofs.

### 3. Store-then-process pattern for subtrees

On subtree announcement: fetch binary → store to blob store → parse → check registrations → emit callbacks. On block processing: retrieve from blob store (or re-fetch if missing) → build STUMPs.

**Rationale:** Avoids re-fetching during block processing. The blob store already supports delete-at-height pruning. Aligns with the existing `storageMode: realtime` config.

### 4. Block processing spawns worker pool for subtrees

The block processor fetches block metadata (JSON) to get the subtree hash list, then processes each subtree using a bounded worker pool (`block.workerPoolSize`, default 16). Each worker retrieves the subtree from the blob store (or fetches from DataHub as fallback), finds registered txids, builds a STUMP, and emits MINED callbacks.

**Rationale:** Matches the existing config design. Bounded concurrency prevents overwhelming Aerospike or DataHub during large blocks.

### 5. Build merkle tree with `BuildMerkleTreeStoreFromBytes` for STUMP construction

Use `go-subtree`'s `BuildMerkleTreeStoreFromBytes(subtree.Nodes)` to get the full merkle tree, then pass to `stump.Build(blockHeight, merkleTree, registeredIndices)`.

**Rationale:** The STUMP builder expects a flat merkle tree array. This function builds it from the subtree's leaf nodes.

### 6. Callback grouping per the existing pattern

For SEEN_ON_NETWORK: one StumpsMessage per (txid, callbackURL) pair.
For MINED: group by callbackURL using `stump.GroupByCallback`, build one STUMP per callback URL containing all that URL's registered txids, send one StumpsMessage per callbackURL.

**Rationale:** Matches the existing StumpsMessage structure and callback delivery service expectations.

## Risks / Trade-offs

- [DataHub unavailable during announcement] → Retry with backoff in the DataHub client. If permanently unreachable, log error and skip (message will not be reprocessed unless Kafka offset is not committed). Future: consider DLQ for failed fetches.
- [Subtree not in blob store during block processing] → Fallback to fetching from DataHub using the block announcement's DataHubURL.
- [Large subtrees with thousands of txids] → BatchGet for registration lookups (already implemented). Registration cache reduces Aerospike load.
- [go-subtree dependency version] → Pin to v1.0.1 (matches teranode's usage).
