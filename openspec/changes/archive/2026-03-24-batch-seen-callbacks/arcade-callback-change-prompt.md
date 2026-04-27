# Arcade Callback Contract Change: Batched SEEN Callbacks

## Prompt for Arcade Claude Code Instance

The Merkle Service is changing how `SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` callbacks are delivered. Instead of one HTTP POST per transaction, the service now sends **one HTTP POST per callbackURL per subtree**, with all matched txids in a single request.

### What changed

The callback HTTP POST body now includes a `txids` JSON array field for `SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` callback types. The scalar `txid` field is no longer populated for these types.

### Before (one request per txid)

```
POST /callback
{"type":"SEEN_ON_NETWORK","txid":"abc123"}

POST /callback
{"type":"SEEN_ON_NETWORK","txid":"def456"}

POST /callback
{"type":"SEEN_ON_NETWORK","txid":"ghi789"}
```

### After (one request per callbackURL per subtree)

```
POST /callback
{"type":"SEEN_ON_NETWORK","txids":["abc123","def456","ghi789"]}
```

### Same change for SEEN_MULTIPLE_NODES

```
POST /callback
{"type":"SEEN_MULTIPLE_NODES","txids":["abc123","ghi789"]}
```

(Only txids that reached the seen-count threshold appear in the `txids` array.)

### Unchanged callback types

- `STUMP` — unchanged, already one-per-subtree with inline stump data
- `BLOCK_PROCESSED` — unchanged, one per block per callbackURL

### What Arcade needs to change

1. **Callback handler**: When `type` is `SEEN_ON_NETWORK` or `SEEN_MULTIPLE_NODES`, read the `txids` JSON array field (string array) instead of the scalar `txid` field.

2. **Processing logic**: Iterate over the `txids` array and apply the same per-txid logic that currently runs for the scalar `txid` field (e.g., marking transactions as seen, updating status).

3. **Backward compatibility**: During the rollout window, the handler should accept both formats:
   - If `txids` array is present and non-empty → use it
   - Else if scalar `txid` is present → treat as `["txid"]` (single-element array)

   This ensures no disruption during rolling deployments.

### TypeScript type change (example)

```typescript
interface CallbackMessage {
  type: 'SEEN_ON_NETWORK' | 'SEEN_MULTIPLE_NODES' | 'STUMP' | 'BLOCK_PROCESSED';
  txid?: string;          // deprecated for SEEN types, still used by other types
  txids?: string[];       // NEW: array of txids for batched SEEN callbacks
  blockHash?: string;
  subtreeIndex?: number;
  stump?: string;
}
```

### Go type change (example)

```go
type CallbackMessage struct {
    Type         string   `json:"type"`
    TxID         string   `json:"txid,omitempty"`
    TxIDs        []string `json:"txids,omitempty"`    // NEW
    BlockHash    string   `json:"blockHash,omitempty"`
    SubtreeIndex int      `json:"subtreeIndex,omitempty"`
    Stump        string   `json:"stump,omitempty"`
}
```

### Implementation instructions

Find the callback handler that processes incoming Merkle Service webhook POST requests. Update it to:

1. Parse the `txids` field from the JSON body as a `string[]` / `[]string`
2. When `type` is `SEEN_ON_NETWORK` or `SEEN_MULTIPLE_NODES` and `txids` is present, loop over the array and process each txid
3. Fall back to the scalar `txid` field if `txids` is absent (backward compatibility)
4. Add tests covering: batched payload with multiple txids, single-txid batch, empty txids with scalar txid fallback
