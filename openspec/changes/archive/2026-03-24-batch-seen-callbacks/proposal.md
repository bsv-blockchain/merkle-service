## Why

SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES callbacks currently fire one HTTP request per registered txid, causing N identical callback calls when N txids in a subtree belong to the same Arcade instance. At scale (subtrees with thousands of transactions), this creates unnecessary HTTP overhead. Arcade already processes callback payloads in bulk — it just needs the txid list. Batching to one callback per (callbackURL, subtree) matches the per-subtree STUMP callback pattern we already use.

## What Changes

- **Batch SEEN_ON_NETWORK**: Instead of publishing one `CallbackTopicMessage` per txid, the subtree processor groups registered txids by callbackURL and publishes one message per callbackURL containing all matched txids as a `TxIDs []string` array.
- **Batch SEEN_MULTIPLE_NODES**: After incrementing seen counters for all registered txids, group the ones that reached threshold by callbackURL and publish one batched message per callbackURL.
- **CallbackTopicMessage schema**: Add `TxIDs []string` field alongside existing `TxID string` (which remains for backward compatibility / single-txid cases).
- **Delivery payload**: Update `callbackPayload` to include `txids []string` array field. The HTTP POST body sent to Arcade carries the full list.
- **BREAKING**: Arcade callback endpoints must accept a `txids` array field in addition to (or instead of) the scalar `txid` field for SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES callback types.

## Capabilities

### New Capabilities
- `batched-seen-callbacks`: Batching SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES callbacks to one HTTP request per (callbackURL, subtree) with a txid array payload

### Modified Capabilities
- `subtree-processing`: The subtree processor changes from per-txid callback emission to per-callbackURL batched emission for SEEN types

## Impact

- `internal/subtree/processor.go` — restructure `emitSeenCallbacks` to batch by callbackURL
- `internal/kafka/messages.go` — add `TxIDs` field to `CallbackTopicMessage`
- `internal/callback/delivery.go` — update `callbackPayload` and `deliverCallback` to handle txid arrays
- Arcade callback handler — must accept `txids` array (breaking contract change, prompt provided)
