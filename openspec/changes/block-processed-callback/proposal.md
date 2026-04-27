## Why

Arcade instances need to know when all subtrees for a given block have been processed so they can begin constructing the full BUMP from all received STUMPs. Currently there is no signal from the merkle service that block processing is complete — Arcade has no way to distinguish "still processing" from "done, no more STUMPs coming."

## What Changes

- Add a new `BLOCK_PROCESSED` status type to the callback system
- After all subtrees for a block are processed, emit a `BLOCK_PROCESSED` callback to every registered callback URL in the database
- The callback payload includes the block hash that finished processing
- Arcade instances that had no STUMPs in that block simply ignore the message
- Introduce a callback URL registry (separate from per-txid registrations) that tracks all unique callback URLs for efficient broadcast

## Capabilities

### New Capabilities

- `block-processed-callback`: Emission of BLOCK_PROCESSED callbacks after all subtrees in a block are processed, broadcast to all registered callback URLs.

### Modified Capabilities

- `block-processing`: Block processor needs to emit BLOCK_PROCESSED after all subtrees complete. Requires access to the set of all registered callback URLs.

## Impact

- New `StatusBlockProcessed` constant in `internal/kafka/messages.go`
- New callback URL registry store (Aerospike set tracking unique callback URLs)
- Modified `internal/block/processor.go` to emit BLOCK_PROCESSED messages after subtree processing completes
- Modified `internal/callback/delivery.go` to handle BLOCK_PROCESSED payloads (no stump data, just block hash and status)
- Registration endpoint (`Add`) must also register the callback URL in the URL registry
- New dedup key scheme for BLOCK_PROCESSED (keyed on blockHash rather than txid)
