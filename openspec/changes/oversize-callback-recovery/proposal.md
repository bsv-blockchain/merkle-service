# Stop stranding blocks when a STUMP callback is rejected as too large

## Why

Live on dev-ovh-1, 2026-08-11, at 1000 TPS: **a block's entire transaction set
was permanently stranded short of `MINED`** because one oversized STUMP
callback was dropped instead of retried.

The chain:

1. `callback.DeliveryService` builds a STUMP body — fetch the blob via
   `stumpStore.Get(msg.StumpRef)`, **hex-encode it (2x inflation)**, JSON-marshal
   it inline (`internal/callback/delivery.go`, `deliverCallback`).
2. arcade's api-server wraps the inbound body in `http.MaxBytesReader` at
   `callback.max_body_bytes`, **default 16 MiB**
   (`arcade config.DefaultCallbackMaxBodyBytes`), and maps overflow to
   `413 Request Entity Too Large`.
3. At 1000 TPS one subtree's STUMP crossed that cap after hex inflation.
   arcade returned **413**.
4. merkle classified the 4xx as **permanent** (`isNonRetryable4xx` →
   `errPermanentDelivery`) and logged, with **zero retry attempts**:

   ```
   callback permanently failed, publishing to DLQ  type:"STUMP" reason:"permanent"
   ```

   → published to `callback-dlq`.
5. arcade's bump-builder was then stuck forever:

   ```
   BLOCK_PROCESSED is missing expected STUMPs — deferring finalization
   expected_stumps:1  received_stumps:0  missing_subtree_indices:[0]
   ```

   No BUMP was ever built, so **every transaction in that block never reached
   `MINED`**.
6. `/reprocess` could not recover it: the regenerated STUMP is byte-identical
   in size and 413s again. Deterministic, not transient.

The classification is the bug. Every other non-retryable 4xx
(400/401/403/404/405/410/415/422) means "this request is wrong and always will
be" — bad URL, bad token, unparseable payload. **413 means something
categorically different: "too big *for my current configuration*"** — an
operational condition curable without changing anything about the message, by
raising the receiver's cap. Collapsing the two threw away the only window in
which the block could heal itself.

Operators mitigated by raising arcade's cap to 128 MiB
(`teranode-argocd-deployments#211`), but that is a band-aid: the payload grows
with subtree size, so the cap will be hit again.

## What Changes

- **413 becomes its own delivery-failure class** (`oversizeDeliveryError`),
  distinct from both `permanentDeliveryError` and the generic retryable
  failure. It keeps the normal retry budget — 5 attempts on a 30 s linear
  backoff, ≈7.5 minutes of durable, Kafka-parked retrying — so an operator
  raising the receiver's cap heals the block with no further action. 413 is
  also removed from `isNonRetryable4xx` so no future caller can silently
  reintroduce "413 == permanent == DLQ == stranded block".
- **An oversize failure never trips the per-URL circuit breaker.** The
  breaker's premise is "this endpoint is dead"; a receiver rejecting one
  oversize STUMP is emphatically alive. arcade registers ONE callback URL for
  the whole deployment, so a block whose subtrees all produce oversize STUMPs
  would cross the default threshold of 20 within a single block and
  auto-disable that URL — killing SEEN and BLOCK_PROCESSED delivery for every
  transaction in flight. That is a strictly worse outage than the one being
  fixed.
- **New pre-flight cap `callback.maxBodyBytes`** (env
  `CALLBACK_MAX_BODY_BYTES`), a mirror of the RECEIVER's limit. When > 0 an
  over-limit body is never POSTed; delivery fails immediately with the block
  hash, subtree index and exact byte count. **Default 0 = disabled**, so the
  change is inert until configured: merkle cannot know a third-party
  receiver's cap, and a non-zero default would start refusing bodies the
  receiver would happily have accepted.
- **Observability.** Two new outcome labels on
  `merkle_callback_messages_total`: `oversize` (per rejected attempt) and
  `oversize_stranded` (retries exhausted → DLQ; the page-worthy "a block
  cannot be finalized" signal). An ERROR at detection and a `BLOCK STRANDED:`
  ERROR at give-up, both naming block hash, subtree index, body size, limit
  and status. Plus an unconditional WARN above a fixed 8 MiB (`bodyWarnBytes`,
  half arcade's original default) so payload growth is visible *before* it
  becomes an outage.

## Capabilities

### New Capabilities

_(none — no new service, topic, message field or wire format. The HTTP body,
the Kafka message schema, the dedup keys and the DLQ topic are all unchanged.)_

### Modified Capabilities

- `unified-callback-topic`: the delivery-failure classification gains a third
  class between "retryable" and "permanent". The DLQ requirement is qualified:
  an oversize body is not a permanent failure and must exhaust its retry
  budget first.

## Impact

- **`internal/callback/delivery.go`**: `oversizeDeliveryError` +
  `errOversizeDelivery` / `asOversizeDeliveryError`; `bodyWarnBytes`;
  size gate and 413 branch in `deliverCallback`; `logOversize`; oversize
  branch in `scheduleRetryOrDLQ`; retry tail extracted to `scheduleRetry` so
  both paths share one backoff ladder; 413 removed from `isNonRetryable4xx`;
  init log gains `maxBodyBytes` / `bodyWarnBytes`.
- **`internal/config/config.go` / `config.yaml`**:
  `CallbackConfig.MaxBodyBytes`, viper default 0, env binding
  `CALLBACK_MAX_BODY_BYTES`.
- **`internal/metrics/labels.go`**: `OutcomeOversize`,
  `OutcomeOversizeStranded`.
- **No changes** to the HTTP body, the encoding, the Kafka message format,
  dedup keys, the DLQ topic, or arcade.

## Deliberately NOT changed (and why)

- **Encoding stays hex.** base64 would cut the body from 2x to 1.33x of the
  blob, but arcade's `stump` field is `models.HexBytes` with a custom
  `UnmarshalJSON` that calls `hex.DecodeString` — base64 would be rejected as
  `400 invalid request body`, or (for a string that happens to be even-length
  all-hex) silently decode to the WRONG bytes. This is a coordinated
  arcade-side change, not a merkle-side one.
- **No gzip.** arcade has no gzip middleware and Go's `net/http` does not
  transparently decompress request bodies, so a `Content-Encoding: gzip` body
  reaches `json.Decoder` as raw gzip and yields an opaque `400`. Independently,
  a STUMP is a BRC-74 path made almost entirely of 32-byte hashes — near
  incompressible — so gzip would only recover the ~2x hex overhead, which
  base64 does more cheaply.
- **No claim-check delivery to arcade.** `StumpRef` is a merkle-INTERNAL
  claim check (Kafka message → `store.StumpStore` → blob store); it is never
  on the wire to arcade, and arcade has no `stumpRef`/`stump_url` field and no
  code path that fetches a STUMP from anywhere. Wiring it would mean a new
  authenticated `GET /api/stump/{ref}` on merkle plus a new fetch path,
  SSRF guard and failure semantics in arcade — a coordinated cross-repo
  change with no consumer today. Ship the recovery + observability fix first;
  see `design.md` for the follow-up sketch.
- **Oversize still reaches the DLQ once the retry budget is spent.** Parking
  it in Kafka indefinitely would head-of-line-block `kafka.callbackTopic`,
  which is pinned at 1 partition, halting callbacks for every OTHER block.
  Stranding one block is bad; stalling all of them is worse. The block stays
  recoverable regardless: arcade's completeness gate leaves `processed_at`
  NULL and its watchdog keeps re-driving `/reprocess`, which re-emits the
  STUMP — so once the cap is raised, the block heals.
