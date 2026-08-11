# Design: oversize-callback-recovery

## The decision that matters: what class is a 413?

`scheduleRetryOrDLQ` had exactly two terminal shapes for a failed delivery:

| Class | Routing | Rationale |
|---|---|---|
| permanent (`errPermanentDelivery`) | DLQ immediately, 0 retries, trip breaker | retrying provably cannot help — blob expired, endpoint gone |
| everything else | retry ladder, DLQ on exhaustion, trip breaker | might be transient |

413 was filed under "permanent" only because `isNonRetryable4xx` swept up
every 4xx that is not 408/429. But the *reason* a 413 cannot be cured by
retrying is not a property of the message — it is a property of the
receiver's **current configuration**. Change one number on the receiver and
the identical request succeeds. That is the definition of an operational
condition, and operational conditions belong on the retry path, not in a
dead-letter queue.

The repo already has this distinction elsewhere: `OutcomeParkedDiskFull`
exists precisely because "the disk is full" is an operational condition that
retention-protected Kafka should ride out rather than DLQ. Oversize is the
same shape.

### Why not simply park it in Kafka forever?

Parking (return an error, never ack, let the offset stall) is the strongest
possible "never lose it" guarantee, and it is what `parked_disk_full` does on
the subtree topic. It is the wrong answer here because
`kafka.callbackTopic` **is pinned at 1 partition** — that is a hard
requirement so single-partition ordering keeps `BLOCK_PROCESSED` behind its
`STUMP`s (`docs/block-processed-completeness.md`). A stalled offset on a
1-partition topic halts callback delivery for every other block in the
system. Trading "one block stranded" for "all blocks stalled" is a strictly
worse outage.

The retry ladder is the right middle: the message is durably parked *in
Kafka* between attempts (republished, not held), so it costs the partition
one short visit per backoff cycle rather than the whole lane.

### Why is the normal retry budget enough?

It isn't, on its own — 5 attempts × 30 s linear ≈ 7.5 minutes. It is enough
because merkle is not the only recovery mechanism:

- arcade's completeness gate (`services/bump_builder/builder.go`,
  `resolveStumps`) deliberately does **not** stamp `processed_at` when
  expected STUMPs are missing, and returns `done` rather than an error —
  because "a Kafka requeue would just replay against the same gap".
- `processed_at IS NULL` is the durable recovery signal:
  `ListStaleBlockProcessingStatus` surfaces the block and the watchdog
  re-drives `POST /reprocess` against merkle, which re-emits the missing
  STUMPs and BLOCK_PROCESSED.

So the block stays recoverable indefinitely (up to the watchdog's
`MaxReprocessAttempts` / `MaxStaleAge` parking). merkle's retry budget buys
the *automatic* window; arcade's watchdog buys the *unbounded* one. What was
missing was neither of those — it was that merkle refused to try at all, and
said so in a log line indistinguishable from a bad auth token.

### Why exclude oversize from the circuit breaker?

`recordCallbackURLFailure` trips a per-URL breaker at
`callback.breakerThreshold` (default **20**) DLQ'd deliveries, after which
the URL is disabled and all fan-out to it stops until a fresh `/watch`.

arcade registers **one** callback URL for the entire deployment. A block
whose subtrees produce oversize STUMPs generates one DLQ per subtree per
subscriber — 20 is reachable inside a single block. Tripping the breaker
would then stop SEEN, STUMP and BLOCK_PROCESSED delivery for *every*
transaction in flight, not just the stranded block's. The breaker exists to
protect against a dead endpoint; a receiver that answers 413 with a JSON body
is demonstrably alive.

## The pre-flight cap, and why its default is 0

`callback.maxBodyBytes` is a mirror of the *receiver's* limit, not a limit of
our own. Two consequences:

- **Default 0 (disabled).** merkle delivers to arbitrary registered URLs and
  cannot know their caps. Any non-zero default would refuse bodies some
  receiver would have accepted — converting a rare, now-recoverable failure
  into a guaranteed one. Set it to match your receiver
  (`ARCADE_CALLBACK_MAX_BODY_BYTES`; arcade's own default is 16 MiB, dev-ovh-1
  runs 128 MiB since `teranode-argocd-deployments#211`).
- **It changes diagnosis, not outcome.** With it set, the failure is
  identical in routing to a 413 — same class, same retries, same metric — but
  it is diagnosed locally, before a doomed multi-MiB upload and a request
  timeout, and the log names the configured limit as well as the body size.

The unconditional 8 MiB WARN (`bodyWarnBytes`) is deliberately a constant, not
config. Its job is "tell me payloads are growing", which needs no tuning; the
*enforcing* knob is configurable because only the operator knows the real cap.
8 MiB is half arcade's original 16 MiB default, i.e. the point at which the
deployment is one subtree-growth step from the incident.

The warning is emitted per *delivery*, not per built body — it sits after the
`bodyCache` lookup, so a subtree fanned out to N subscribers logs N lines.
That is deliberate: the body is memoized but the risk is per-request, and each
of those N deliveries can independently be the one that 413s. Bodies above
8 MiB are exceptional by construction (a normal STUMP body is ~545 KB), so the
volume is bounded by how close the deployment already is to an outage — which
is exactly when an operator wants to be told loudly.

## Follow-up: the claim-check path (needs a coordinated arcade change)

Investigated and explicitly deferred. Findings:

- merkle already claim-checks STUMPs **internally**: `CallbackTopicMessage`
  carries `StumpRef` and the delivery service resolves it through
  `store.StumpStore` (blob store, `blobStore.url`) rather than putting the
  blob on the Kafka topic. That ref never leaves merkle.
- arcade has **no** counterpart: no `stumpRef` / `stump_url` / `ref` field on
  `models.CallbackMessage`, and no code anywhere that fetches a STUMP. Its
  only STUMP ingress is the inline hex `stump` field, which
  `handleStump` writes synchronously to its own store.
- The `CALLBACK_STUMP_CACHE_MODE` / `CALLBACK_STUMP_CACHE_LRU_SIZE` /
  `CALLBACK_STUMP_CACHE_TTL_SEC` env vars present in `deploy/k8s/*.yaml` and
  `deploy/k8s/README.md` are **dead configuration** — nothing under
  `internal/` or `cmd/` reads them. They are residue from the
  `k8s-distributed-stump-cache` change; the shipped claim check is the blob
  store, not an Aerospike stump cache. Setting
  `CALLBACK_STUMP_CACHE_MODE=aerospike` has no effect on anything today.

A real claim-check delivery would need, in order:

1. merkle: an authenticated `GET /api/stump/{ref}` on the API server, bounded
   and rate-limited, serving `store.StumpStore.Get`. Must respect the STUMP
   blob's DAH so a ref cannot outlive its blob.
2. merkle: an additive `stumpUrl` field on the callback body, emitted only
   when a new `callback.stumpDeliveryMode` is set to `ref`.
3. arcade: `models.CallbackMessage.StumpURL`, a fetch path in `handleStump`
   with its own timeout/retry/SSRF guard, and a decision about what a fetch
   failure means for the 200-means-durable contract (today a 200 on the
   callback IS the durability guarantee that the bump-builder relies on).
4. Rollout: merkle must keep sending inline until every arcade is upgraded,
   so the field is negotiated, not switched.

That is a multi-repo change with real failure-mode surface. It is the right
long-term answer to unbounded payload growth; it is not the right thing to
bundle with an incident fix whose whole point is that the current failure
mode is unrecoverable.

## Alternatives rejected

| Option | Why not |
|---|---|
| base64 instead of hex (2x → 1.33x) | arcade decodes `stump` with `hex.DecodeString` via `models.HexBytes.UnmarshalJSON`. base64 gives `400`, or silently wrong bytes for an even-length all-hex string. Breaking wire change. |
| gzip the body | arcade has no gzip middleware; Go does not auto-decompress request bodies → opaque `400`. And a BRC-74 path of 32-byte hashes is near incompressible; gzip would only undo the hex overhead. |
| Give oversize an unbounded retry budget | Each not-yet-due retry costs a 2 s in-process wait on a 1-partition topic (`futureRetryWaitCap`). An immortal message would consume a large fraction of the single delivery lane forever. |
| Split a STUMP across multiple callbacks | Changes the wire contract and arcade's `(blockHash, subtreeIndex)` STUMP key, and reintroduces a partial-delivery completeness problem one layer down. |
| Just raise arcade's cap | Already done as the interim mitigation (`teranode-argocd-deployments#211`). It does not make the failure recoverable or observable when the next threshold is crossed. |
