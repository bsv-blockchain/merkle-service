# Design: peer-health attribution

## Context

`PeerHealth` (internal/datahub/peerhealth.go) is an in-memory, host-keyed
consecutive-failure breaker shared by three call sites: the subtree-fetcher's
`IsHealthy` ack-and-drop gate, block-processor failover ordering, and the /reprocess
probe loop. Failures were recorded in one place — the DataHub client's
`recordPeerOutcome` — with a blanket rule: any non-nil error counts against the peer.

That rule is wrong in exactly two ways, both observed on dev-ovh-1 (15 Jul 2026):

- **Caller cancellations.** The subtree consumer cancels in-flight handler contexts on
  shutdown, rebalance, and partition loss. Each aborted fetch surfaced as
  `context.Canceled` → `RecordFailure`. Three per host opens the breaker; a rollout
  produces far more than three.
- **Stale-announcement 404s.** Teranode's asset cache serves subtree data for a bounded
  window (~2h). Under consumer lag, announcements age past that window while parked in
  Kafka; the eventual fetch 404s on an honest peer. Deep backlogs produce an unbroken
  run of these, so the breaker re-opened after every 5m cooldown — with a single peer,
  the pre-store path stayed dead for hours.

The breaker also had zero observability: no trip log, no metrics — the incident was
only diagnosable from DEBUG-level "skipping subtree fetch" lines.

## Goals / Non-Goals

**Goals:**

- An aborted fetch whose caller ctx is dead records nothing (success or failure).
- A client-side HTTP timeout with a live caller ctx still counts — peer slowness is
  peer-attributable. Distinguish via `ctx.Err()`, never error-string matching.
- A 404 on an announcement older than a configurable grace is attributed to our lag,
  not the peer; the message still routes to the DLQ unchanged.
- Every breaker opening is observable: WARN log at the recording site + a transitions
  counter + a per-peer health gauge.
- Wire compatibility: in-flight `SubtreeMessage`s without the new stamp behave exactly
  as before (404 counted).

**Non-Goals:**

- Changing the `IsHealthy` gate's ack-and-drop semantics, 404 → permanent-DLQ routing,
  ENOSPC parking, or the threshold/cooldown defaults.
- Cross-pod breaker state (stays in-memory per process).
- Age-aware attribution for the block-metadata path: block fetches fail over across
  peers and re-drive via bounded retries, so a lag-aged block 404 self-heals; only the
  subtree path (announced URL is authoritative, no failover) had the poisoning loop.

## Decisions

### 1. Cancellation neutrality lives at the recording chokepoint, keyed on ctx.Err()

`recordPeerOutcome` gains the ctx and returns early when `ctx.Err() != nil` — including
for successes: a success observed while the ctx is dead must not reset a genuinely
failing peer's counter either. Matching on `errors.Is(err, context.Canceled)` instead
was rejected: the client's own `http.Client.Timeout` also surfaces as a
context-cancellation-shaped error on the request's derived context, and that case (peer
too slow while the caller still wants the data) must keep counting. `ctx.Err()` on the
caller's context is the only signal that separates "we gave up" from "the peer failed".

### 2. The subtree processor owns recording for its fetch, via a per-call opt-out

Only the processor knows the announcement's age, so the client can't classify 404s
itself. `FetchSubtreeRaw` gains a variadic `FetchOption` with `WithoutPeerRecording()`
(variadic keeps every existing call site source-compatible), and the processor records
after classifying. The alternative — threading the announcement timestamp into the
client — was rejected: it leaks message-bus concepts into the HTTP client and would
have forced every other `FetchSubtreeRaw` caller (block-processor probe paths) to
supply a meaningless age.

Classification order: ctx dead → nothing; success → RecordSuccess; `ErrNotFound` AND
age strictly > grace → nothing (log at DEBUG); everything else → RecordFailure. A 404
with a zero/missing stamp counts — treating unknown age as stale would blind the
breaker to genuinely lying peers during the rollout window, the conservative direction
is to keep pre-change behavior.

### 3. Announcement age travels in the message, stamped at P2P publish time

`SubtreeMessage.AnnouncedAtUnixMs` (`omitempty`) is set by the P2P client when it maps
the teranode announcement to Kafka. Retries re-encode the same struct, so the original
stamp survives the retry pipeline — age reflects time since announcement, not time
since last attempt, which is what the asset-cache-retention argument needs. Kafka
record timestamps were rejected as the age source: the retry republish resets them,
and the consumer API surfaces them less uniformly than a message field.

### 4. Trip signal from RecordFailure; metrics inside PeerHealth

`RecordFailure` returns true exactly once per healthy→unhealthy transition ("healthy"
includes an expired-but-not-yet-cleared cooldown, so re-tripping after expiry reports a
new transition). The WARN log stays at the recording call sites (client and processor)
where the causing error is in hand. The counter/gauge updates live inside `PeerHealth`
itself — it is the only place transitions are detectable under the lock, and it covers
every current and future caller. Gauge is refreshed on first sight and every
state-affecting call, including lazy cooldown-expiry recovery inside `IsHealthy`.

### 5. Metric label is the peer host, not the base URL

The proposal sketch said "peer label = the DataHub base URL", but internal/metrics'
registry policy is explicit: URL-derived labels MUST be hostname-only via `HostLabel`,
never the full URL (cardinality guard). `PeerHealth` already keys state on the URL
host, so `peer_host` (the existing label constant) loses nothing at ~3 peers and keeps
the new series joinable with `merkle_subtree_datahub_fetch_*{peer_host}`.

### 6. Grace config follows the existing peerhealth key pattern

`datahub.peerhealth.stale404gracesec`, default 3600, env
`DATAHUB_PEER_HEALTH_STALE404_GRACE_SEC` (matching the existing
`DATAHUB_PEER_HEALTH_FAILURE_THRESHOLD` / `DATAHUB_PEER_HEALTH_COOLDOWN_SEC` spelling).
Zero/negative selects the default rather than disabling suppression — a zero grace
would classify every stamped 404 as stale, which is never what an operator means.

## Risks / Trade-offs

- **A peer that prunes faster than the grace** (serves <1h) can 404 on stale-but-
  in-grace announcements and still trip the breaker. Acceptable: that peer genuinely
  fails announcements at a rate the gate exists to contain, and the grace is tunable.
- **A lying peer under deep consumer lag** (404s on data it announced >grace ago) is no
  longer breaker-visible for those messages. Acceptable: the messages still DLQ, the
  per-peer fetch metrics still show the 404 rate, and fresh announcements from the same
  peer keep counting.
- **Gauge writes on the hot path** (`IsHealthy` per message): one bounded-label
  `GaugeVec.Set` per call, negligible next to the existing per-message histogram work.
- **Two peers sharing a hostname on different ports** collapse to one `peer_host`
  metric series while breaker state stays per host:port. Not a real topology today;
  breaker behavior is unaffected.
