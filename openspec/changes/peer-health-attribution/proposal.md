# Correct peer-health attribution for DataHub fetch failures

## Why

The peer-health breaker (threshold 3, cooldown 5m) counts **any** non-nil fetch error as
a peer failure (`recordPeerOutcome`, internal/datahub/client.go). Two mis-attributions
observed on dev-ovh-1 (15 Jul 2026) poisoned it:

1. **Caller cancellations.** A pod shutdown, consumer rebalance, or partition loss
   aborting an in-flight fetch recorded `context.Canceled` against the peer — one
   rollout tripped the breaker on fresh pods within minutes.
2. **Stale-announcement 404s.** Consumer lag aged subtree announcements past teranode's
   ~2h asset-cache retention; each resulting 404 (our lag, not a lying peer) re-tripped
   the breaker after every cooldown.

In the single-peer topology an open breaker means 100% of subtree announcements are
ack-and-dropped at the `IsHealthy` gate (internal/subtree/processor.go) — the entire
pre-store path is dead, and nothing on a dashboard says why: the breaker had no metrics
and no trip log.

## What Changes

- **Cancellation-neutral recording (all client call sites).** `recordPeerOutcome` takes
  the caller ctx; when `ctx.Err() != nil` at record time it records NOTHING — neither
  success nor failure. The client's own HTTP timeout firing while the caller ctx is
  alive still records a failure (peer slowness is peer-attributable); the two are
  distinguished via `ctx.Err()`, never by error string.
- **Age-aware 404 attribution (subtree fetch path only).** `kafka.SubtreeMessage` gains
  `announcedAtUnixMs`, stamped by the P2P client at publish time (missing/zero = age
  unknown = fresh, backward compatible with in-flight messages). The subtree processor
  fetches with a new `datahub.WithoutPeerRecording()` option and records explicitly
  after classifying: success → RecordSuccess; ctx canceled → nothing; 404 on an
  announcement older than `datahub.peerhealth.stale404GraceSec` (new config, default
  3600, env `DATAHUB_PEER_HEALTH_STALE404_GRACE_SEC`) → nothing; everything else
  (fresh/unstamped 404, transport, 5xx, parse) → RecordFailure. The message itself
  still routes to the DLQ as a permanent failure — only peer attribution changes.
- **Breaker-trip observability.** `PeerHealth.RecordFailure` returns `tripped bool`
  (true exactly once per healthy→unhealthy transition); recording call sites WARN-log
  the trip with the peer URL, threshold, and cooldown. New metrics:
  `merkle_datahub_peer_unhealthy_transitions_total{peer_host}` counter and
  `merkle_datahub_peer_healthy{peer_host}` gauge (1/0, set on first sight and every
  transition, including lazy cooldown-expiry recovery).
- **Unchanged:** the `IsHealthy` gate's ack-and-drop semantics, 404 → permanent DLQ
  routing for the message, ENOSPC parking, threshold/cooldown defaults.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `subtree-processing`: subtree fetch outcomes are classified (ctx state, announcement
  age) before feeding the peer-health breaker, instead of blanket-counting every error.
- `datahub-client`: peer-health recording is cancellation-neutral, per-call opt-out-able,
  and breaker transitions are observable via log + metrics.

## Impact

- **`internal/datahub/client.go`**: `recordPeerOutcome(ctx, ...)`; `FetchOption` /
  `WithoutPeerRecording`; trip WARN log.
- **`internal/datahub/peerhealth.go`**: `RecordFailure` returns `tripped`;
  `Threshold()`/`Cooldown()` accessors; gauge/counter updates; `DefaultStale404Grace`.
- **`internal/subtree/processor.go`**: fetch opts out of client recording;
  `recordPeerFetchOutcome` classification; stale comment fixes.
- **`internal/kafka/messages.go`**: `SubtreeMessage.AnnouncedAtUnixMs`.
- **`internal/p2p/client.go`**: stamps `AnnouncedAtUnixMs` at publish time.
- **`internal/config/config.go` / `config.yaml`**: `datahub.peerhealth.stale404gracesec`
  default + env binding; documented `peerHealth` block.
- **`internal/metrics/peerhealth.go`** (new): the transitions counter and health gauge.
- **Operational**: zero-downtime — unstamped in-flight messages classify as fresh
  (pre-change attribution), and the new config key defaults on.
