## 1. Message stamp

- [x] 1.1 `SubtreeMessage.AnnouncedAtUnixMs int64` (`announcedAtUnixMs,omitempty`) in
  `internal/kafka/messages.go`; round-trip + backward-compatibility tests (legacy JSON
  decodes to 0; zero stamp omitted on encode)
- [x] 1.2 P2P client stamps `AnnouncedAtUnixMs: time.Now().UnixMilli()` when mapping a
  teranode subtree announcement to Kafka (`handleSubtreeMessage`); publish-window test

## 2. Breaker trip signal + metrics

- [x] 2.1 `PeerHealth.RecordFailure` returns `tripped bool` — true exactly once per
  healthy→unhealthy transition, including re-trips after cooldown expiry; nil receiver
  returns false; `Threshold()` / `Cooldown()` accessors for trip-log call sites
- [x] 2.2 New `internal/metrics/peerhealth.go`:
  `merkle_datahub_peer_unhealthy_transitions_total{peer_host}` counter and
  `merkle_datahub_peer_healthy{peer_host}` gauge + `IncPeerUnhealthyTransition` /
  `SetPeerHealthy` helpers (host label via `HostLabel`, per registry cardinality policy)
- [x] 2.3 Gauge/counter updated inside `PeerHealth` on first sight and every
  transition, including lazy cooldown-expiry recovery in `IsHealthy`; metric-delta tests

## 3. Cancellation-neutral client recording

- [x] 3.1 `recordPeerOutcome(ctx, url, err)`: dead caller ctx records nothing (success
  or failure); live-ctx failures WARN-log on trip with threshold/cooldown; all three
  block-metadata call sites pass ctx
- [x] 3.2 Tests: canceled-ctx failure and success record nothing; caller cancellation
  mid-fetch not recorded; client HTTP timeout with live caller ctx IS recorded

## 4. Age-aware 404 attribution in the subtree processor

- [x] 4.1 `FetchOption` + `WithoutPeerRecording()` on `FetchSubtreeRaw` (variadic;
  existing call sites unchanged); test that the option suppresses recording and the
  default path still records
- [x] 4.2 Config `datahub.peerhealth.stale404gracesec` (default 3600, env
  `DATAHUB_PEER_HEALTH_STALE404_GRACE_SEC`); `datahub.DefaultStale404Grace`; defaults +
  env-binding tests
- [x] 4.3 `recordPeerFetchOutcome` in the subtree processor: ctx dead → nothing;
  success → RecordSuccess; stale 404 → nothing (DEBUG log); fresh/unstamped 404,
  transport, 5xx, parse → RecordFailure with WARN on trip; stale comments at the fetch
  site and Init updated
- [x] 4.4 Classification tests incl. exact-grace boundary; regression: N sequential
  stale-404 messages never open the breaker (all still DLQ); fresh 404s still open it
  and the IsHealthy gate still ack-and-drops

## 5. Gates

- [x] 5.1 `go build ./...`, `go test ./... -count=1`, `go test -race` on
  internal/datahub + internal/subtree + internal/kafka + internal/p2p +
  internal/config + internal/metrics, `make lint`
