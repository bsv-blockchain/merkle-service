## ADDED Requirements

### Requirement: Peer-health recording is cancellation-neutral
The DataHub client SHALL NOT record any peer-health outcome — success or failure — for
a fetch whose caller context is already dead (`ctx.Err() != nil`) at record time. The
client's own HTTP timeout firing while the caller context is alive SHALL still record
a failure. The two cases MUST be distinguished via the caller context's error state,
never by matching error strings.

#### Scenario: Aborted fetch says nothing about the peer
- **WHEN** the caller's context is canceled while a block or subtree fetch is in flight
- **THEN** neither a failure nor a success is recorded against the peer

#### Scenario: Peer slowness still counts
- **WHEN** the client's configured HTTP timeout elapses while the caller's context is still alive
- **THEN** a failure is recorded against the peer

### Requirement: Call sites can opt out of internal peer-health recording
The DataHub client SHALL accept a per-call fetch option (`WithoutPeerRecording`) that
suppresses its internal peer-health recording for that fetch, so a call site with more
context (the subtree processor, which knows the announcement age) can classify and
record the outcome itself. Fetches without the option MUST record exactly as before.

#### Scenario: Opt-out fetch records nothing internally
- **WHEN** a subtree fetch is invoked with WithoutPeerRecording and fails
- **THEN** the client records no peer-health outcome for it, leaving attribution to the caller

### Requirement: Breaker transitions are observable
`PeerHealth.RecordFailure` SHALL report whether the call transitioned the peer from
healthy to unhealthy, returning true exactly once per breaker opening (a peer whose
cooldown has lapsed counts as healthy again, so a later threshold crossing reports a
new transition). Recording call sites SHALL log a WARN with the peer URL, failure
threshold, and cooldown on each trip. The service SHALL expose
`merkle_datahub_peer_unhealthy_transitions_total{peer_host}` (transitions counter) and
`merkle_datahub_peer_healthy{peer_host}` (1 healthy / 0 unhealthy), the gauge being set
on first sight of a peer and on every transition, including lazy cooldown-expiry
recovery inside `IsHealthy`.

#### Scenario: Breaker opening is logged and counted once
- **WHEN** a peer's consecutive attributed failures reach the threshold
- **THEN** exactly one WARN log and one transitions-counter increment are emitted, and the peer's health gauge drops to 0; further failures while open produce neither

#### Scenario: Recovery restores the gauge
- **WHEN** a tripped peer records a success, or its cooldown expires and it is next consulted
- **THEN** the peer's health gauge returns to 1
