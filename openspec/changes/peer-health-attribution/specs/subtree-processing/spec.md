## ADDED Requirements

### Requirement: Subtree fetch outcomes are classified before peer-health attribution
The subtree processor SHALL record DataHub subtree-fetch outcomes against the
peer-health tracker only after classifying them: a fetch whose caller context is
already canceled at record time SHALL record nothing (neither success nor failure); a
successful fetch SHALL record success; a 404 (`datahub: not found`) on an announcement
whose `announcedAtUnixMs` stamp is strictly older than
`datahub.peerhealth.stale404GraceSec` (default 3600) SHALL record nothing; every other
failure — a 404 on a fresh or unstamped announcement, transport errors, 5xx responses,
parse failures — SHALL record a failure. Message-level handling MUST be unchanged by
attribution: a 404 still routes the message to the permanent-failure DLQ path, and an
unhealthy peer's announcements are still acknowledged and dropped at the health gate.

#### Scenario: Caller cancellation is not attributed to the peer
- **WHEN** a pod shutdown, consumer rebalance, or partition loss cancels the handler context while a subtree fetch is in flight
- **THEN** the resulting fetch error is not recorded against the announcing peer, and the peer-health breaker state is unchanged

#### Scenario: Stale-announcement 404 is attributed to consumer lag
- **WHEN** a subtree announcement older than the stale-404 grace (because it sat in Kafka past teranode's asset-cache retention) fetches a 404 from the announcing peer
- **THEN** no peer-health failure is recorded, and the message still routes to the subtree DLQ as a permanent failure

#### Scenario: Sequential stale 404s never open the breaker
- **WHEN** an unbroken run of lag-aged announcements all fetch 404s from the same peer
- **THEN** the peer remains healthy and subsequent fresh announcements from it are still fetched, instead of the breaker re-opening after every cooldown

#### Scenario: Fresh 404s still open the breaker
- **WHEN** a peer returns 404 for announcements at or under the grace age (including unstamped messages produced before the stamp existed) failureThreshold times consecutively
- **THEN** the peer is marked unhealthy and its announcements are acknowledged and dropped at the health gate until the cooldown expires

#### Scenario: Client HTTP timeout with a live caller is attributed
- **WHEN** the DataHub client's own HTTP timeout fires while the caller's context is still alive
- **THEN** the failure is recorded against the peer, because peer slowness is peer-attributable

### Requirement: Subtree announcements carry their publish time
The P2P client SHALL stamp `announcedAtUnixMs` (Unix milliseconds) on every subtree
message at Kafka publish time. The field MUST survive the retry republish path
unchanged, MUST be omitted from JSON when zero, and consumers MUST treat a missing or
zero stamp as "age unknown" and classify such messages as fresh.

#### Scenario: Announcement stamped at publish time
- **WHEN** the P2P client maps a teranode subtree announcement to a Kafka subtree message
- **THEN** the published message carries announcedAtUnixMs set to the publish wall-clock time

#### Scenario: Legacy messages remain compatible
- **WHEN** a subtree message produced before the stamp existed is consumed
- **THEN** it decodes with a zero stamp and its fetch failures are attributed exactly as before the change
