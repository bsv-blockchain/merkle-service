## ADDED Requirements

### Requirement: Subtree-work retries back off exponentially
The subtree-worker SHALL wait an exponential, context-aware backoff before re-publishing
a transiently-failed work item to `subtree-work`: `block.retryBackoffBaseMs` doubling
per attempt, capped at 5 seconds. A backoff of 0 (base <= 0) disables the wait. The
backoff MUST run before the AttemptCount bump and the republish, and an interrupted
wait (canceled consumer context) MUST surface an error without publishing so the
unacked original is redelivered.

#### Scenario: Transient failure retries after backoff
- **WHEN** a work item fails transiently below the retry budget with retryBackoffBaseMs 1000
- **THEN** the worker waits 1s, 2s, 4s, 5s, 5s… (per attempt) before each republish, and the retry publish carries AttemptCount+1

#### Scenario: Backoff aborts on context cancellation
- **WHEN** the consumer context is canceled while a retry backoff is in progress
- **THEN** the worker returns an error without re-publishing, and the original message is redelivered

#### Scenario: Zero base disables backoff
- **WHEN** block.retryBackoffBaseMs is 0 or unset (struct-literal test configs)
- **THEN** retries republish immediately, preserving pre-existing behavior

### Requirement: Disk-full work items are parked, never dead-lettered
The subtree-worker SHALL treat a full-filesystem failure (ENOSPC or quota/space text) as
an operational condition: log a warning, count `parked_disk_full`, throttle, and return
an error WITHOUT bumping AttemptCount, decrementing the per-block subtree counter, or
publishing to `subtree-work-dlq`. The unacked message stays in Kafka under topic
retention until the disk recovers.

#### Scenario: ENOSPC on STUMP blob write parks the work item
- **WHEN** the STUMP store Put fails with an error chain containing syscall.ENOSPC, even at maxAttempts-1
- **THEN** the worker publishes nothing to any topic, leaves AttemptCount and the subtree counter untouched, and returns an error so the consumer redelivers

#### Scenario: Quota-style text errors park too
- **WHEN** a failure arrives as text ("no space left on device", "disk quota exceeded") without an errno chain
- **THEN** the work item is parked exactly as for ENOSPC

### Requirement: DataHub 404s consume a reduced retry budget
The subtree-worker SHALL route a work item whose failure chain matches
`datahub.ErrNotFound` to the DLQ once `AttemptCount+1 >= block.notFoundMaxAttempts`
(default 3), decrementing the per-block subtree counter exactly once as on any DLQ
hand-off. The overall `block.maxAttempts` ceiling SHALL remain an independent DLQ
condition so a misconfigured notFoundMaxAttempts cannot extend the budget.

#### Scenario: Not-found exhausts its reduced budget
- **WHEN** a work item at AttemptCount 2 fails with a DataHub 404 and notFoundMaxAttempts is 3
- **THEN** the item is published to subtree-work-dlq and the counter is decremented exactly once

#### Scenario: Not-found below budget retries with backoff
- **WHEN** a work item at AttemptCount 0 fails with a DataHub 404
- **THEN** it is re-published for retry after its backoff, with no DLQ publish and no counter decrement

#### Scenario: maxAttempts remains a hard ceiling
- **WHEN** notFoundMaxAttempts is misconfigured above maxAttempts and a 404 item reaches AttemptCount maxAttempts-1
- **THEN** the item routes to the DLQ under the maxAttempts clause

### Requirement: Subtree-work retry outcomes are observable
The worker SHALL expose `merkle_subtree_work_messages_total` with an `outcome` label
(`retried`, `dlq`, `parked_disk_full`) incremented on each corresponding
`handleTransientFailure` branch.

#### Scenario: Outcomes counted per branch
- **WHEN** a work item is re-published for retry, routed to the DLQ, or parked on disk-full
- **THEN** the counter increments with outcome retried, dlq, or parked_disk_full respectively
