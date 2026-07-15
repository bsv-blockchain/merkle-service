# Design: subtree-worker retry backpressure

## Context

`internal/block/subtree_worker.go` consumes `subtree-work` (one message per subtree per
block) on a per-partition serial worker (`internal/kafka/consumer.go`). On transient
failure, `handleTransientFailure` either re-publishes the item with `AttemptCount+1`
(retry) or, at `block.maxAttempts` / on a permanent SSRF-classified URL, decrements the
per-block subtree counter and publishes to `subtree-work-dlq` (terminal). The counter is
decremented exactly once per work item — success or DLQ, never retry — which is what
lets BLOCK_PROCESSED still fire when a subtree's retries exhaust.

The fetcher (`internal/subtree/processor.go`) already solved the same three problems for
the `subtree` topic in v0.4.6: exponential backoff (`retryBackoff`/`waitBackoff`),
ENOSPC parking (`isDiskFull`), and permanent-failure classification. This change ports
that shape to the worker without disturbing the counter contract.

## Goals / Non-Goals

**Goals:**

- Retry republishes span real time instead of burning the budget in milliseconds.
- A full disk never dead-letters work items (the DLQ has no replay).
- Deterministically-stale 404s stop consuming the full 10-attempt budget.
- Per-outcome counter metric for the worker retry path.
- Zero behavior change for the fetcher; zero-value config keeps existing worker tests
  (and any struct-literal construction) backoff-free.

**Non-Goals:**

- Changing consumer semantics (chunked commits, rewind, rebalance handling).
- DLQ replay tooling.
- Raising `aerospike.subtreeCounterTTLSec` (flagged as an owner decision, out of scope).

## Decisions

### 1. Worker backoff cap = 5s, NOT the fetcher's 30s

The fetcher's default budget (`subtree.maxAttempts=3`) never reaches its 30s cap — its
schedule tops out at 4s. The worker's (`block.maxAttempts=10`) would *live* at a 30s
cap. That interacts badly with the consumer's chunked dispatch:

- `processBatch` runs 50-record chunks with no intra-chunk quit check, and a graceful
  revoke drains the whole in-flight chunk without context cancellation.
- At 30s/record, a chunk of failing records sleeps ~25 minutes against
  `rebalanceTimeout = 5m` → the member is fenced. Worse, a fenced member cannot even
  observe the fencing promptly: `partitionsLost` runs on the poll goroutine, which is
  blocked dispatching into the sleeping partition's depth-5 channel.
- A **5s cap** bounds the worst-case chunk at ~4.2 minutes — under the 5m rebalance
  window.

Schedule at the default base (1000ms): 1s, 2s, 4s, 5s, 5s, … ≈ 37s of deliberate wait
across a 10-attempt life. Each republish also re-traverses the partition backlog before
its next attempt, so the real spread during an incident is minutes — enough for a
dependency blip to clear, and it removes the ~10× immediate-republish amplification.

The cap is a *parameter* of `retryutil.Backoff` (not a package constant), so the fetcher
keeps `subtreeRetryBackoffCap = 30s` locally and the worker declares
`workerRetryBackoffCap = 5s` next to the consumer-semantics comment that justifies it.

### 2. Extract `internal/retryutil` instead of copying

The strongest argument is `IsDiskFull`: ENOSPC via `errors.Is` **plus** a string match
list ("no space left on device", "disk quota exceeded") for store layers that drop the
errno chain. A copied list silently forks the moment one site learns a new spelling.
`retryutil` imports nothing internal (stdlib only), so there is no cycle risk from
either `internal/subtree` or `internal/block`. The fetcher keeps one-line wrapper
methods (`retryBackoff`, `waitBackoff`, `isDiskFull`) so its existing tests — including
`processor_backpressure_test.go` — pass unmodified, which is the zero-behavior-change
gate for the refactor.

### 3. Disk-full parks BEFORE attempt accounting, and never touches the counter

The park branch sits at the top of `handleTransientFailure`, before `nextAttempt` is
computed: WARN + `merkle_subtree_work_messages_total{outcome="parked_disk_full"}` + a
context-aware throttle sleep (`retryBackoff(AttemptCount+1)`) + **return the error**.
No AttemptCount bump (the message is never re-published), no counter decrement, no DLQ.
The consumer does not advance past the record; its rewind/redeliver cycle (~1.5–2s per
partition on top of the throttle) retries under topic retention until the disk recovers.

**Counter-TTL caveat (runbook item):** parking relies on the per-block subtree counter
still existing when the disk recovers. An ENOSPC outage that leaves a block's counter
untouched for longer than `aerospike.subtreeCounterTTLSec` (7200 on dev-ovh-1) expires
it; on recovery the parked items' decrements hit the existing `ErrCounterNotFound`
ACK+ALERT path, and those blocks need a manual `POST /api/v1/blocks/{hash}/reprocess`
to rebuild the counter and re-emit BLOCK_PROCESSED. This is strictly better than
today's behavior (the same outage currently dead-letters the items un-replayably AND
still loses BLOCK_PROCESSED coordination), but operators must know the recovery step.
Widening the TTL is a separate owner decision.

### 4. 404s get a reduced budget with a hard ceiling — not the fetcher's immediate DLQ

The fetcher DLQs a DataHub 404 immediately (`handlePermanentFailure`): at announcement
time, a peer 404ing a subtree it just announced is lying. The worker deliberately
diverges: its subtrees **provably existed** at announcement time (the block referenced
them), so a 404 here is usually cache pruning on one peer — a short retry can win via
another attempt window, but retrying 10 times cannot. `block.notFoundMaxAttempts`
(default 3) bounds the budget for `errors.Is(cause, datahub.ErrNotFound)`.

`nextAttempt >= maxAttempts` stays an **independent** clause of the DLQ condition, so a
misconfigured `notFoundMaxAttempts > maxAttempts` can never extend the overall budget.

### 5. Backoff waits before the republish, and an interrupted wait publishes nothing

`retryutil.Wait(ctx, backoff)` runs BEFORE the AttemptCount bump/encode/publish. The
ctx is the per-partition worker context, canceled on partitions-lost — a sleeping
handler aborts promptly instead of finishing a hand-off for a partition another member
owns. An interrupted wait returns an error without publishing: the unacked original is
redelivered, so nothing is lost or double-counted. The DLQ branch stays sleep-free
(it's terminal; delaying it only delays BLOCK_PROCESSED).

## Risks / Trade-offs

- **Wider fetch→commit window** slightly raises the pre-existing odds of a
  duplicate republish around a rebalance (at-least-once). Absorbed by receiver-side
  dedup; tests must not assume exactly-once.
- **Consumer lag during outages is now expected.** `subtree-work` lag alerts fire
  during a long dependency outage where previously items would have burned to the DLQ
  quickly. This is the intended trade: lag is recoverable, the DLQ is not.
- **Parked messages hold their partition.** One disk-full worker parks all partitions
  it owns; that is exactly the fetcher's accepted semantics (a full disk is
  process-wide anyway).
- **Counter TTL vs long outages** — see decision 3; documented runbook step, not code.
