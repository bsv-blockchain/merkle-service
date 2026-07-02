## Context

merkle-service had structured-ish logging (JSON via slog) but no shared
field vocabulary with arcade, and no distributed tracing at all. Debugging a
production issue meant reading two services' logs with two different key
spellings for the same concept, and no way to see a request's full path
across an HTTP call into merkle-service, the Kafka hops it triggers, and the
eventual callback HTTP call back into arcade. Prometheus `/metrics` already
existed (`internal/metrics`, ~50 counters/histograms/gauges across `api`,
`bump`, `callback`, `database`, `kafka`, `subtree`) but nothing exported to
Coralogix directly, and there was no tracing SDK in the dependency tree at
all.

This change adds OpenTelemetry traces + metrics (push, off by default) and a
shared snake_case log-field canon, without touching the existing Prometheus
scrape path or requiring a rewrite of ~50 existing instruments.

## Goals / Non-Goals

**Goals:**
- A single Coralogix search for a `txid` or `block_hash` (or a `trace_id`)
  returns the full lifecycle across both merkle-service and arcade.
- Traces and metrics exportable to Coralogix via OTLP, with zero behavioral
  or performance cost when disabled.
- No duplicated instrumentation: OTLP metrics ride the existing Prometheus
  registry.
- Fix the `LOG_LEVEL` regression in subtree-fetcher/callback-delivery as
  part of the same logging-hygiene pass.

**Non-Goals:**
- OTLP log export. Stdout JSON → cluster filelog stays the only log path;
  adding a log exporter is a separate, larger decision (log volume/cost,
  buffering semantics) deliberately deferred.
- Any change to the existing Prometheus `/metrics` scrape contract — it
  continues to work unmodified alongside the new OTLP metrics pipeline.
- Production deployment wiring (collector topology, Coralogix-side parsing
  rules) — owned by the `bsva-infra-flux` repo, tracked in a separate PR.
- Per-service log-level configuration, log format changes, or log
  destination changes — out of scope; `configurable-log-level` already
  covers the level knob, this change only restores its conformance.

## Decisions

### 1. OTLP push to a node-local collector, not scrape

**Decision:** merkle-service pushes traces/metrics via OTLP (gRPC or HTTP)
to a collector endpoint, typically a node-local DaemonSet reached through
the Kubernetes downward API (`OTEL_EXPORTER_OTLP_ENDPOINT=http://$(HOST_IP):4317`),
rather than exposing an OTLP-shaped pull endpoint or relying solely on the
existing Prometheus scrape.

**Rationale:** Push matches how OTel SDKs are designed to work (batch
exporters, no separate scrape-target discovery needed for traces at all —
spans have no scrape analogue), keeps collector deployment topology (one
per node vs. cluster-central) an infra-repo decision independent of
application code, and means enabling telemetry per-pod is just flipping
`TELEMETRY_ENABLED` plus the two downward-API vars, with no service
discovery config in this repo.

**Alternative considered:** Keep Prometheus-only (scrape) and skip OTLP for
metrics, doing traces only. Rejected: Coralogix's native experience and
correlation features are built around OTLP; a scrape-only setup would still
need a Prometheus-remote-write or scrape-and-forward hop somewhere, which is
strictly more moving parts than a direct push, and it would leave metrics
and traces on two different transports/pipelines for no benefit.

### 2. Prometheus bridge over the private registry, not a rewrite

**Decision:** Bridge the OTLP metrics pipeline from the *existing* private
`internal/metrics.Registry` using
`go.opentelemetry.io/contrib/bridges/prometheus`
(`prometheusbridge.NewMetricProducer` + `sdkmetric.WithProducer`), explicitly
passing `WithGatherer(gatherer)` with that private registry.

**Rationale:** ~50 instruments already exist across `internal/metrics/*.go`.
Rewriting them against the OTel metrics API would touch every call site that
increments a counter or observes a histogram, for no operator-visible
benefit — the bridge makes the *existing* instruments appear on the OTLP
pipeline with zero call-site changes. The private-registry requirement is
load-bearing: the bridge's default gatherer is `prometheus.DefaultGatherer`,
which merkle-service never registers against (it uses its own
`metrics.Registry` so multiple in-process services in all-in-one mode don't
collide with global state) — omitting `WithGatherer` would silently export
zero metrics, which is why `telemetry.Options.Gatherer` is a required,
loudly-documented field rather than defaulted.

**Alternative considered:** Migrate to `go.opentelemetry.io/otel/metric`
natively and drop Prometheus. Rejected for this change: doubles the
migration surface (every instrument, every call site) for a metrics-only
win, when the bridge gets both transports for near-zero cost. Worth
revisiting only if Prometheus `/metrics` itself is ever deprecated.

### 3. Off by default, fail fast when misconfigured

**Decision:** `telemetry.enabled` defaults to `false`. With it false,
`telemetry.Init` builds no providers, sets no OTEL globals, and opens no
network connections — behaviorally identical to a build with no OTEL
support. When `true` but neither `telemetry.endpoint` nor the
`OTEL_EXPORTER_OTLP_*_ENDPOINT` env vars are set, `Init` returns an error
immediately rather than exporting into the void.

**Rationale:** Telemetry is an operational add-on, not a correctness
dependency — a slow/unreachable collector must never be able to take down
merkle-service or silently degrade its actual job (registering txids,
delivering callbacks). Defaulting off means every existing deployment is
unaffected until an operator opts in per-environment. Failing fast on a
missing endpoint (rather than starting up "successfully" and exporting
nothing) surfaces a misconfiguration at startup instead of as a silent gap
in a Coralogix dashboard days later.

### 4. Snake_case log-field canon shared with arcade

**Decision:** `internal/logfields` centralizes every well-known log
identifier (`txid`, `block_hash`, `callback_url`, ...) behind typed
constructors returning `slog.Attr`, using snake_case keys, matching a
canon shared by name with the arcade repo's own log-field package.
`make lint-logfields` greps for banned pre-canon key literals
(`blockHash`, `callbackUrl`, ...) in non-test `internal/`, `cmd/`, `tools/`
source, exempting struct tags and `FormValue` reads (wire formats, not log
fields).

**Rationale:** The entire point of a shared canon is Coralogix query
portability — `block_hash:"<hash>"` must return matches from both services.
That only holds if both repos independently agree on the same key spelling
for the same concept, which requires (a) a canonical source of truth per
repo, and (b) a way to prevent drift. Typed constructors make the correct
key the path of least resistance (call `logfields.BlockHash(v)`, don't
remember a string); the lint rule catches the case where someone reaches
for a literal anyway (e.g. copy-pasting from before the canon existed).
`txids` (list) vs `txid_count` (count) are kept as separate keys rather than
one polymorphic key — mixing an array and an int under the same JSON path
breaks Coralogix/Elasticsearch field-type inference for every future log
line under that key, not just the mixed one.

### 5. OTLP logs off — stdout → filelog stays canonical

**Decision:** No OTLP log exporter is implemented. `TelemetryConfig` has no
log-related fields at all (by contrast with `Traces`/`Metrics`).

**Rationale:** merkle-service's log path (stdout JSON → cluster filelog →
Coralogix) already works, is already how every other BSV Association
service ships logs, and carries none of the OTLP SDK's log-record batching
or backpressure risk. Splitting logs onto a second pipeline would mean two
divergent code paths that could disagree (an OTLP export failure vs. a
stdout write succeeding) for a signal that isn't the bottleneck this change
addresses — the bottleneck was field-name inconsistency, which the log-field
canon fixes without touching transport at all. `trace_id`/`span_id`
stamping (via `logfields.NewTraceHandler`) is what actually closes the
logs↔traces correlation gap, without requiring a log exporter.

### 6. franz-go header propagation, gated both directions for zero cost

**Decision:** The Kafka producer (`internal/kafka/otel_carrier.go`) injects
a W3C `traceparent` into `*kgo.Record` headers via a `recordCarrier`
(`propagation.TextMapCarrier` adapter, no intermediate map), but only when
`trace.SpanContextFromContext(ctx).IsValid()`. The consumer
(`internal/kafka/consumer.go`) extracts trace context from headers and
starts a `SpanKindConsumer` span named `"<topic> process"` (topic only, no
offset/key, to bound cardinality) — but only when the *extracted* context
carries a valid span. A record with no inbound trace context is not spanned
at all.

**Rationale:** This is the same "off means truly off" posture as decision 3,
applied per-message rather than per-process: telemetry disabled means every
`injectTraceContext`/`dispatchRecord` call is a single `IsValid` boolean
check and nothing else — no header allocation, no span, no propagator
call — verified by `BenchmarkInjectTraceContext_NoSpan` and an
`AllocsPerRun` assertion in `otel_carrier_test.go`. Symmetric gating on
extract matters independently of the producer-side gate: even with
telemetry enabled fleet-wide, a record produced by an older binary (or
during a partial rollout) carries no `traceparent` header, and starting a
span with an invalid/empty parent context would fabricate a disconnected
trace root that adds noise without adding correlation value. "No inbound
context → no span" keeps the signal honest: a span exists only when it can
actually be tied back to something.

### 7. Retry/DLQ republish uses `context.WithoutCancel`

**Decision:** Every retry/DLQ republish call (subtree processor, block
subtree worker, callback delivery) wraps its produce context with
`context.WithoutCancel(ctx)`.

**Rationale:** These are durable hand-offs — a message must reach the retry
or DLQ topic even if the consumer's context is mid-cancellation (shutdown,
rebalance) when the republish fires; dropping it there would silently lose
the message. `context.WithoutCancel` preserves everything else about the
context — notably the trace context injected onto it — while detaching it
from the parent's `Done()` channel/deadline, so the republished record still
carries `traceparent` and the trace stays connected across the retry hop
without inheriting a cancellation that would defeat the retry's purpose.
The same pattern already existed in `internal/telemetry.Init`'s own abort
path (bounding cleanup after a partial-init failure) before this decision
generalized it to the retry/DLQ call sites.

### 8. P2P announcements start root spans

**Decision:** `internal/p2p/otel.go` starts a root span (not a child of
anything) for each inbound subtree/block gossip message, named by a fixed
low-cardinality string (`"subtree announce"` / `"block announce"`) with the
hash attached as a span *attribute*, never folded into the name.

**Rationale:** A P2P gossip message has no inbound HTTP request or Kafka
record to inherit a trace from — the network is genuinely the origin of
this part of the pipeline. Starting a root span here (rather than skipping
tracing for the P2P entry point entirely) is what lets the rest of the
pipeline — Kafka produce, subtree/block processing, callback delivery — join
one trace instead of three disconnected ones. Keeping the hash out of the
span name (vs. e.g. `"subtree announce <hash>"`) bounds span-name
cardinality for any backend that indexes on it, matching the same
low-cardinality discipline already applied to the `otelhttp` route-pattern
span names and the Kafka consumer's `"<topic> process"` naming.

## Risks / Trade-offs

- **[Collector unavailability]** A down/unreachable OTLP collector could add
  latency or memory pressure (buffered spans/metrics) if not bounded →
  `telemetry.exportTimeoutMs` bounds every ForceFlush+Shutdown at process
  exit, and export failures are logged, never surfaced as a process error or
  non-zero exit code — a bad collector cannot block a deploy or crash a pod.
- **[Sampling loss]** `telemetry.sampleRatio` < 1.0 means some traces are
  invisible for exactly the requests that weren't sampled → default is 1.0
  (sample everything); operators reduce it deliberately, trading completeness
  for collector/ingestion load at scale.
- **[Partial-rollout trace gaps]** During a rolling deploy, some pods run
  with telemetry enabled and some without → by design, a record produced by
  a disabled/older pod simply carries no `traceparent` and the consumer
  doesn't fabricate a span for it (decision 6) — gaps are silent and
  non-fatal, never a disconnected/misleading partial trace.
- **[New dependency surface]** OTel SDK + exporters + the Prometheus bridge
  add several new modules to `go.mod` → all are official
  `go.opentelemetry.io/*` packages, already vetted for the Go ecosystem;
  none are on the hot path when `telemetry.enabled` is false.
- **[Log-field lint false negatives]** `lint-logfields`'s grep-based check
  only catches key literals matching its banned list, not novel
  mis-spellings introduced after this change → the constructor pattern
  (call `logfields.X(v)` instead of writing a string) is the primary
  defense; the lint rule is a backstop for the migration, not a general
  schema enforcer.
