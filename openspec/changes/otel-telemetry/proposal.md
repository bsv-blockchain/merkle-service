## Why

Debugging a single transaction or block across arcade and merkle-service
meant grepping unstructured logs on both sides by hand, with no shared
vocabulary for "this is a txid" vs "this is a block hash" and no way to see
a request's path across an HTTP call, a Kafka hop, and a callback delivery.
Two independent capabilities are needed to fix that:

1. A shared, structured log-field canon (snake_case keys) between
   merkle-service and arcade, so a single Coralogix search for a `txid` or
   `block_hash` returns every log line — in either service — that touched
   it, instead of two separately-spelled fields that never join.
2. OpenTelemetry traces and metrics where none existed, so a request can be
   followed end-to-end (arcade → merkle-service HTTP → Kafka → callback HTTP
   → arcade) via a single `trace_id`, and so Coralogix can host OTLP-native
   dashboards without duplicating Prometheus instrumentation.

Along the way, a latent bug surfaced: subtree-fetcher and callback-delivery
ignored the configured `LOG_LEVEL` and always logged at `slog.LevelInfo`,
violating the existing `configurable-log-level` capability
(`openspec/specs/configurable-log-level/spec.md`, "All services use
configured log level"). That regression is fixed as part of this change.

## What Changes

- Add `internal/logfields`: canonical snake_case log-field keys (`txid`,
  `txids`, `txid_count`, `txids_truncated`, `block_hash`, `block_height`,
  `subtree_hash`, `subtree_index`, `callback_url`, `datahub_url`, `peer_id`,
  `request_id`, `trace_id`, `span_id`) with typed constructors, shared with
  arcade's own log-field canon. Add `make lint-logfields` to fail the build
  on a banned pre-canon key literal outside struct tags / `FormValue`.
- Fix subtree-fetcher and callback-delivery to honor the configured
  `LOG_LEVEL` instead of always logging at Info (restores
  `configurable-log-level` conformance).
- Add SEEN-path txid visibility: `/watch` logs `"registration accepted"`
  with `txid` + `callback_url`; SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES batch
  publishes log `"seen callback batch published"` with `subtree_hash`,
  `callback_url`, `txid_count`, and the matching `txids` capped by the new
  `subtree.seenTxidLogMax` config (default 1000, env
  `SUBTREE_SEEN_TXID_LOG_MAX`, `0` = counts-only).
- Add `internal/telemetry`: OTLP push export of traces and metrics to an
  external collector, off by default (`telemetry.enabled`, env
  `TELEMETRY_ENABLED`). Metrics are bridged from the existing private
  Prometheus registry (`internal/metrics.Registry`) via
  `go.opentelemetry.io/contrib/bridges/prometheus`, so the `/metrics`
  endpoint and the OTLP metrics pipeline both serve the same instruments
  without duplicated call sites. OTLP log export is explicitly NOT
  implemented — stdout JSON → cluster filelog remains the only log path.
- Add a shared OTEL resource (`service.name`/`namespace`/`version`/
  `instance.id` + a `merkle.mode` attribute identifying which of the 7
  service binaries is running) built in `internal/telemetry.buildResource`,
  merged with env-detected `OTEL_SERVICE_NAME`/`OTEL_RESOURCE_ATTRIBUTES`.
  `internal/version.Version` is set via `-ldflags` at Docker build time.
- Wire `service.InitTelemetry` into all 7 service `cmd/` mains
  (`merkle-service`, `api-server`, `block-processor`, `callback-delivery`,
  `p2p-client`, `subtree-fetcher`, `subtree-worker`).
- Correlate logs with traces: `logfields.NewTraceHandler` wraps every
  `slog.Handler` built by `service.NewLogger` and stamps `trace_id`/`span_id`
  onto any log record made via a `*Context` slog method under a valid span.
- Add inbound HTTP spans via `otelhttp.NewHandler` on the chi router
  (route-pattern span names, not raw paths).
- Add outbound HTTP tracing via `otelhttp.NewTransport` on the DataHub
  client and the callback-delivery HTTP client, applied outside the
  SSRF-guarding transport so the security check still runs on every dial.
- Propagate W3C `traceparent` through Kafka (franz-go): the producer injects
  into record headers only when the publishing context carries a valid span
  (zero-allocation no-op otherwise); the consumer extracts and, only when
  the extracted context carries a valid span, starts a `SpanKindConsumer`
  span before invoking the handler (same zero-cost-when-disabled guard).
  Retry/DLQ republish paths use `context.WithoutCancel(ctx)` so a durable
  hand-off survives a canceling consumer context while still carrying trace
  context forward.
- Add P2P root spans (`"subtree announce"` / `"block announce"`) for inbound
  gossip messages — the origin of merkle-service's half of the
  arcade → merkle → arcade trace.
- Document the pipeline in `docs/observability.md`: transport per signal,
  config/env reference, resource attributes, the log-field canon, Coralogix
  search recipes, the dual Prometheus+OTLP metrics export, and how
  `trace_id` correlates logs and traces.
- Add reference OTLP env wiring (`HOST_IP`/`POD_NAME` downward API,
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `TELEMETRY_ENABLED=false`) to every
  `deploy/k8s/*.yaml` service Deployment. These manifests are illustrative
  examples; the authoritative production rollout is a separate
  `bsva-infra-flux` PR.

## Capabilities

### New Capabilities

- `observability-telemetry`: OTel traces/metrics export (off by default),
  resource attributes, trace propagation across HTTP/Kafka/P2P, and the
  dual Prometheus+OTLP metrics posture.
- `structured-log-fields`: the shared snake_case log-field canon, its
  enforcement (`lint-logfields`), and the registration/SEEN-path log lines
  built on it.

### Modified Capabilities

- `configurable-log-level`: restores conformance with the existing
  requirement that every service use the configured log level —
  subtree-fetcher and callback-delivery previously ignored `LOG_LEVEL`.

## Impact

- New packages: `internal/telemetry`, `internal/logfields`,
  `internal/version`.
- New config block: `telemetry:` (`internal/config.TelemetryConfig`) with
  `TELEMETRY_*` env bindings; new `subtree.seenTxidLogMax` field
  (`SUBTREE_SEEN_TXID_LOG_MAX`).
- All 7 `cmd/` service mains gain telemetry init/shutdown and a
  version-stamped logger.
- `internal/api`, `internal/datahub`, `internal/callback`, `internal/kafka`,
  `internal/p2p` gain tracing instrumentation; no behavioral change when
  `telemetry.enabled` is `false` (the default).
- `internal/subtree`, `internal/api/handlers.go` gain new Info-level log
  lines (SEEN batch publish, registration accepted).
- `deploy/k8s/*.yaml`: reference-only env additions, default-safe
  (`TELEMETRY_ENABLED: "false"`).
- No breaking changes: every new behavior is additive and off by default
  except the `LOG_LEVEL` bug fix, which brings subtree-fetcher and
  callback-delivery in line with every other service's existing, documented
  behavior.
