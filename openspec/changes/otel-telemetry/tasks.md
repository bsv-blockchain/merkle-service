## 1. Log-Field Canon

- [x] 1.1 Create `internal/logfields` package with canonical snake_case key
      consts (`txid`, `txids`, `txid_count`, `txids_truncated`,
      `block_hash`, `block_height`, `subtree_hash`, `subtree_index`,
      `callback_url`, `datahub_url`, `peer_id`, `request_id`, `trace_id`,
      `span_id`) and typed `slog.Attr` constructors for each
- [x] 1.2 Migrate all existing log call sites in `internal/`, `cmd/`,
      `tools/` from ad-hoc string keys to the `logfields` constructors
- [x] 1.3 Fix `txids`/`txid_count` key collision found during the migration
      sweep; add `txids_truncated` to the canon
- [x] 1.4 Add `lint-logfields` Make target: fails the build on a banned
      pre-canon key literal (`blockHash`, `subtreeHash`, `subtreeID`,
      `subtreeIndex`, `callbackUrl`, `callbackURL`, `blockHeight`, `peerID`,
      `peerId`, `requestId`, `dataHubUrl`) in non-test source, exempting
      struct tags (`json:`/`yaml:`/`mapstructure:`) and `FormValue` reads
- [x] 1.5 Add `internal/logfields/logfields_test.go` covering every
      constructor's key/value round-trip

## 2. LOG_LEVEL Conformance Fix

- [x] 2.1 Fix `cmd/subtree-fetcher/main.go` and
      `cmd/callback-delivery/main.go` to construct their logger via
      `service.NewLogger(config.ParseLogLevel(cfg.LogLevel))` instead of
      the hardcoded `slog.LevelInfo` default, restoring
      `configurable-log-level` conformance
- [x] 2.2 Add/extend tests asserting `LOG_LEVEL=debug` produces debug output
      from subtree-fetcher and callback-delivery

## 3. SEEN-Path and Registration Log Visibility

- [x] 3.1 Add `subtree.seenTxidLogMax` config field (default 1000, env
      `SUBTREE_SEEN_TXID_LOG_MAX`) capping how many matched txids are
      logged per SEEN batch; `0` disables the list (counts-only)
- [x] 3.2 Log `"seen callback batch published"` once a SEEN_ON_NETWORK /
      SEEN_MULTIPLE_NODES batch is durably published, with `subtree_hash`,
      `callback_url`, `txid_count`, and (when under the cap) `txids` +
      `txids_truncated`
- [x] 3.3 Log `"registration accepted"` on `/watch` with `txid` +
      `callback_url`
- [x] 3.4 Add tests covering the truncation boundary (`seenTxidLogMax`
      exactly met, exceeded, and `0`)

## 4. OTel Telemetry Foundation

- [x] 4.1 Add `internal/version` package (`Version` var, `-ldflags`
      overridable) and wire `-ldflags "-X .../version.Version=$VERSION"`
      into the Dockerfile build for every binary
- [x] 4.2 Add `telemetry:` config block (`internal/config.TelemetryConfig`):
      `enabled`, `endpoint`, `protocol`, `insecure`, `serviceName`,
      `namespace`, `traces`, `metrics`, `sampleRatio`, `exportTimeoutMs`,
      each with an explicit Viper default and `TELEMETRY_*` env binding
- [x] 4.3 Add config validation: reject an invalid `protocol`, an
      out-of-range `sampleRatio`, or a scheme-prefixed `endpoint`
- [x] 4.4 Implement `internal/telemetry.Init`: builds the OTEL resource,
      trace pipeline (OTLP gRPC/HTTP exporter + `ParentBased(TraceIDRatioBased)`
      sampler + batch span processor), and metric pipeline (OTLP exporter +
      `PeriodicReader` fed by the Prometheus bridge over the caller's
      private `Gatherer`) — building every pipeline before publishing any
      OTEL global, so a failure never leaves the process half-instrumented
- [x] 4.5 Implement the disabled-is-true-noop contract: `Enabled=false`
      installs no providers, sets no globals, opens no connections
- [x] 4.6 Implement resource attribute resolution: `service.name/namespace/
      version/instance.id` + `merkle.mode`, merged with env-detected
      `OTEL_SERVICE_NAME`/`OTEL_RESOURCE_ATTRIBUTES` (env wins)
- [x] 4.7 Implement `service.InitTelemetry` wrapper in `internal/service`
      and call it from all 7 service `cmd/` mains with each binary's mode
      string and `metrics.Registry` as the gatherer
- [x] 4.8 Add `internal/telemetry/telemetry_test.go` covering disabled-noop,
      missing-endpoint failure, resource attribute precedence, and
      partial-init-failure cleanup

## 5. Log/Trace Correlation and Inbound HTTP Spans

- [x] 5.1 Implement `logfields.NewTraceHandler`: wraps an `slog.Handler` to
      stamp `trace_id`/`span_id` on records made via a `*Context` slog
      method under a valid span context
- [x] 5.2 Wire `NewTraceHandler` into `service.NewLogger` so every service
      logger gets trace correlation for free
- [x] 5.3 Wrap the chi router with `otelhttp.NewHandler`, naming spans by
      route pattern rather than raw path
- [x] 5.4 Add `internal/logfields/tracehandler_test.go` and
      `internal/api/tracing_test.go` covering the with-span and
      no-span-in-context cases

## 6. Outbound HTTP Tracing

- [x] 6.1 Wrap the DataHub client's transport with `otelhttp.NewTransport`,
      applied outside the SSRF-guarding transport
- [x] 6.2 Wrap the callback-delivery HTTP client's transport with
      `otelhttp.NewTransport`, same SSRF-guard ordering
- [x] 6.3 Add `internal/datahub/tracing_test.go` and
      `internal/callback/tracing_test.go` asserting spans are created for
      outbound calls and the SSRF guard still runs

## 7. Kafka Trace Propagation (franz-go)

- [x] 7.1 Add `internal/kafka/otel_carrier.go`: `recordCarrier`
      (`propagation.TextMapCarrier` over `*kgo.Record` headers),
      `injectTraceContext` (no-op unless the context carries a valid span),
      `extractTraceContext` (no-op when the record has no headers)
- [x] 7.2 Wire `injectTraceContext` into the producer's publish path
- [x] 7.3 Wire `extractTraceContext` + a gated `SpanKindConsumer` span
      (`"<topic> process"`, only started when the extracted context carries
      a valid span) into `dispatchRecord`
- [x] 7.4 Switch retry/DLQ republish call sites (subtree processor, block
      subtree worker, callback delivery) to `context.WithoutCancel(ctx)` so
      trace context survives a canceling consumer context
- [x] 7.5 Add `internal/kafka/otel_carrier_test.go` with
      `BenchmarkInjectTraceContext_NoSpan` + an `AllocsPerRun` assertion
      proving the disabled/no-span path is zero-allocation
- [x] 7.6 Add `internal/kafka/tracing_test.go` covering inject→extract
      round-trip and the no-inbound-context (no span started) case

## 8. P2P Root Spans

- [x] 8.1 Add `internal/p2p/otel.go`: `startAnnouncementSpan` starting a
      root span (`"subtree announce"` / `"block announce"`, hash as an
      attribute, never in the span name) for each inbound gossip message
- [x] 8.2 Wire `startAnnouncementSpan` into subtree/block message handling,
      propagating the resulting context into the Kafka publish so
      `injectTraceContext` carries it onward
- [x] 8.3 Add `internal/p2p/tracing_test.go` covering span creation and
      attribute correctness

## 9. Documentation

- [x] 9.1 Write `docs/observability.md`: pipeline overview (OTLP push for
      traces/metrics, stdout→filelog for logs, dual Prometheus+OTLP metrics
      export), config/env reference table, resource attributes, the
      log-field canon table, key log lines, Coralogix search recipes, and
      the deployment/telemetry-enablement section
- [x] 9.2 Verify `config.yaml`'s `telemetry:` and `metrics:` blocks stay in
      sync with `internal/config.TelemetryConfig`/`MetricsConfig`

## 10. Deployment Reference and Verification

- [x] 10.1 Add `HOST_IP`/`POD_NAME` downward-API env vars,
      `OTEL_EXPORTER_OTLP_ENDPOINT=http://$(HOST_IP):4317`, and
      `TELEMETRY_ENABLED="false"` to every `deploy/k8s/*.yaml` service
      Deployment's container `env:` block
- [x] 10.2 Note in `deploy/k8s/README.md` and `docs/observability.md` that
      these manifests are illustrative and the authoritative production
      rollout is the `bsva-infra-flux` repo
- [x] 10.3 Verify `go build ./...`, `go test ./... -count=1`, `gofmt -l .`,
      and `make lint` (including `lint-logfields`) are all clean
