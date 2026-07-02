## ADDED Requirements

### Requirement: Telemetry disabled by default with zero runtime cost
The system SHALL default `telemetry.enabled` to `false`, and when disabled SHALL NOT create any OTEL trace or metric providers, SHALL NOT set any OTEL global providers, and SHALL NOT open any network connection to an OTLP collector. Runtime behavior with telemetry disabled SHALL be indistinguishable from a build with no OTEL support at all.

#### Scenario: Telemetry disabled is a true no-op
- **WHEN** `telemetry.enabled` is `false` (the default)
- **THEN** `internal/telemetry.Init` SHALL install no trace or metric providers and SHALL open no network connections
- **AND** the returned shutdown function SHALL be a no-op that never fails

#### Scenario: Kafka produce/consume cost nothing extra when disabled
- **WHEN** telemetry is disabled and a Kafka message is produced or consumed
- **THEN** the trace-context injection/extraction path SHALL perform at most a single validity check on the context's span, with no additional allocation, no header mutation, and no span creation

### Requirement: OTLP export of traces and metrics with resource attributes
When `telemetry.enabled` is `true`, the system SHALL export traces (when `telemetry.traces` is `true`) and metrics (when `telemetry.metrics` is `true`) to the configured OTLP endpoint, and every exported span and metric SHALL carry a shared resource including `service.name`, `service.version`, `service.instance.id`, and a `merkle.mode` attribute identifying which service binary produced it.

#### Scenario: Traces and metrics exported when enabled
- **WHEN** `telemetry.enabled` is `true` and both `telemetry.traces` and `telemetry.metrics` are `true`
- **THEN** the system SHALL export spans and metrics via OTLP to `telemetry.endpoint` (or the standard `OTEL_EXPORTER_OTLP_*` fallback endpoints when `telemetry.endpoint` is empty)

#### Scenario: Resource carries merkle.mode
- **WHEN** any service binary (`all-in-one`, `api-server`, `block-processor`, `callback-delivery`, `p2p-client`, `subtree-fetcher`, or `subtree-worker`) initializes telemetry
- **THEN** every span and metric it exports SHALL carry a `merkle.mode` resource attribute identifying that binary

#### Scenario: Environment resource attributes take priority
- **WHEN** `OTEL_SERVICE_NAME` or `OTEL_RESOURCE_ATTRIBUTES` environment variables are set
- **THEN** the values they specify SHALL override the corresponding config-derived resource attributes

#### Scenario: Missing endpoint fails fast
- **WHEN** `telemetry.enabled` is `true`, a signal (`telemetry.traces` or `telemetry.metrics`) is enabled, and neither `telemetry.endpoint` nor the corresponding `OTEL_EXPORTER_OTLP_*_ENDPOINT` environment variable is set
- **THEN** `internal/telemetry.Init` SHALL return an error rather than starting with no export destination

### Requirement: Metrics dual-export without duplicated instrumentation
The system SHALL bridge the OTLP metrics pipeline from the same private Prometheus registry (`internal/metrics.Registry`) that the `/metrics` HTTP endpoint serves, so that enabling `telemetry.metrics` requires no instrumentation call-site changes and the Prometheus `/metrics` endpoint and OTLP metrics export can both run simultaneously and independently.

#### Scenario: Prometheus and OTLP metrics run simultaneously
- **WHEN** `metrics.enabled` is `true` and `telemetry.enabled` with `telemetry.metrics` is also `true`
- **THEN** the same set of registered instruments SHALL be observable both via the Prometheus `/metrics` HTTP endpoint and via the OTLP metrics export, without either exporter disabling the other

#### Scenario: Disabling one metrics path leaves the other unaffected
- **WHEN** `metrics.enabled` is set to `false`
- **THEN** the OTLP metrics pipeline SHALL be unaffected by that setting and continue to export per `telemetry.metrics`

### Requirement: Distributed trace propagation across HTTP and Kafka
The system SHALL propagate W3C trace context across every hop of the arcade to merkle-service to arcade path when telemetry is enabled: inbound HTTP requests, outbound HTTP calls to DataHub and callback URLs, and Kafka records produced and consumed via franz-go.

#### Scenario: Inbound HTTP request is spanned by route pattern
- **WHEN** an HTTP request reaches a merkle-service endpoint (e.g. `/watch`) with telemetry enabled
- **THEN** the system SHALL start a span named after the route pattern (never the raw request path with embedded identifiers)

#### Scenario: Outbound HTTP calls carry trace context
- **WHEN** the system makes an outbound HTTP call to a DataHub peer or a callback URL within a traced context
- **THEN** the HTTP request SHALL carry a W3C `traceparent` header
- **AND** the SSRF guard SHALL still evaluate the destination on every call

#### Scenario: Kafka producer injects traceparent only within a trace
- **WHEN** a Kafka message is produced by a call whose context carries a valid span
- **THEN** the produced record SHALL carry a W3C `traceparent` header encoding that trace and span

#### Scenario: Kafka producer with no active trace injects nothing
- **WHEN** a Kafka message is produced by a call whose context carries no valid span (e.g. telemetry disabled, or an internal fire-and-forget publish)
- **THEN** the produced record SHALL carry no `traceparent` header and no additional processing SHALL occur beyond the validity check

#### Scenario: Kafka consumer spans only records with inbound trace context
- **WHEN** a Kafka consumer receives a record carrying a valid `traceparent` header
- **THEN** the system SHALL start a `SpanKindConsumer` span named `"<topic> process"` around the message handler, as a continuation of the producer's trace

#### Scenario: Kafka consumer does not span records with no inbound trace context
- **WHEN** a Kafka consumer receives a record with no `traceparent` header (or an invalid one)
- **THEN** the system SHALL NOT start a consumer span for that record and SHALL invoke the message handler directly

#### Scenario: Retry and DLQ republish preserve trace context past cancellation
- **WHEN** a message is republished to a retry or dead-letter topic during shutdown or consumer-context cancellation
- **THEN** the republish SHALL complete using a context detached from the parent's cancellation (`context.WithoutCancel`) while still carrying the original trace context forward

#### Scenario: P2P announcements start a root span
- **WHEN** the system receives a P2P subtree or block announcement
- **THEN** the system SHALL start a root span (`"subtree announce"` or `"block announce"`) with the subtree/block hash attached as a span attribute, not embedded in the span name
- **AND** that span's context SHALL be propagated onward through any resulting Kafka publish

### Requirement: OTLP log export is explicitly out of scope
The system SHALL NOT export logs via OTLP; structured JSON logs written to stdout, forwarded by the cluster's log agent, SHALL remain the only log transport.

#### Scenario: No OTLP log exporter exists
- **WHEN** telemetry is enabled, including both traces and metrics
- **THEN** the system SHALL NOT open an OTLP log export connection and SHALL continue writing structured JSON logs to stdout exactly as when telemetry is disabled

### Requirement: Logs correlate with traces via trace_id
When a log record is produced via an `slog` `*Context` method under a context carrying a valid OTEL span, the system SHALL stamp the record with `trace_id` and `span_id` fields matching that span's identifiers.

#### Scenario: Context-aware log call under an active span is stamped
- **WHEN** a log call is made via `InfoContext`/`WarnContext`/`ErrorContext`/`DebugContext` with a context carrying a valid span
- **THEN** the resulting log record SHALL include `trace_id` and `span_id` fields matching that span

#### Scenario: Non-context log calls are never stamped
- **WHEN** a log call is made via a non-`Context` method (`Info`, `Warn`, `Error`, `Debug`) or via a context carrying no valid span
- **THEN** the resulting log record SHALL NOT include `trace_id` or `span_id` fields
