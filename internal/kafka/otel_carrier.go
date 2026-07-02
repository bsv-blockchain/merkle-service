package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies this package's spans/tracer to the OTEL SDK. Kept as
// the package import path per OTEL convention (mirrors how otelhttp names its
// own instrumentation scope, and the equivalent arcade/kafka carrier).
const tracerName = "github.com/bsv-blockchain/merkle-service/internal/kafka"

// recordCarrier adapts a *kgo.Record's headers to propagation.TextMapCarrier
// so the standard W3C tracecontext/baggage propagators can inject into and
// extract from a Kafka record directly — no intermediate map[string]string
// copy. Unlike a map-backed carrier, Set on a record with an existing header
// of the same key appends a duplicate (kgo.RecordHeader has no map
// semantics); this is fine here because Inject is only ever called once per
// outbound record, immediately before Produce.
type recordCarrier struct {
	rec *kgo.Record
}

// Get implements propagation.TextMapCarrier. Returns the value of the FIRST
// header matching key, or "" if absent.
func (c recordCarrier) Get(key string) string {
	for _, h := range c.rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set implements propagation.TextMapCarrier.
func (c recordCarrier) Set(key, value string) {
	c.rec.Headers = append(c.rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys implements propagation.TextMapCarrier.
func (c recordCarrier) Keys() []string {
	keys := make([]string, len(c.rec.Headers))
	for i, h := range c.rec.Headers {
		keys[i] = h.Key
	}
	return keys
}

// injectTraceContext injects ctx's trace context into rec's Kafka headers,
// but only when ctx carries a valid span.
//
// This is the hot-path guard: when telemetry is disabled (or ctx simply
// carries no active span — e.g. an internal fire-and-forget publish), this
// returns immediately without touching rec.Headers or calling the
// propagator, so every produce call on that path costs one interface-boxed
// trace.SpanContext comparison and nothing else — zero additional
// allocation. See BenchmarkInjectTraceContext_NoSpan and the AllocsPerRun
// assertion in otel_carrier_test.go.
func injectTraceContext(ctx context.Context, rec *kgo.Record) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, recordCarrier{rec: rec})
}

// extractTraceContext returns ctx annotated with the trace context carried
// by rec's Kafka headers. When rec has no headers (the producer never
// injected — telemetry disabled end-to-end, or a message produced before
// this feature shipped), ctx is returned unchanged with no propagator call.
func extractTraceContext(ctx context.Context, rec *kgo.Record) context.Context {
	if len(rec.Headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, recordCarrier{rec: rec})
}
