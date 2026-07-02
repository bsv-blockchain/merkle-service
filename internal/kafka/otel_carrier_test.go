package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// withTestPropagator installs the W3C tracecontext+baggage propagator as the
// OTEL global for the duration of the test, restoring whatever was
// previously installed on cleanup.
func withTestPropagator(t *testing.T) {
	t.Helper()
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
}

// TestRecordCarrier_RoundTrip verifies Get/Set/Keys round-trip through a
// *kgo.Record's headers.
func TestRecordCarrier_RoundTrip(t *testing.T) {
	rec := &kgo.Record{Topic: "test"}
	c := recordCarrier{rec: rec}

	c.Set("traceparent", "00-aaaa-bbbb-01")
	c.Set("baggage", "k=v")

	if got := c.Get("traceparent"); got != "00-aaaa-bbbb-01" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "00-aaaa-bbbb-01")
	}
	if got := c.Get("baggage"); got != "k=v" {
		t.Errorf("Get(baggage) = %q, want %q", got, "k=v")
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}

	keys := c.Keys()
	if len(keys) != 2 || keys[0] != "traceparent" || keys[1] != "baggage" {
		t.Errorf("Keys() = %v, want [traceparent baggage]", keys)
	}

	if len(rec.Headers) != 2 {
		t.Fatalf("rec.Headers has %d entries, want 2", len(rec.Headers))
	}
}

// TestInjectExtractTraceContext_RoundTrip verifies that injecting a valid
// span's context into a record and extracting it back out produces a
// context carrying the same trace ID.
func TestInjectExtractTraceContext_RoundTrip(t *testing.T) {
	withTestPropagator(t)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "produce")
	defer span.End()
	wantTraceID := span.SpanContext().TraceID()

	rec := &kgo.Record{Topic: "test"}
	injectTraceContext(ctx, rec)
	if len(rec.Headers) == 0 {
		t.Fatal("injectTraceContext did not add any headers for a valid span")
	}

	extracted := extractTraceContext(context.Background(), rec)
	gotSC := trace.SpanContextFromContext(extracted)
	if !gotSC.IsValid() {
		t.Fatal("extracted context carries no valid span context")
	}
	if gotSC.TraceID() != wantTraceID {
		t.Errorf("extracted trace ID = %s, want %s", gotSC.TraceID(), wantTraceID)
	}
}

// TestInjectTraceContext_NoSpanIsNoop verifies that injecting from a context
// with no valid span touches nothing.
func TestInjectTraceContext_NoSpanIsNoop(t *testing.T) {
	withTestPropagator(t)

	rec := &kgo.Record{Topic: "test"}
	injectTraceContext(context.Background(), rec)
	if len(rec.Headers) != 0 {
		t.Errorf("expected no headers added for a context with no valid span, got %v", rec.Headers)
	}
}

// TestExtractTraceContext_NoHeadersIsNoop verifies extraction from a record
// with no headers returns ctx unchanged (no propagator call, no span).
func TestExtractTraceContext_NoHeadersIsNoop(t *testing.T) {
	withTestPropagator(t)

	rec := &kgo.Record{Topic: "test"}
	ctx := context.Background()
	got := extractTraceContext(ctx, rec)
	if trace.SpanContextFromContext(got).IsValid() {
		t.Error("expected no valid span context extracted from a record with no headers")
	}
}

// TestInjectTraceContext_NoAllocOnNoSpanPath proves the zero-cost guardrail:
// injecting from a context with no valid span must not allocate.
func TestInjectTraceContext_NoAllocOnNoSpanPath(t *testing.T) {
	withTestPropagator(t)

	ctx := context.Background()
	rec := &kgo.Record{Topic: "test"}
	allocs := testing.AllocsPerRun(1000, func() {
		injectTraceContext(ctx, rec)
	})
	if allocs != 0 {
		t.Errorf("injectTraceContext on the no-span path allocated %.1f times per run, want 0", allocs)
	}
}

// BenchmarkInjectTraceContext_NoSpan measures the disabled/no-span produce
// path — this must show 0 allocs/op.
func BenchmarkInjectTraceContext_NoSpan(b *testing.B) {
	ctx := context.Background()
	rec := &kgo.Record{Topic: "bench"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		injectTraceContext(ctx, rec)
	}
}

// BenchmarkInjectTraceContext_WithSpan measures the enabled/valid-span
// produce path for comparison.
func BenchmarkInjectTraceContext_WithSpan(b *testing.B) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	defer otel.SetTextMapPropagator(prev)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("bench").Start(context.Background(), "produce")
	defer span.End()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := &kgo.Record{Topic: "bench"}
		injectTraceContext(ctx, rec)
	}
}
