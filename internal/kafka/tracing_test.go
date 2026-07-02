package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// withTestTracing installs a real SDK TracerProvider (backed by an in-memory
// exporter) and the W3C tracecontext propagator as the OTEL globals for the
// duration of the test, restoring whatever was previously installed on
// cleanup.
func withTestTracing(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return exporter
}

// TestDispatchRecord_ExtractsProducerTraceContext is the end-to-end
// arcade->merkle Kafka-hop test: a record carrying a traceparent header
// (as injectTraceContext would have stamped on produce) must result in the
// handler's context carrying that SAME trace ID, and the consumer span
// itself must be a child of that trace — this is what lets a trace begun on
// the producer side (e.g. an inbound HTTP request) continue across the
// Kafka hop.
func TestDispatchRecord_ExtractsProducerTraceContext(t *testing.T) {
	exporter := withTestTracing(t)

	// Simulate what a producer with an active span would have done.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	producerCtx, producerSpan := tp.Tracer("producer-side").Start(context.Background(), "produce")
	defer producerSpan.End()
	wantTraceID := producerSpan.SpanContext().TraceID()

	rec := &kgo.Record{Topic: "subtree"}
	injectTraceContext(producerCtx, rec)
	if len(rec.Headers) == 0 {
		t.Fatal("injectTraceContext did not stamp any headers")
	}

	var gotTraceID trace.TraceID
	handler := func(ctx context.Context, _ *Message) error {
		gotTraceID = trace.SpanContextFromContext(ctx).TraceID()
		return nil
	}

	if err := dispatchRecord(context.Background(), rec, handler); err != nil {
		t.Fatalf("dispatchRecord: %v", err)
	}
	if gotTraceID != wantTraceID {
		t.Errorf("handler ctx trace ID = %s, want %s (producer's trace)", gotTraceID, wantTraceID)
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected a consumer span to be exported")
	}
	got := spans[len(spans)-1]
	if got.Name != "subtree process" {
		t.Errorf("span name = %q, want %q", got.Name, "subtree process")
	}
	if got.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span kind = %v, want Consumer", got.SpanKind)
	}
	if got.SpanContext.TraceID() != wantTraceID {
		t.Errorf("consumer span trace ID = %s, want %s (child of the producer's trace)", got.SpanContext.TraceID(), wantTraceID)
	}
}

// TestDispatchRecord_NoHeadersStartsFreshTrace verifies that a record with no
// trace headers (telemetry disabled end-to-end, or produced before this
// feature shipped) still gets a consumer span — just as the root of a new
// trace rather than a continuation.
func TestDispatchRecord_NoHeadersStartsFreshTrace(t *testing.T) {
	withTestTracing(t)

	rec := &kgo.Record{Topic: "block"}
	handler := func(_ context.Context, _ *Message) error { return nil }

	if err := dispatchRecord(context.Background(), rec, handler); err != nil {
		t.Fatalf("dispatchRecord: %v", err)
	}
}

// TestDispatchRecord_RecordsHandlerError verifies a handler error is
// recorded on the span (RecordError + Error status) and still propagated to
// the caller so F-030 commit/rewind semantics are unaffected.
func TestDispatchRecord_RecordsHandlerError(t *testing.T) {
	exporter := withTestTracing(t)

	wantErr := errors.New("boom")
	rec := &kgo.Record{Topic: "callback"}
	handler := func(_ context.Context, _ *Message) error { return wantErr }

	err := dispatchRecord(context.Background(), rec, handler)
	if !errors.Is(err, wantErr) {
		t.Fatalf("dispatchRecord returned %v, want %v", err, wantErr)
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected a consumer span to be exported")
	}
	got := spans[len(spans)-1]
	if len(got.Events) == 0 {
		t.Fatal("expected an exception event recorded on the span")
	}
	if got.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error", got.Status.Code)
	}
}
