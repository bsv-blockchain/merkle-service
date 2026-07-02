package callback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// withTestTracing installs a real SDK TracerProvider and the W3C
// tracecontext propagator as the OTEL globals for the duration of the test,
// restoring whatever was previously installed on cleanup.
func withTestTracing(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
}

// TestDeliverCallback_PropagatesTraceparent verifies that deliverCallback's
// HTTP POST (via newDeliveryHTTPClient's otelhttp.NewTransport wrap) is
// inert with telemetry disabled, and — once a real TracerProvider/propagator
// is installed — carries the caller's active span's trace ID onward. This is
// what lets the trace begun on Kafka consume ride the callback POST back
// into arcade.
func TestDeliverCallback_PropagatesTraceparent(t *testing.T) {
	var gotTraceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	cfg := defaultTestConfig()
	cfg.Callback.AllowPrivateIPs = true // httptest binds to 127.0.0.1
	client := newDeliveryHTTPClient(cfg.Callback)
	ds, _, _, _ := newTestDeliveryServiceWithStumps(t, cfg, client) //nolint:dogsled // helper returns four values; only ds is needed here

	msg := &kafka.CallbackTopicMessage{
		CallbackURL: server.URL,
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "deadbeef",
	}

	t.Run("telemetry disabled: no traceparent", func(t *testing.T) {
		gotTraceparent = ""
		if err := ds.deliverCallback(context.Background(), msg); err != nil {
			t.Fatalf("deliverCallback: %v", err)
		}
		if gotTraceparent != "" {
			t.Errorf("expected no traceparent header with telemetry disabled, got %q", gotTraceparent)
		}
	})

	t.Run("telemetry enabled: carries the active span's trace ID", func(t *testing.T) {
		withTestTracing(t)
		gotTraceparent = ""
		ctx, span := otel.Tracer("test").Start(context.Background(), "outbound-test")
		defer span.End()
		wantTraceID := span.SpanContext().TraceID().String()

		if err := ds.deliverCallback(ctx, msg); err != nil {
			t.Fatalf("deliverCallback: %v", err)
		}
		if gotTraceparent == "" {
			t.Fatal("expected a traceparent header with an active span, got none")
		}
		if !strings.Contains(gotTraceparent, wantTraceID) {
			t.Errorf("traceparent %q does not carry the parent span's trace ID %q", gotTraceparent, wantTraceID)
		}
	})
}
