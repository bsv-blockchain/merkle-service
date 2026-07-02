package datahub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

// TestNewSSRFAwareHTTPClient_PropagatesTraceparent verifies the outbound
// otelhttp.NewTransport wrap (see newSSRFAwareHTTPClient) is inert with
// telemetry disabled (no traceparent header at all), and — once a real
// TracerProvider/propagator is installed — that a request made under an
// active parent span carries that span's trace ID onward, closing the
// arcade->merkle->DataHub leg of the trace.
func TestNewSSRFAwareHTTPClient_PropagatesTraceparent(t *testing.T) {
	var gotTraceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	// allowPrivateIPs=true: httptest binds to 127.0.0.1.
	client := newSSRFAwareHTTPClient(5, true)

	t.Run("telemetry disabled: no traceparent", func(t *testing.T) {
		// No withTestTracing here: the global TracerProvider/propagator are
		// whatever they default to (no-op), matching a production process
		// with telemetry.Init never called.
		gotTraceparent = ""
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		_ = resp.Body.Close()
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

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		_ = resp.Body.Close()
		if gotTraceparent == "" {
			t.Fatal("expected a traceparent header with an active span, got none")
		}
		if !strings.Contains(gotTraceparent, wantTraceID) {
			t.Errorf("traceparent %q does not carry the parent span's trace ID %q", gotTraceparent, wantTraceID)
		}
	})
}
