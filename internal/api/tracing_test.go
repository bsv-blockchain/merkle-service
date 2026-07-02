package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/logfields"
)

// withTestTracing installs a real SDK TracerProvider (backed by an in-memory
// exporter) and the W3C tracecontext propagator as the OTEL globals for the
// duration of the test, restoring whatever was previously installed on
// cleanup. Tests must not run in parallel with each other while this is
// active (OTEL globals are process-wide).
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

// newTracedTestServer builds a fully-wired *Server (via NewServer + Init) so
// the real middleware stack — including the otelhttp span-per-request
// wrapper and tracingRouteMiddleware — is exercised, and returns the
// assembled http.Handler (s.httpServer.Handler) tests can drive directly
// without binding a listener.
func newTracedTestServer(t *testing.T, logger *slog.Logger) http.Handler {
	t.Helper()
	s := NewServer(config.APIConfig{Port: 0}, &fakeRegStore{}, nil, nil, logger)
	if err := s.Init(nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return s.httpServer.Handler
}

func TestInboundTracing_SpanNamedByRoutePattern(t *testing.T) {
	exporter := withTestTracing(t)
	handler := newTracedTestServer(t, slog.New(slog.NewJSONHandler(bytes.NewBuffer(nil), nil)))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/watch", strings.NewReader(`{"txid":"","callbackUrl":""}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span to be exported")
	}
	got := spans[len(spans)-1]
	if want := "POST /watch"; got.Name != want {
		t.Errorf("span name = %q, want %q", got.Name, want)
	}
	if !hasStringAttr(got.Attributes, "http.route", "/watch") {
		t.Errorf("expected http.route=/watch attribute, got %+v", got.Attributes)
	}
}

func TestInboundTracing_LookupRoutePattern(t *testing.T) {
	exporter := withTestTracing(t)
	handler := newTracedTestServer(t, slog.New(slog.NewJSONHandler(bytes.NewBuffer(nil), nil)))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/lookup/"+strings.Repeat("a", 64), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span to be exported")
	}
	got := spans[len(spans)-1]
	if want := "GET /api/lookup/{txid}"; got.Name != want {
		t.Errorf("span name = %q, want %q", got.Name, want)
	}
	if !hasStringAttr(got.Attributes, "http.route", "/api/lookup/{txid}") {
		t.Errorf("expected http.route=/api/lookup/{txid} attribute, got %+v", got.Attributes)
	}
}

func TestInboundTracing_UnmatchedRouteLabeled(t *testing.T) {
	exporter := withTestTracing(t)
	handler := newTracedTestServer(t, slog.New(slog.NewJSONHandler(bytes.NewBuffer(nil), nil)))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/no/such/route", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unmatched route, got %d", w.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one span to be exported")
	}
	got := spans[len(spans)-1]
	if want := "GET unmatched"; got.Name != want {
		t.Errorf("span name = %q, want %q", got.Name, want)
	}
	if !hasStringAttr(got.Attributes, "http.route", "unmatched") {
		t.Errorf("expected http.route=unmatched attribute, got %+v", got.Attributes)
	}
}

// TestInboundTracing_LogCorrelation verifies that the per-request log line
// emitted by middlewareLogger carries trace_id/span_id when a real tracer is
// active — closing the "log/trace correlation" half of Task 8 for inbound
// requests.
func TestInboundTracing_LogCorrelation(t *testing.T) {
	withTestTracing(t)

	var logBuf bytes.Buffer
	logger := slog.New(logfields.NewTraceHandler(slog.NewJSONHandler(&logBuf, nil)))
	handler := newTracedTestServer(t, logger)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var lastLine map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logBuf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		if m["msg"] == "request" {
			lastLine = m
		}
	}
	if lastLine == nil {
		t.Fatal("no \"request\" log line captured")
	}
	if _, ok := lastLine["trace_id"]; !ok {
		t.Errorf("request log line missing trace_id: %v", lastLine)
	}
	if _, ok := lastLine["span_id"]; !ok {
		t.Errorf("request log line missing span_id: %v", lastLine)
	}
}

func hasStringAttr(attrs []attribute.KeyValue, key, val string) bool {
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == val {
			return true
		}
	}
	return false
}
