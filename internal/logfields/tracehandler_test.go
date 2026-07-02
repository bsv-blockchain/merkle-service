package logfields

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestTraceHandler_StampsUnderActiveSpan verifies that a log line emitted via
// an InfoContext call whose context carries a valid (recording) span gets
// trace_id/span_id attached.
func TestTraceHandler_StampsUnderActiveSpan(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(&buf, nil)))

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "test-span")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line: %v (line: %s)", err, buf.String())
	}

	sc := span.SpanContext()
	if got[KeyTraceID] != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %s", got[KeyTraceID], sc.TraceID().String())
	}
	if got[KeySpanID] != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %s", got[KeySpanID], sc.SpanID().String())
	}
}

// TestTraceHandler_AbsentWithoutSpan verifies that a log line made under a
// context with no span (or via a non-Context call, which log/slog internally
// backs with context.Background()) carries no trace_id/span_id keys.
func TestTraceHandler_AbsentWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(&buf, nil)))

	logger.InfoContext(context.Background(), "hello")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line: %v (line: %s)", err, buf.String())
	}
	if _, ok := got[KeyTraceID]; ok {
		t.Errorf("trace_id present without an active span: %v", got)
	}
	if _, ok := got[KeySpanID]; ok {
		t.Errorf("span_id present without an active span: %v", got)
	}

	buf.Reset()
	logger.Info("plain call")
	got = nil
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line: %v (line: %s)", err, buf.String())
	}
	if _, ok := got[KeyTraceID]; ok {
		t.Errorf("trace_id present on a non-Context log call: %v", got)
	}
}

// TestTraceHandler_WithAttrsWithGroup verifies the pass-through delegation
// contract: WithAttrs/WithGroup preserve trace-stamping on the derived
// handler.
func TestTraceHandler_WithAttrsWithGroup(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(NewTraceHandler(slog.NewJSONHandler(&buf, nil)))
	logger := base.With("service", "test").WithGroup("grp")

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "test-span")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line: %v (line: %s)", err, buf.String())
	}
	if got["service"] != "test" {
		t.Errorf("service attr lost through WithAttrs delegation: %v", got)
	}
	grp, ok := got["grp"].(map[string]any)
	if !ok {
		t.Fatalf("expected a nested 'grp' group carrying the trace fields, got: %v", got)
	}
	if grp[KeyTraceID] != span.SpanContext().TraceID().String() {
		t.Errorf("trace_id lost through WithAttrs/WithGroup delegation: %v", got)
	}
}
