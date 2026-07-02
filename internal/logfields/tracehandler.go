package logfields

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// traceHandler wraps an slog.Handler so that any log record made under a
// context carrying a valid OTEL span is stamped with trace_id/span_id
// (KeyTraceID/KeySpanID), correlating the log line with the distributed
// trace it happened inside.
//
// This only takes effect for log calls made through a *Context method
// (InfoContext, ErrorContext, WarnContext, DebugContext, ...): those are the
// only slog calls that thread the caller's context.Context into Handle. The
// non-Context methods (Info, Error, ...) pass context.Background() internally
// (see the log/slog package), which never carries a span, so they are
// unaffected — exactly the "only *Context calls" contract callers should
// expect.
type traceHandler struct {
	next slog.Handler
}

// NewTraceHandler returns an slog.Handler that delegates to h, adding
// trace_id/span_id attributes whenever the record's context carries a valid
// span.SpanContext. When the context carries no valid span — telemetry
// disabled, or simply a call site that didn't thread a span-bearing
// context — Handle adds nothing beyond the SpanContextFromContext/IsValid
// check, so the disabled/no-span path costs no allocation.
func NewTraceHandler(h slog.Handler) slog.Handler {
	return &traceHandler{next: h}
}

// Enabled delegates to the wrapped handler unchanged.
func (t *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return t.next.Enabled(ctx, level)
}

// Handle adds trace_id/span_id to rec when ctx carries a valid span, then
// delegates to the wrapped handler.
func (t *traceHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			TraceID(sc.TraceID().String()),
			SpanID(sc.SpanID().String()),
		)
	}
	return t.next.Handle(ctx, rec)
}

// WithAttrs delegates to the wrapped handler, preserving the trace-stamping
// behavior on the returned handler.
func (t *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{next: t.next.WithAttrs(attrs)}
}

// WithGroup delegates to the wrapped handler, preserving the trace-stamping
// behavior on the returned handler.
func (t *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{next: t.next.WithGroup(name)}
}
