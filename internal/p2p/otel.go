package p2p

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies this package's spans/tracer to the OTEL SDK. Kept as
// the package import path per OTEL convention.
const tracerName = "github.com/bsv-blockchain/merkle-service/internal/p2p"

// startAnnouncementSpan starts a root span for a single P2P announcement
// (subtree or block). It is a ROOT span (not a child of anything) because
// each announcement is the origin of its own trace — the P2P network gossip
// that triggers it carries no inbound HTTP request or existing trace to
// inherit from. This is the span that Kafka produce (see
// injectTraceContext) propagates onward through the pipeline to callback
// delivery, closing merkle's half of the announce->callback trace.
//
// name is a fixed, low-cardinality string ("subtree announce" /
// "block announce"); the identifying hash is attached as an ATTRIBUTE, never
// folded into the span name, so cardinality stays bounded. With telemetry
// disabled this uses the global no-op TracerProvider, so Start/End are inert.
func startAnnouncementSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// This is a start-span factory: the span is returned for the caller to
	// end (handleSubtreeMessage/handleBlockMessage do `defer span.End()`),
	// not ended in this function, so spancheck's per-function End-call check
	// is a false positive here.
	//nolint:spancheck // span.End() is the caller's responsibility; see above
	spanCtx, span := otel.Tracer(tracerName).Start(ctx, name, trace.WithAttributes(attrs...))
	return spanCtx, span //nolint:spancheck // span.End() is the caller's responsibility; see above
}
