package p2p

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	teranode "github.com/bsv-blockchain/teranode/services/p2p"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/logfields"
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

func stringAttr(attrs []attribute.KeyValue, key string) (string, bool) {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString(), true
		}
	}
	return "", false
}

// TestHandleSubtreeMessage_RootSpanHashAttribute verifies the P2P subtree
// announcement gets a root span named "subtree announce" carrying the subtree
// hash as an ATTRIBUTE (never the span name), and that the span is ended.
func TestHandleSubtreeMessage_RootSpanHashAttribute(t *testing.T) {
	exporter := withTestTracing(t)
	client, _, _ := newTestClient(t)

	msg := teranode.SubtreeMessage{Hash: "subtree-xyz", DataHubURL: "https://dh.example/st"}
	if err := client.handleSubtreeMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleSubtreeMessage: %v", err)
	}

	spans := exporter.GetSpans() // GetSpans only returns ENDED spans
	if len(spans) != 1 {
		t.Fatalf("expected exactly 1 ended span, got %d", len(spans))
	}
	got := spans[0]
	if got.Name != "subtree announce" {
		t.Errorf("span name = %q, want %q", got.Name, "subtree announce")
	}
	if v, ok := stringAttr(got.Attributes, logfields.KeySubtreeHash); !ok || v != "subtree-xyz" {
		t.Errorf("subtree_hash attribute = %q (present=%v), want %q", v, ok, "subtree-xyz")
	}
}

// TestHandleBlockMessage_RootSpanHashAttribute verifies the block announcement
// root span mirrors the subtree one: "block announce" with the block hash as
// an attribute.
func TestHandleBlockMessage_RootSpanHashAttribute(t *testing.T) {
	exporter := withTestTracing(t)
	client, _, _ := newTestClient(t)

	msg := teranode.BlockMessage{Hash: "block-xyz", Height: 42, DataHubURL: "https://dh.example/blk"}
	if err := client.handleBlockMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleBlockMessage: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected exactly 1 ended span, got %d", len(spans))
	}
	got := spans[0]
	if got.Name != "block announce" {
		t.Errorf("span name = %q, want %q", got.Name, "block announce")
	}
	if v, ok := stringAttr(got.Attributes, logfields.KeyBlockHash); !ok || v != "block-xyz" {
		t.Errorf("block_hash attribute = %q (present=%v), want %q", v, ok, "block-xyz")
	}
}

// TestHandleSubtreeMessage_SpanEndsAndRecordsErrorOnPublishExhaustion drives
// the early-return failure path (publish exhausted) and asserts the root span
// still ends (GetSpans returns it), carries the hash attribute, and has the
// error recorded on it.
func TestHandleSubtreeMessage_SpanEndsAndRecordsErrorOnPublishExhaustion(t *testing.T) {
	withFastRetries(t)
	exporter := withTestTracing(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	failing := kafka.NewTestProducer(&mockSyncProducer{failErr: errors.New("kafka down")}, "subtree", logger)
	client := NewClient(config.P2PConfig{}, failing, failing, nil, false, logger)
	client.lookupIP = func(string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("203.0.113.10")}, nil
	}

	msg := teranode.SubtreeMessage{Hash: "subtree-fail", DataHubURL: "https://dh.example/st"}
	if err := client.handleSubtreeMessage(context.Background(), msg); !errors.Is(err, ErrPublishExhausted) {
		t.Fatalf("expected ErrPublishExhausted, got %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected exactly 1 ended span, got %d", len(spans))
	}
	got := spans[0]
	if v, ok := stringAttr(got.Attributes, logfields.KeySubtreeHash); !ok || v != "subtree-fail" {
		t.Errorf("subtree_hash attribute = %q (present=%v), want %q", v, ok, "subtree-fail")
	}
	if got.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error", got.Status.Code)
	}
	if len(got.Events) == 0 {
		t.Error("expected an exception event recorded on the span")
	}
}
