package callback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// TestDeliveryHTTPClient_BlocksLoopbackByDefault verifies the SSRF
// dial-time guard refuses to connect to a 127.0.0.1 server even though
// httptest.NewServer happily accepts it. This catches the DNS-rebinding
// scenario where a callback URL points at "evil.example.com" but the
// attacker has flipped their DNS to return 127.0.0.1 between
// registration time and delivery time.
func TestDeliveryHTTPClient_BlocksLoopbackByDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not have been reached: %s %s", r.Method, r.URL)
	}))
	t.Cleanup(srv.Close)

	cfg := config.CallbackConfig{TimeoutSec: 2, AllowPrivateIPs: false}
	client := newDeliveryHTTPClient(cfg)

	// httptest binds to 127.0.0.1; attempting to reach it must fail.
	resp, err := client.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected dial-time block, got status %d", resp.StatusCode)
	}
	if !strings.Contains(err.Error(), "blocked") &&
		!strings.Contains(err.Error(), "refusing to dial") {
		t.Fatalf("expected SSRF guard error, got: %v", err)
	}
}

// TestDeliveryHTTPClient_AllowPrivateOptIn verifies the operator
// escape-hatch: with AllowPrivateIPs=true the same client successfully
// dials 127.0.0.1.
func TestDeliveryHTTPClient_AllowPrivateOptIn(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	cfg := config.CallbackConfig{TimeoutSec: 5, AllowPrivateIPs: true}
	client := newDeliveryHTTPClient(cfg)

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("expected success with allowPrivateIPs=true, got %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
	if !hit {
		t.Fatal("server handler was not reached")
	}
}

// TestDeliverCallback_RefusesPrivateIPViaTransport ensures
// deliverCallback (the high-level entry point) reports a transport
// error when the URL host is private and the SSRF guard is on. This is
// the end-to-end check at the delivery layer.
func TestDeliverCallback_RefusesPrivateIPViaTransport(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Callback.AllowPrivateIPs = false
	client := newDeliveryHTTPClient(cfg.Callback)

	ds, _, _, _ := newTestDeliveryServiceWithStumps(t, cfg, client)

	msg := &kafka.CallbackTopicMessage{
		// Valid public-looking URL parse, but resolves to loopback.
		CallbackURL: "http://127.0.0.1:1/cb",
		Type:        kafka.CallbackSeenOnNetwork,
		TxID:        "deadbeef",
	}
	err := ds.deliverCallback(context.Background(), msg)
	if err == nil {
		t.Fatal("expected delivery to fail with SSRF block, got nil")
	}
	if !strings.Contains(err.Error(), "blocked") &&
		!strings.Contains(err.Error(), "refusing") {
		t.Fatalf("expected SSRF block error, got: %v", err)
	}
}
