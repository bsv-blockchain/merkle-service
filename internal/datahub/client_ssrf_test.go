package datahub

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/ssrfguard"
)

// TestFetchSubtreeRaw_RejectsLoopbackURL verifies that a peer-supplied
// DataHub URL pointing at 127.0.0.1 is rejected at request-time
// validation, before any network I/O happens. This is the first layer of
// the SSRF defense: an attacker cannot turn a forged P2P announcement
// into a GET against a service listening on the loopback interface.
func TestFetchSubtreeRaw_RejectsLoopbackURL(t *testing.T) {
	client := NewClientWithSSRFGuard(5, 0, 0, 0, false, testLogger())

	_, err := client.FetchSubtreeRaw(context.Background(), "http://127.0.0.1:9999", "abc")
	if err == nil {
		t.Fatal("expected SSRF rejection for loopback URL, got nil")
	}
	if !errors.Is(err, ssrfguard.ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress, got: %v", err)
	}
}

// TestFetchBlockMetadata_RejectsPrivateIPURL verifies the same
// protection on the block-metadata path: a peer-supplied URL pointing
// at an RFC1918 IP must be refused.
func TestFetchBlockMetadata_RejectsPrivateIPURL(t *testing.T) {
	client := NewClientWithSSRFGuard(5, 0, 0, 0, false, testLogger())

	_, err := client.FetchBlockMetadata(context.Background(), "http://10.0.0.5:8080", "blockhash")
	if err == nil {
		t.Fatal("expected SSRF rejection for RFC1918 URL, got nil")
	}
	if !errors.Is(err, ssrfguard.ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress, got: %v", err)
	}
}

// TestFetchSubtreeRaw_RejectsCloudMetadataHost verifies the cloud
// metadata hostname denylist is enforced: even if a peer sends
// "metadata.google.internal" the request never leaves the process.
func TestFetchSubtreeRaw_RejectsCloudMetadataHost(t *testing.T) {
	client := NewClientWithSSRFGuard(5, 0, 0, 0, false, testLogger())

	// 169.254.169.254 is on the metadata-hostname denylist regardless
	// of allowPrivateIPs.
	_, err := client.FetchSubtreeRaw(context.Background(), "http://169.254.169.254/latest/meta-data", "abc")
	if err == nil {
		t.Fatal("expected SSRF rejection for cloud metadata IP, got nil")
	}
	if !errors.Is(err, ssrfguard.ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress, got: %v", err)
	}
}

// TestFetchSubtreeRaw_RejectsBadScheme verifies that non-http(s)
// schemes (file://, gopher://, etc.) are rejected as invalid before
// any I/O happens.
func TestFetchSubtreeRaw_RejectsBadScheme(t *testing.T) {
	client := NewClientWithSSRFGuard(5, 0, 0, 0, false, testLogger())

	_, err := client.FetchSubtreeRaw(context.Background(), "file:///etc/passwd", "abc")
	if err == nil {
		t.Fatal("expected rejection for file:// URL, got nil")
	}
	if !errors.Is(err, ssrfguard.ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL, got: %v", err)
	}
}

// TestFetchSubtreeRaw_DialBlocksLoopback verifies the dial-time SSRF
// guard. We craft a request that survives ValidateURL (using
// allowPrivateIPs=true at construction would defeat the test, so we
// instead aim a public-looking hostname at a loopback port via
// httptest, matching the DNS-rebinding scenario). Here we shortcut by
// using a literal IP after asserting validation; if the dial-time guard
// is removed, the connection succeeds and the test fails.
//
// The httptest server binds to 127.0.0.1; to exercise the dial hook
// rather than the URL hook, we directly invoke the lower-level
// doGetWithRetry so the test demonstrates the second-layer protection.
func TestFetchSubtreeRaw_DialBlocksLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not have been reached: %s %s", r.Method, r.URL)
	}))
	t.Cleanup(srv.Close)

	client := NewClientWithSSRFGuard(2, 0, 0, 0, false, testLogger())

	// doGetWithRetry skips the URL-level ssrfguard.ValidateURL, so a
	// successful block here can only come from the Dialer.Control hook.
	_, err := client.doGetWithRetry(context.Background(), srv.URL+"/subtree/abc", DefaultMaxSubtreeBytes)
	if err == nil {
		t.Fatal("expected dial-time SSRF block, got nil")
	}
	if !strings.Contains(err.Error(), "blocked") &&
		!strings.Contains(err.Error(), "refusing to dial") {
		t.Fatalf("expected SSRF dial-block error, got: %v", err)
	}
}

// TestFetchSubtreeRaw_AllowPrivateIPsOptIn verifies the operator
// escape-hatch: with AllowPrivateIPs=true the client successfully
// fetches from an httptest server on 127.0.0.1.
func TestFetchSubtreeRaw_AllowPrivateIPsOptIn(t *testing.T) {
	subtreeBytes := buildRawSubtreeBytes(2)
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(subtreeBytes)
	}))
	t.Cleanup(srv.Close)

	client := NewClientWithSSRFGuard(5, 0, 0, 0, true, testLogger())

	body, err := client.FetchSubtreeRaw(context.Background(), srv.URL, "abc")
	if err != nil {
		t.Fatalf("expected success with allowPrivateIPs=true, got %v", err)
	}
	if !hit {
		t.Fatal("server handler was not reached")
	}
	if len(body) != len(subtreeBytes) {
		t.Fatalf("expected body length %d, got %d", len(subtreeBytes), len(body))
	}
}

// TestFetchBlockMetadata_RejectsEmptyURL verifies that an empty
// dataHubURL (e.g. a peer that omitted the field) is rejected as an
// invalid URL rather than crashing or making a malformed request.
func TestFetchBlockMetadata_RejectsEmptyURL(t *testing.T) {
	client := NewClientWithSSRFGuard(5, 0, 0, 0, false, testLogger())

	_, err := client.FetchBlockMetadata(context.Background(), "", "blockhash")
	if err == nil {
		t.Fatal("expected rejection for empty URL, got nil")
	}
	if !errors.Is(err, ssrfguard.ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL, got: %v", err)
	}
}
