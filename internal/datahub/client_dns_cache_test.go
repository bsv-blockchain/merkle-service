package datahub

import (
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

// countingLookup returns a fixed IP and counts how many times it was invoked,
// standing in for net.LookupIP so the validation cache can be observed without
// real DNS.
func countingLookup(ip string, calls *int) func(string) ([]net.IP, error) {
	return func(string) ([]net.IP, error) {
		*calls++
		return []net.IP{net.ParseIP(ip)}, nil
	}
}

func TestValidateDataHubURL_CachesSuccess(t *testing.T) {
	var calls int
	c := NewClientWithSSRFGuard(5, 1, 0, 0, false, slog.Default())
	c.lookupIP = countingLookup("93.184.216.34", &calls) // public IP -> passes

	const url = "http://datahub-peer.example:8080"
	for i := 0; i < 3; i++ {
		if err := c.validateDataHubURL(url); err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
	}
	if calls != 1 {
		t.Fatalf("resolver called %d times for the same URL, want 1 (cached)", calls)
	}

	// A different peer URL must validate independently (separate cache entry).
	if err := c.validateDataHubURL("http://other-peer.example:8080"); err != nil {
		t.Fatalf("second URL: unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("resolver called %d times across two distinct URLs, want 2", calls)
	}
}

func TestValidateDataHubURL_DoesNotCacheFailure(t *testing.T) {
	var calls int
	// allowPrivateIPs=false so a private resolved IP is rejected.
	c := NewClientWithSSRFGuard(5, 1, 0, 0, false, slog.Default())
	c.lookupIP = countingLookup("10.0.0.1", &calls) // private IP -> rejected

	const url = "http://evil-peer.example:8080"
	for i := 0; i < 3; i++ {
		if err := c.validateDataHubURL(url); err == nil {
			t.Fatalf("call %d: expected SSRF rejection, got nil", i)
		}
	}
	if calls != 3 {
		t.Fatalf("resolver called %d times, want 3 (failures must never be cached)", calls)
	}
}

func TestValidateDataHubURL_ExpiredEntryRevalidates(t *testing.T) {
	var calls int
	c := NewClientWithSSRFGuard(5, 1, 0, 0, false, slog.Default())
	c.lookupIP = countingLookup("93.184.216.34", &calls)

	const url = "http://datahub-peer.example:8080"
	// Pre-seed an already-expired entry; validation must re-run, not trust it.
	c.validatedURLs.Store(url, time.Now().Add(-time.Second).UnixNano())

	if err := c.validateDataHubURL(url); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expired entry was not re-validated (resolver called %d times, want 1)", calls)
	}
	// And the refreshed entry is now cached.
	if err := c.validateDataHubURL(url); err != nil {
		t.Fatalf("unexpected error after refresh: %v", err)
	}
	if calls != 1 {
		t.Fatalf("refreshed entry not cached (resolver called %d times, want 1)", calls)
	}
}

func TestNewSSRFAwareHTTPClient_PoolTuning(t *testing.T) {
	client := newSSRFAwareHTTPClient(5, false)
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", client.Transport)
	}
	// Guard against a regression back to net/http's default of 2 idle
	// conns/host, which forces re-dials under same-peer fan-out.
	if tr.MaxIdleConnsPerHost != 64 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 64", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns != 128 {
		t.Errorf("MaxIdleConns = %d, want 128", tr.MaxIdleConns)
	}
}
