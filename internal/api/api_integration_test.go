//go:build integration

package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/api"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// setupTestServer creates an API server backed by real Aerospike and returns
// an httptest.Server wrapping its chi router. The caller should defer ts.Close().
func setupTestServer(t *testing.T) (*httptest.Server, *store.RegistrationStore, *store.AerospikeClient) {
	t.Helper()

	asClient, err := store.NewAerospikeClient("localhost", 3000, "test", 3, 100, slog.Default())
	if err != nil {
		t.Fatalf("failed to create Aerospike client: %v", err)
	}
	t.Cleanup(func() { asClient.Close() })

	setName := fmt.Sprintf("api_integ_%d", time.Now().UnixNano())
	regStore := store.NewRegistrationStore(asClient, setName, 3, 100, 0, slog.Default())

	srv := api.NewServer(config.APIConfig{Port: 0}, regStore, asClient, slog.Default())

	// Call Init to build the chi router and routes.
	if err := srv.Init(nil); err != nil {
		t.Fatalf("server Init failed: %v", err)
	}

	// Use httptest.NewServer wrapping the router.
	// We access the router via the Init-created HTTP handler by starting the server
	// through httptest which is the simplest approach.
	ts := httptest.NewServer(srv.Router())

	return ts, regStore, asClient
}

func postWatch(t *testing.T, baseURL string, body interface{}) *http.Response {
	t.Helper()
	jsonBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	resp, err := http.Post(baseURL+"/watch", "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		t.Fatalf("POST /watch failed: %v", err)
	}
	return resp
}

func TestAPIIntegration_WatchValidRequest(t *testing.T) {
	ts, regStore, _ := setupTestServer(t)
	defer ts.Close()

	txid := "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	callbackURL := "https://example.com/callback"

	req := map[string]string{
		"txid":        txid,
		"callbackUrl": callbackURL,
	}

	resp := postWatch(t, ts.URL, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var watchResp map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&watchResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if watchResp["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", watchResp["status"])
	}

	// Verify the registration is stored in Aerospike.
	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("regStore.Get failed: %v", err)
	}
	if len(urls) != 1 {
		t.Fatalf("expected 1 callback in store, got %d", len(urls))
	}
	if urls[0] != callbackURL {
		t.Fatalf("expected callback %q, got %q", callbackURL, urls[0])
	}
}

func TestAPIIntegration_WatchDuplicateRegistration(t *testing.T) {
	ts, regStore, _ := setupTestServer(t)
	defer ts.Close()

	txid := "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
	callbackURL := "https://example.com/dup-callback"

	req := map[string]string{
		"txid":        txid,
		"callbackUrl": callbackURL,
	}

	// First registration.
	resp1 := postWatch(t, ts.URL, req)
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first POST expected 200, got %d", resp1.StatusCode)
	}

	// Duplicate registration.
	resp2 := postWatch(t, ts.URL, req)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second POST expected 200, got %d", resp2.StatusCode)
	}

	// Verify idempotency: only 1 callback stored.
	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("regStore.Get failed: %v", err)
	}
	if len(urls) != 1 {
		t.Fatalf("expected 1 callback (idempotent), got %d: %v", len(urls), urls)
	}
}

func TestAPIIntegration_WatchMultipleCallbacksSameTxid(t *testing.T) {
	ts, regStore, _ := setupTestServer(t)
	defer ts.Close()

	txid := "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"
	callbacks := []string{
		"https://example.com/cb-alpha",
		"https://example.com/cb-beta",
		"https://example.com/cb-gamma",
	}

	for _, cb := range callbacks {
		req := map[string]string{
			"txid":        txid,
			"callbackUrl": cb,
		}
		resp := postWatch(t, ts.URL, req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST /watch for %q expected 200, got %d", cb, resp.StatusCode)
		}
	}

	// Verify all callbacks stored.
	urls, err := regStore.Get(txid)
	if err != nil {
		t.Fatalf("regStore.Get failed: %v", err)
	}
	if len(urls) != len(callbacks) {
		t.Fatalf("expected %d callbacks, got %d: %v", len(callbacks), len(urls), urls)
	}

	// The store uses ordered list, so verify sorted order.
	for i, u := range urls {
		if u != callbacks[i] {
			t.Errorf("callback[%d] = %q, want %q", i, u, callbacks[i])
		}
	}
}
