package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

// fakeRegStore is a minimal RegistrationStore stub used by tests. When
// addErr is non-nil, Add returns it (so error-mapping behavior can be
// exercised). Otherwise Add records each (txid, url) pair in `added`.
type fakeRegStore struct {
	addErr error
	added  []struct{ txid, url string }
}

func (f *fakeRegStore) Add(txid, url string) error {
	if f.addErr != nil {
		return f.addErr
	}
	f.added = append(f.added, struct{ txid, url string }{txid, url})
	return nil
}

func (f *fakeRegStore) Get(string) ([]string, error) {
	return nil, nil
}

func (f *fakeRegStore) BatchGet([]string) (map[string][]string, error) {
	return nil, nil
}
func (f *fakeRegStore) UpdateTTL(string, time.Duration) error        { return nil }
func (f *fakeRegStore) BatchUpdateTTL([]string, time.Duration) error { return nil }

func newTestRouterWithRegStore(rs store.RegistrationStore) (*chi.Mux, *Server) {
	router := chi.NewRouter()
	s := &Server{regStore: rs}
	s.InitBase("test")
	router.Get("/", handleDashboard)
	router.Post("/watch", s.handleWatch)
	router.Get("/health", s.handleHealth)
	router.Get("/api/lookup/{txid}", s.handleLookup)
	return router, s
}

func newTestRouter() *chi.Mux {
	router := chi.NewRouter()
	s := &Server{}
	s.InitBase("test")
	router.Get("/", handleDashboard)
	router.Post("/watch", s.handleWatch)
	router.Get("/health", s.handleHealth)
	router.Get("/api/lookup/{txid}", s.handleLookup)
	return router
}

func postWatch(router http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/watch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestHandleWatch_MissingTxID(t *testing.T) {
	router := newTestRouter()
	w := postWatch(router, `{"callbackUrl": "https://example.com/cb"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp ErrorResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != "txid is required" {
		t.Fatalf("expected 'txid is required', got %q", resp.Error)
	}
}

func TestHandleWatch_InvalidTxID(t *testing.T) {
	router := newTestRouter()
	w := postWatch(router, `{"txid": "xyz", "callbackUrl": "https://example.com/cb"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWatch_MissingCallbackURL(t *testing.T) {
	router := newTestRouter()
	w := postWatch(router, `{"txid": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWatch_InvalidCallbackURL(t *testing.T) {
	router := newTestRouter()
	w := postWatch(router, `{"txid": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", "callbackUrl": "not-a-url"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWatch_InvalidBody(t *testing.T) {
	router := newTestRouter()
	w := postWatch(router, `not json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleLookup_InvalidTxID(t *testing.T) {
	router := newTestRouter()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/lookup/invalid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp ErrorResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == "" {
		t.Fatal("expected error message in response")
	}
}

func TestHandleLookup_NoRegStore(t *testing.T) {
	router := newTestRouter()
	txid := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/lookup/"+txid, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleDashboard(t *testing.T) {
	router := newTestRouter()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Fatalf("expected text/html content type, got %q", ct)
	}
	if w.Body.Len() == 0 {
		t.Fatal("expected non-empty body")
	}
}

// TestHandleWatch_MaxCallbacksReturns429 verifies that the /watch endpoint
// translates store.ErrMaxCallbacksPerTxIDExceeded to HTTP 429 with a clear
// JSON error body. F-050 / issue #27.
func TestHandleWatch_MaxCallbacksReturns429(t *testing.T) {
	router, _ := newTestRouterWithRegStore(&fakeRegStore{addErr: store.ErrMaxCallbacksPerTxIDExceeded})
	// IP literal avoids DNS lookup so the test runs in offline/sandbox
	// environments. 1.1.1.1 is public and not on the SSRF deny-list.
	w := postWatch(router, `{"txid":"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","callbackUrl":"https://1.1.1.1/cb"}`)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", w.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected non-empty error message in 429 body")
	}
}

// TestServerHasURLRegistryField verifies the Server struct holds a urlRegistry
// field and that NewServer wires it correctly.
func TestServerHasURLRegistryField(t *testing.T) {
	s := &Server{}
	if s.urlRegistry != nil {
		t.Error("expected nil urlRegistry on zero-value Server")
	}

	// Verify NewServer accepts and stores the registry.
	s2 := NewServer(config.APIConfig{Port: 8080}, nil, nil, nil, nil)
	if s2.urlRegistry != nil {
		t.Error("expected nil urlRegistry when nil passed to NewServer")
	}
}

// TestHandleWatch_RejectsSSRFTargets ensures /watch refuses callback URLs
// pointing at private/loopback/link-local destinations and metadata
// endpoints. Verifies the registration-time SSRF guard for F-008.
func TestHandleWatch_RejectsSSRFTargets(t *testing.T) {
	const txid = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	cases := []struct {
		name string
		url  string
	}{
		{"loopback v4", "http://127.0.0.1/cb"},
		{"loopback v6", "http://[::1]/cb"},
		{"link-local metadata", "http://169.254.169.254/latest/meta-data/"},
		{"rfc1918 10/8", "http://10.0.0.1/cb"},
		{"rfc1918 192.168", "http://192.168.1.1/cb"},
		{"rfc1918 172.16", "http://172.16.0.1/cb"},
		{"link-local v6", "http://[fe80::1]/cb"},
		{"unspecified v4", "http://0.0.0.0/cb"},
		{"metadata.google.internal", "http://metadata.google.internal/computeMetadata/v1/"},
		// URL parser quirk: this parses with hostname=127.0.0.1 (the
		// "@" splits userinfo from host). Userinfo is independently
		// rejected and the resolved hostname is loopback — either path
		// must fail.
		{"userinfo bypass", "https://example.com:80@127.0.0.1/foo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter()
			body := `{"txid": "` + txid + `", "callbackUrl": "` + tc.url + `"}`
			w := postWatch(router, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s (%q), got %d (body=%s)", tc.name, tc.url, w.Code, w.Body.String())
			}
		})
	}
}

// TestHandleWatch_RejectsBadScheme ensures non-http(s) schemes are
// refused. file:, gopher:, etc. are SSRF amplifiers in Go's net/http.
func TestHandleWatch_RejectsBadScheme(t *testing.T) {
	const txid = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	for _, raw := range []string{
		"file:///etc/passwd",
		"gopher://example.com/foo",
		"ftp://example.com/foo",
	} {
		t.Run(raw, func(t *testing.T) {
			router := newTestRouter()
			body := `{"txid": "` + txid + `", "callbackUrl": "` + raw + `"}`
			w := postWatch(router, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d (%s)", w.Code, w.Body.String())
			}
		})
	}
}

// TestHandleWatch_AllowPrivateIPs verifies the operator escape hatch:
// when SetAllowPrivateCallbackIPs(true) is set, private/loopback URLs
// are accepted at registration time.
func TestHandleWatch_AllowPrivateIPs(t *testing.T) {
	router := chi.NewRouter()
	fake := &fakeRegStore{}
	s := NewServer(config.APIConfig{Port: 8080}, fake, nil, nil, nil)
	router.Post("/watch", s.handleWatch)

	// Default deny: same URL is rejected with 400.
	const txid = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	body := `{"txid": "` + txid + `", "callbackUrl": "http://127.0.0.1:9000/cb"}`
	w := postWatch(router, body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("default-deny: expected 400, got %d", w.Code)
	}

	// Opt in: now the same URL is accepted.
	s.SetAllowPrivateCallbackIPs(true)
	w = postWatch(router, body)
	if w.Code != http.StatusOK {
		t.Fatalf("allowPrivate=true: expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if len(fake.added) != 1 || fake.added[0].url != "http://127.0.0.1:9000/cb" {
		t.Fatalf("expected fakeRegStore to record private callback, got %+v", fake.added)
	}
}
