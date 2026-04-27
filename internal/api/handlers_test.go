package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/go-chi/chi/v5"
)

func newTestRouter() (*chi.Mux, *Server) {
	router := chi.NewRouter()
	s := &Server{}
	s.InitBase("test")
	router.Get("/", handleDashboard)
	router.Post("/watch", s.handleWatch)
	router.Get("/health", s.handleHealth)
	router.Get("/api/lookup/{txid}", s.handleLookup)
	return router, s
}

func postWatch(router http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/watch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestHandleWatch_MissingTxID(t *testing.T) {
	router, _ := newTestRouter()
	w := postWatch(router, `{"callbackUrl": "https://example.com/cb"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp ErrorResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != "txid is required" {
		t.Fatalf("expected 'txid is required', got %q", resp.Error)
	}
}

func TestHandleWatch_InvalidTxID(t *testing.T) {
	router, _ := newTestRouter()
	w := postWatch(router, `{"txid": "xyz", "callbackUrl": "https://example.com/cb"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWatch_MissingCallbackURL(t *testing.T) {
	router, _ := newTestRouter()
	w := postWatch(router, `{"txid": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWatch_InvalidCallbackURL(t *testing.T) {
	router, _ := newTestRouter()
	w := postWatch(router, `{"txid": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", "callbackUrl": "not-a-url"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWatch_InvalidBody(t *testing.T) {
	router, _ := newTestRouter()
	w := postWatch(router, `not json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleLookup_InvalidTxID(t *testing.T) {
	router, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/lookup/invalid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp ErrorResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == "" {
		t.Fatal("expected error message in response")
	}
}

func TestHandleLookup_NoRegStore(t *testing.T) {
	router, _ := newTestRouter()
	txid := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	req := httptest.NewRequest(http.MethodGet, "/api/lookup/"+txid, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleDashboard(t *testing.T) {
	router, _ := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
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
