package main

import (
	"encoding/json"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestHandleCallbackReceive_ValidPayload(t *testing.T) {
	store := NewCallbackStore(100)
	h := &Handlers{
		callbackStore: store,
		logger:        testLogger(),
	}

	body := `{"txid":"abc123","status":"SEEN_ON_NETWORK"}`
	req := httptest.NewRequest(http.MethodPost, "/callbacks/receive", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.handleCallbackReceive(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	entries := store.GetAll()
	if len(entries) != 1 {
		t.Fatalf("expected 1 stored callback, got %d", len(entries))
	}
	if entries[0].RawJSON != body {
		t.Errorf("expected stored body %q, got %q", body, entries[0].RawJSON)
	}
	if entries[0].Body["status"] != "SEEN_ON_NETWORK" {
		t.Errorf("expected status SEEN_ON_NETWORK, got %v", entries[0].Body["status"])
	}
}

func testTemplates() map[string]*template.Template {
	templates, err := parseTemplates()
	if err != nil {
		panic("failed to parse test templates: " + err.Error())
	}
	return templates
}

func TestHandleRegister_CustomCallbackURL(t *testing.T) {
	// Mock merkle-service that records the request.
	var receivedPayload map[string]string
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &receivedPayload)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer mockAPI.Close()

	h := &Handlers{
		callbackStore: NewCallbackStore(100),
		txidTracker:   NewTxidTracker(),
		templates:     testTemplates(),
		merkleAPIURL:  mockAPI.URL,
		callbackURL:   "http://localhost:9900/callbacks/receive",
		logger:        testLogger(),
	}

	txid := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	customURL := "http://my-arcade:8080/callback"

	form := url.Values{}
	form.Set("txid", txid)
	form.Set("callbackUrl", customURL)
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	h.handleRegister(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if receivedPayload["callbackUrl"] != customURL {
		t.Errorf("expected callbackUrl %q sent to merkle-service, got %q", customURL, receivedPayload["callbackUrl"])
	}
	tracked := h.txidTracker.GetAll()
	if len(tracked) != 1 {
		t.Fatalf("expected 1 tracked txid, got %d", len(tracked))
	}
	if tracked[0].CallbackURLs[0] != customURL {
		t.Errorf("expected tracked callback URL %q, got %q", customURL, tracked[0].CallbackURLs[0])
	}
}

func TestHandleRegister_EmptyCallbackURL(t *testing.T) {
	apiCalled := false
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer mockAPI.Close()

	h := &Handlers{
		callbackStore: NewCallbackStore(100),
		txidTracker:   NewTxidTracker(),
		templates:     testTemplates(),
		merkleAPIURL:  mockAPI.URL,
		callbackURL:   "http://localhost:9900/callbacks/receive",
		logger:        testLogger(),
	}

	txid := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	form := url.Values{}
	form.Set("txid", txid)
	form.Set("callbackUrl", "")
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	h.handleRegister(w, req)

	if apiCalled {
		t.Error("expected merkle-service API to NOT be called for empty callback URL")
	}
	if h.txidTracker.Count() != 0 {
		t.Error("expected no txids tracked for empty callback URL")
	}
}

func TestHandleRegister_DefaultCallbackURL(t *testing.T) {
	var receivedPayload map[string]string
	mockAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &receivedPayload)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer mockAPI.Close()

	defaultURL := "http://localhost:9900/callbacks/receive"
	h := &Handlers{
		callbackStore: NewCallbackStore(100),
		txidTracker:   NewTxidTracker(),
		templates:     testTemplates(),
		merkleAPIURL:  mockAPI.URL,
		callbackURL:   defaultURL,
		logger:        testLogger(),
	}

	txid := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	form := url.Values{}
	form.Set("txid", txid)
	form.Set("callbackUrl", defaultURL)
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	h.handleRegister(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if receivedPayload["callbackUrl"] != defaultURL {
		t.Errorf("expected default callbackUrl %q, got %q", defaultURL, receivedPayload["callbackUrl"])
	}
}

func TestHandleStump_Returns200(t *testing.T) {
	h := &Handlers{
		callbackStore: NewCallbackStore(100),
		txidTracker:   NewTxidTracker(),
		templates:     testTemplates(),
		logger:        testLogger(),
	}

	req := httptest.NewRequest(http.MethodGet, "/stump", nil)
	w := httptest.NewRecorder()

	h.handleStump(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "stump-input") {
		t.Error("expected page to contain textarea with id 'stump-input'")
	}
	if !strings.Contains(body, "decode-btn") {
		t.Error("expected page to contain decode button with id 'decode-btn'")
	}
	if !strings.Contains(body, "STUMP / BUMP Visualizer") {
		t.Error("expected page to contain 'STUMP / BUMP Visualizer' heading")
	}
}

func TestParseTemplates_IncludesStump(t *testing.T) {
	templates := testTemplates()
	if _, ok := templates["stump.html"]; !ok {
		t.Error("expected stump.html to be in parsed templates")
	}
}

func TestTxidValidation(t *testing.T) {
	tests := []struct {
		txid  string
		valid bool
	}{
		{"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", true},
		{"AABB00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDD", true},
		{"short", false},
		{"g1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", false}, // 'g' not hex
		{"", false},
	}

	for _, tt := range tests {
		result := txidRegex.MatchString(tt.txid)
		if result != tt.valid {
			t.Errorf("txidRegex(%q) = %v, want %v", tt.txid, result, tt.valid)
		}
	}
}
