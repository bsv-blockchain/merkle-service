package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// guardedReprocessRouter wires /reprocess with the same per-route middleware
// chain Init uses (auth → rate limit → in-flight cap), driven off cfg. deps==nil
// leaves /reprocess "not configured" so a request that clears the middleware
// reaches the handler and gets a deterministic 503 — a convenient "passed the
// guards" marker distinct from the guards' own 401/429.
func guardedReprocessRouter(t *testing.T, cfg config.APIConfig, deps *ReprocessDeps) (*Server, http.Handler) {
	t.Helper()
	s := &Server{cfg: cfg}
	s.InitBase("test")
	s.Logger = discardLogger()
	if deps != nil {
		s.SetReprocessDeps(deps)
	}
	s.initReprocessGuards()
	router := chi.NewRouter()
	router.With(s.authMiddleware, s.reprocessLimit, s.reprocessInflight).Post("/reprocess", s.handleReprocess)
	return s, router
}

func postReprocessWithToken(router http.Handler, body, bearer string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/reprocess", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func reprocessBody() string {
	return fmt.Sprintf(`{"blockHash":%q,"callbackUrl":"https://1.1.1.1/cb"}`, fixtureBlockHash)
}

// TestReprocessAuth covers the bearer-token gate on /reprocess: fail-open with
// no token configured, and enforce (401) once a token is set.
func TestReprocessAuth(t *testing.T) {
	cases := []struct {
		name    string
		token   string // configured server-side token ("" = auth disabled)
		bearer  string // token presented by the client
		want    int
		wantMsg string
	}{
		// Auth disabled: request passes through to the (unconfigured) handler.
		{"fail-open no token configured", "", "", http.StatusServiceUnavailable, ""},
		{"reject missing bearer", "s3cret", "", http.StatusUnauthorized, "unauthorized"},
		{"reject wrong bearer", "s3cret", "nope", http.StatusUnauthorized, "unauthorized"},
		// Correct token clears the gate; handler is unconfigured -> 503.
		{"accept correct bearer", "s3cret", "s3cret", http.StatusServiceUnavailable, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, router := guardedReprocessRouter(t, config.APIConfig{AuthToken: tc.token}, nil)
			w := postReprocessWithToken(router, reprocessBody(), tc.bearer)
			if w.Code != tc.want {
				t.Fatalf("expected %d, got %d (body=%s)", tc.want, w.Code, w.Body.String())
			}
			if tc.wantMsg != "" {
				var resp ErrorResponse
				_ = json.NewDecoder(w.Body).Decode(&resp)
				if resp.Error != tc.wantMsg {
					t.Fatalf("expected error %q, got %q", tc.wantMsg, resp.Error)
				}
			}
		})
	}
}

// TestReprocessRateLimit verifies the token bucket returns 429 once the burst
// is exhausted. Auth is disabled so requests reach the limiter unconditionally.
func TestReprocessRateLimit(t *testing.T) {
	cfg := config.APIConfig{ReprocessRateLimitRps: 1, ReprocessBurst: 1}
	_, router := guardedReprocessRouter(t, cfg, nil)

	// First request consumes the single burst token (handler is unconfigured
	// -> 503, but the limiter admitted it).
	if w := postReprocessWithToken(router, reprocessBody(), ""); w.Code == http.StatusTooManyRequests {
		t.Fatalf("first request should be admitted, got 429")
	}
	// Second request within the same second is rejected.
	w := postReprocessWithToken(router, reprocessBody(), "")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on second request, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// TestReprocessInflightCap verifies the concurrency semaphore returns 429 when
// the cap is saturated, and admits again once a slot frees.
func TestReprocessInflightCap(t *testing.T) {
	cfg := config.APIConfig{MaxInflightReprocess: 1}
	s, router := guardedReprocessRouter(t, cfg, nil)

	// Saturate the single slot as if a request were in flight.
	s.reprocessSem <- struct{}{}
	w := postReprocessWithToken(router, reprocessBody(), "")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 while in-flight cap saturated, got %d", w.Code)
	}

	// Free the slot; the next request is admitted (handler unconfigured -> 503).
	<-s.reprocessSem
	w = postReprocessWithToken(router, reprocessBody(), "")
	if w.Code == http.StatusTooManyRequests {
		t.Fatalf("expected admission after slot freed, still got 429")
	}
}

// TestWatchAuthOptIn verifies /watch is unauthenticated by default and only
// enforces the bearer token when api.requireWatchAuth is set. A missing-txid
// body yields 400 once auth is cleared, distinguishing "passed auth" from 401.
func TestWatchAuthOptIn(t *testing.T) {
	newRouter := func(cfg config.APIConfig) http.Handler {
		s := &Server{cfg: cfg, regStore: &fakeRegStore{}}
		s.InitBase("test")
		s.Logger = discardLogger()
		router := chi.NewRouter()
		if cfg.RequireWatchAuth {
			router.With(s.authMiddleware).Post("/watch", s.handleWatch)
		} else {
			router.Post("/watch", s.handleWatch)
		}
		return router
	}
	post := func(router http.Handler, bearer string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/watch", bytes.NewBufferString(`{"callbackUrl":"https://example.com/cb"}`))
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	// Default: no auth on /watch -> handler runs -> 400 (missing txid).
	if w := post(newRouter(config.APIConfig{}), ""); w.Code != http.StatusBadRequest {
		t.Fatalf("default /watch: expected 400 (unauthenticated pass-through), got %d", w.Code)
	}
	// Opt-in, missing bearer -> 401.
	optIn := config.APIConfig{AuthToken: "s3cret", RequireWatchAuth: true}
	if w := post(newRouter(optIn), ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("opt-in /watch without token: expected 401, got %d", w.Code)
	}
	// Opt-in, correct bearer -> auth cleared -> 400 (missing txid).
	if w := post(newRouter(optIn), "s3cret"); w.Code != http.StatusBadRequest {
		t.Fatalf("opt-in /watch with token: expected 400 (auth cleared), got %d", w.Code)
	}
}
