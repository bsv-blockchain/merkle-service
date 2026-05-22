package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestChiMiddleware_RoutePatternLabel asserts the chi route-pattern path is
// used for the route label, NOT the raw request URL. This is the load-bearing
// guarantee that lookups by txid don't explode cardinality.
func TestChiMiddleware_RoutePatternLabel(t *testing.T) {
	r := chi.NewRouter()
	r.Use(ChiMiddleware)
	r.Get("/api/lookup/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(r)
	defer srv.Close()

	// Same route template, three distinct txids — must collapse to one series.
	for _, p := range []string{
		"/api/lookup/aaaa000000000000000000000000000000000000000000000000000000000001",
		"/api/lookup/aaaa000000000000000000000000000000000000000000000000000000000002",
		"/api/lookup/aaaa000000000000000000000000000000000000000000000000000000000003",
	} {
		resp, err := http.Get(srv.URL + p) //nolint:gosec,noctx
		if err != nil {
			t.Fatalf("GET %s: %v", p, err)
		}
		_ = resp.Body.Close()
	}
	resp, err := http.Get(srv.URL + "/health") //nolint:gosec,noctx
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	_ = resp.Body.Close()

	if got := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("/api/lookup/{txid}", "GET", "2xx")); got != 3 {
		t.Errorf("expected 3 hits on /api/lookup/{txid}, got %v", got)
	}
	if got := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("/health", "GET", "2xx")); got != 1 {
		t.Errorf("expected 1 hit on /health, got %v", got)
	}

	// 404 path → "unmatched" route label so probes can't blow up cardinality.
	resp, err = http.Get(srv.URL + "/does-not-exist") //nolint:gosec,noctx
	if err != nil {
		t.Fatalf("GET 404: %v", err)
	}
	_ = resp.Body.Close()
	if got := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("unmatched", "GET", "4xx")); got != 1 {
		t.Errorf("expected 1 hit on unmatched route, got %v", got)
	}
}
