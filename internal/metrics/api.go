package metrics

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
)

// HTTP RED metrics for the API server. Route labels are sourced from
// chi.RouteContext(...).RoutePattern(), never r.URL.Path, so /api/lookup/{txid}
// stays as one series instead of one per txid.

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_http_requests_total",
			Help: "Total HTTP requests served by the API server.",
		},
		[]string{labelRoute, labelMethod, labelStatus},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: HTTPBuckets,
		},
		[]string{labelRoute, labelMethod, labelStatus},
	)

	httpRequestsInFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "merkle_http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		},
		[]string{labelRoute, labelMethod},
	)

	httpRequestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_http_request_size_bytes",
			Help:    "HTTP request body size in bytes.",
			Buckets: MsgSizeBuckets,
		},
		[]string{labelRoute, labelMethod},
	)

	httpResponseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_http_response_size_bytes",
			Help:    "HTTP response body size in bytes.",
			Buckets: MsgSizeBuckets,
		},
		[]string{labelRoute, labelMethod},
	)
)

func init() {
	Registry.MustRegister(
		httpRequestsTotal,
		httpRequestDuration,
		httpRequestsInFlight,
		httpRequestSize,
		httpResponseSize,
	)
}

// ChiMiddleware records RED metrics for every HTTP request. Insert it
// before middleware.Recoverer in the chain so it sees the final response
// status (Recoverer turns panics into 500s).
//
// The route label uses chi.RouteContext(ctx).RoutePattern() captured AFTER
// the inner handler runs — chi sets the matched pattern during routing.
// When the request doesn't match any registered route (404 from chi), we
// label it "unmatched" to keep cardinality bounded.
func ChiMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// In-flight gauge uses the pre-routing label "in_progress" because
		// the route pattern isn't known yet — we count by method only.
		// Increment with route="" and resolve after; cardinality of the
		// transient state is bounded by the worker pool.
		inflightLabel := "in_progress"
		method := r.Method
		httpRequestsInFlight.WithLabelValues(inflightLabel, method).Inc()
		defer httpRequestsInFlight.WithLabelValues(inflightLabel, method).Dec()

		start := time.Now()
		ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := routePattern(r)
		status := StatusClass(ww.Status(), nil)
		duration := time.Since(start).Seconds()

		httpRequestsTotal.WithLabelValues(route, method, status).Inc()
		httpRequestDuration.WithLabelValues(route, method, status).Observe(duration)
		if r.ContentLength > 0 {
			httpRequestSize.WithLabelValues(route, method).Observe(float64(r.ContentLength))
		}
		if n := ww.BytesWritten(); n > 0 {
			httpResponseSize.WithLabelValues(route, method).Observe(float64(n))
		}
	})
}

// routePattern extracts the chi route template (e.g. "/api/lookup/{txid}")
// for use as a bounded route label. Falls back to "unmatched" when chi
// couldn't resolve the path so a flood of 404 probes doesn't explode
// cardinality.
func routePattern(r *http.Request) string {
	if ctx := chi.RouteContext(r.Context()); ctx != nil {
		if p := ctx.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}
