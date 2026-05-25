package api

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Server implements the API server service.
type Server struct {
	service.BaseService

	cfg             config.APIConfig
	httpServer      *http.Server
	router          chi.Router
	regStore        store.RegistrationStore
	urlRegistry     store.CallbackURLRegistry
	dataHubRegistry store.DataHubRegistry
	dataHubClient   *datahub.Client
	blockProducer   reprocessBlockProducer
	// dedupStore, when non-nil, lets /reprocess clear stale callback-dedup
	// entries for the target (blockHash, callbackURL) so that
	// freshly-emitted STUMP and BLOCK_PROCESSED callbacks from a
	// reprocess aren't suppressed by entries left behind by a prior
	// delivery that exhausted its retry chain and went to DLQ
	// (bsv-blockchain/merkle-service#122).
	dedupStore store.CallbackDedupStore
	// backend is the configured store backend label ("aerospike"/"sql")
	// used to tag DB metrics emitted from handlers.
	backend string
	// fallbackDataHubURLs are operator-configured DataHubs probed by
	// /reprocess before the dynamically-discovered ones.
	fallbackDataHubURLs []string
	health              store.BackendHealth
	// allowPrivateCallbackIPs, when true, lets /watch accept callback
	// URLs that resolve to loopback/link-local/RFC1918 addresses.
	// Defaults to false (deny). See SetAllowPrivateCallbackIPs and
	// CallbackConfig.AllowPrivateIPs.
	allowPrivateCallbackIPs bool
}

// reprocessBlockProducer is the minimal kafka.Producer surface the /reprocess
// handler needs. Defined as an interface so unit tests can substitute a
// no-network producer without touching Sarama.
type reprocessBlockProducer interface {
	PublishWithHashKey(key string, value []byte) error
}

// ReprocessDeps bundles the dependencies the /reprocess endpoint needs that
// /watch does not. Bundled in their own struct so existing callers of
// NewServer keep their signature; pass nil to disable /reprocess.
type ReprocessDeps struct {
	DataHubRegistry     store.DataHubRegistry
	DataHubClient       *datahub.Client
	BlockProducer       *kafka.Producer
	FallbackDataHubURLs []string
	// DedupStore wires the callback-dedup store so /reprocess can clear
	// stale dedup entries for the (blockHash, callbackURL) pair before
	// publishing. Optional — when nil, /reprocess proceeds without
	// clearing (legacy behavior).
	DedupStore store.CallbackDedupStore
}

func NewServer(cfg config.APIConfig, regStore store.RegistrationStore, urlRegistry store.CallbackURLRegistry, health store.BackendHealth, logger *slog.Logger) *Server {
	s := &Server{
		cfg:         cfg,
		regStore:    regStore,
		urlRegistry: urlRegistry,
		health:      health,
	}
	s.InitBase("api-server")
	if logger != nil {
		s.Logger = logger
	}
	return s
}

// SetReprocessDeps wires the dependencies needed for the /reprocess endpoint.
// Must be called before Init for the route to be registered. Passing nil or
// any-nil leaves /reprocess disabled (the route returns 503 at request time).
func (s *Server) SetReprocessDeps(deps *ReprocessDeps) {
	if deps == nil {
		return
	}
	s.dataHubRegistry = deps.DataHubRegistry
	s.dataHubClient = deps.DataHubClient
	if deps.BlockProducer != nil {
		s.blockProducer = deps.BlockProducer
	}
	s.fallbackDataHubURLs = deps.FallbackDataHubURLs
	s.dedupStore = deps.DedupStore
}

// SetBackend records the store-backend label for DB metrics emitted from
// API handlers ("aerospike" or "sql").
func (s *Server) SetBackend(backend string) {
	if backend == config.BackendSQL {
		s.backend = metrics.BackendSQL
	} else {
		s.backend = metrics.BackendAerospike
	}
}

// backendLabel returns the configured store-backend label. Defaults to
// aerospike when SetBackend hasn't been called.
func (s *Server) backendLabel() string {
	if s.backend == "" {
		return metrics.BackendAerospike
	}
	return s.backend
}

// SetAllowPrivateCallbackIPs toggles the SSRF guard on /watch. When false
// (the default) callback URLs resolving to private/loopback/link-local
// destinations are rejected at registration time. Wire this from
// CallbackConfig.AllowPrivateIPs at startup.
func (s *Server) SetAllowPrivateCallbackIPs(allow bool) {
	s.allowPrivateCallbackIPs = allow
	if allow && s.Logger != nil {
		s.Logger.Warn(
			"SSRF guard relaxed: /watch will accept callback URLs pointing at private/loopback/link-local addresses",
			"setting", "callback.allowPrivateIPs",
		)
	}
}

// Router returns the chi router. Must be called after Init.
func (s *Server) Router() chi.Router {
	return s.router
}

func (s *Server) Init(cfg interface{}) error {
	s.router = chi.NewRouter()

	// Middleware
	s.router.Use(middleware.RequestID)
	s.router.Use(middlewareLogger(s.Logger))
	s.router.Use(metrics.ChiMiddleware)
	s.router.Use(middleware.Recoverer)

	// Routes
	s.router.Get("/", handleDashboard)
	s.router.Post("/watch", s.handleWatch)
	s.router.Post("/reprocess", s.handleReprocess)
	s.router.Get("/health", s.handleHealth)
	s.router.Get("/api/lookup/{txid}", s.handleLookup)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.cfg.Port),
		Handler:      s.router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.SetStarted(true)
	s.Logger.Info("starting API server", "port", s.cfg.Port)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.Logger.Error("HTTP server error", "error", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	s.Logger.Info("stopping API server")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.SetStarted(false)
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Health() service.HealthStatus {
	status := "healthy"
	details := map[string]string{}

	if s.health != nil && !s.health.Healthy() {
		status = "unhealthy"
		details["backend"] = "disconnected"
	} else {
		details["backend"] = "connected"
	}

	return service.HealthStatus{
		Name:    "api-server",
		Status:  status,
		Details: details,
	}
}

// middlewareLogger creates a chi middleware that logs requests using slog.
func middlewareLogger(logger *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			logger.Info(
				"request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration", time.Since(start),
				"requestId", middleware.GetReqID(r.Context()),
			)
		})
	}
}
