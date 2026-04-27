package api

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Server implements the API server service.
type Server struct {
	service.BaseService
	cfg         config.APIConfig
	httpServer  *http.Server
	router      chi.Router
	regStore    store.RegistrationStore
	urlRegistry store.CallbackURLRegistry
	health      store.BackendHealth
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

// Router returns the chi router. Must be called after Init.
func (s *Server) Router() chi.Router {
	return s.router
}

func (s *Server) Init(cfg interface{}) error {
	s.router = chi.NewRouter()

	// Middleware
	s.router.Use(middleware.RequestID)
	s.router.Use(middleware.RealIP)
	s.router.Use(middlewareLogger(s.Logger))
	s.router.Use(middleware.Recoverer)

	// Routes
	s.router.Get("/", handleDashboard)
	s.router.Post("/watch", s.handleWatch)
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

			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration", time.Since(start),
				"requestId", middleware.GetReqID(r.Context()),
			)
		})
	}
}
