package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/service"
)

// Server exposes the Prometheus /metrics endpoint and a /health probe.
// Implements service.Service so it slots into the existing lifecycle.
type Server struct {
	service.BaseService

	cfg        config.MetricsConfig
	httpServer *http.Server
}

// NewServer constructs a metrics Server. The server is a no-op when
// cfg.Enabled is false so callers can wire it into the services slice
// unconditionally.
func NewServer(cfg config.MetricsConfig, logger *slog.Logger) *Server {
	s := &Server{cfg: cfg}
	s.InitBase("metrics")
	if logger != nil {
		s.Logger = logger
	}
	return s
}

// Init prepares the HTTP server. Safe to call when Enabled=false — it
// simply returns nil without configuring anything; Start is a no-op too.
func (s *Server) Init(_ interface{}) error {
	if !s.cfg.Enabled {
		return nil
	}
	path := s.cfg.Path
	if path == "" {
		path = "/metrics"
	}

	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return nil
}

// Start runs the HTTP server in a background goroutine.
func (s *Server) Start(_ context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}
	s.SetStarted(true)
	s.Logger.Info("starting metrics server", "port", s.cfg.Port, "path", s.cfg.Path)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.Logger.Error("metrics server error", "error", err)
		}
	}()
	return nil
}

// Stop shuts the metrics server down with a short grace window.
func (s *Server) Stop() error {
	if !s.cfg.Enabled || s.httpServer == nil {
		return nil
	}
	s.Logger.Info("stopping metrics server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.SetStarted(false)
	return s.httpServer.Shutdown(ctx)
}

// Health returns the metrics server's health status.
func (s *Server) Health() service.HealthStatus {
	status := "healthy"
	if s.cfg.Enabled && !s.IsStarted() {
		status = "unhealthy"
	}
	return service.HealthStatus{
		Name:   "metrics",
		Status: status,
	}
}
