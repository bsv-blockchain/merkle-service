package service

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/bsv-blockchain/merkle-service/internal/logfields"
)

// BaseService provides common lifecycle logic for all services.
type BaseService struct {
	Name    string
	Logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
}

// NewLogger creates a JSON slog.Logger at the given level writing to stdout.
//
// The JSON handler is wrapped in logfields.NewTraceHandler so every logger
// derived from the entrypoint gains trace_id/span_id correlation on log
// calls made through a *Context method (InfoContext, ErrorContext, ...) when
// the call's context carries a valid OTEL span. This is a no-op wrapper when
// telemetry is disabled or the context carries no span.
func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(logfields.NewTraceHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))
}

// InitBase sets up the logger and context for the service.
// If a logger has already been set (e.g. by the entrypoint), it is preserved.
func (b *BaseService) InitBase(name string) {
	b.Name = name
	if b.Logger == nil {
		b.Logger = NewLogger(slog.LevelInfo).With("service", name)
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.started.Store(false)
}

// Context returns the service's context.
func (b *BaseService) Context() context.Context {
	return b.ctx
}

// Cancel cancels the service's context.
func (b *BaseService) Cancel() {
	if b.cancel != nil {
		b.cancel()
	}
}

// IsStarted returns whether the service has been started.
func (b *BaseService) IsStarted() bool {
	return b.started.Load()
}

// SetStarted sets the started state of the service.
func (b *BaseService) SetStarted(started bool) {
	b.started.Store(started)
}

// WaitForShutdown blocks until a SIGTERM or SIGINT signal is received,
// or until the provided context is canceled.
func (b *BaseService) WaitForShutdown(ctx context.Context) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		b.Logger.Info("received shutdown signal", "signal", sig.String())
	case <-ctx.Done():
		b.Logger.Info("context canceled, shutting down")
	}

	signal.Stop(sigCh)
	b.Cancel()
}
