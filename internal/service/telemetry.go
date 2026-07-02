package service

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/telemetry"
)

// InitTelemetry wires OpenTelemetry per cfg for a single binary/service mode
// and returns telemetry.Init's shutdown func (already bounded internally by
// cfg.ExportTimeoutMs, so callers can simply `defer shutdown(context.Background())`).
//
// This is a thin wrapper that exists only so each of the cmd/* mains doesn't
// repeat an identical telemetry.Options literal; gatherer should always be
// metrics.Registry (merkle-service's private Prometheus registry — see
// telemetry.Options.Gatherer for why the default gatherer would silently
// export nothing).
func InitTelemetry(ctx context.Context, cfg config.TelemetryConfig, mode, version string, gatherer prometheus.Gatherer, logger *slog.Logger) (func(context.Context) error, error) {
	return telemetry.Init(ctx, cfg, telemetry.Options{
		Mode:     mode,
		Version:  version,
		Gatherer: gatherer,
	}, logger)
}
