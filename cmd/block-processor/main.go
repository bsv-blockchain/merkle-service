package main

import (
	"context"
	"log"

	"github.com/bsv-blockchain/merkle-service/internal/block"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	_ "github.com/bsv-blockchain/merkle-service/internal/store/sql" // register SQL backend
	"github.com/bsv-blockchain/merkle-service/internal/version"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config: ", err)
	}

	logger := service.NewLogger(config.ParseLogLevel(cfg.LogLevel))

	ctx := context.Background()

	telemetryShutdown, err := service.InitTelemetry(ctx, cfg.Telemetry, "block-processor", version.Version, metrics.Registry, logger)
	if err != nil {
		log.Fatal("failed to init telemetry: ", err)
	}
	defer func() { _ = telemetryShutdown(context.Background()) }()

	registry, err := store.NewFromConfigWithRetry(ctx, cfg, logger)
	if err != nil {
		log.Fatal("failed to build store registry: ", err)
	}
	defer func() { _ = registry.Close() }()

	processor := block.NewProcessor(
		cfg.Kafka, cfg.Block, cfg.DataHub,
		registry.Registration, registry.Subtree, registry.CallbackURLRegistry, registry.DataHubRegistry, registry.SubtreeCounter,
		logger,
	)

	var metricsSrv *metrics.Server
	if cfg.Metrics.Enabled {
		metricsSrv = metrics.NewServer(cfg.Metrics, logger)
		if err := metricsSrv.Init(nil); err != nil {
			log.Fatal("failed to init metrics server: ", err)
		}
		if err := metricsSrv.Start(ctx); err != nil {
			log.Fatal("failed to start metrics server: ", err)
		}
		defer func() {
			if err := metricsSrv.Stop(); err != nil {
				logger.Error("failed to stop metrics server", "error", err)
			}
		}()
	}

	if err := processor.Init(nil); err != nil {
		log.Fatal("failed to init block processor: ", err)
	}

	if err := processor.Start(ctx); err != nil {
		log.Fatal("failed to start block processor: ", err)
	}

	var base service.BaseService
	base.InitBase("block-processor")
	base.WaitForShutdown(ctx)

	if err := processor.Stop(); err != nil {
		logger.Error("failed to stop block processor", "error", err)
	}
}
