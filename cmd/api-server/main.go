package main

import (
	"context"
	"log"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/api"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
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

	telemetryShutdown, err := service.InitTelemetry(ctx, cfg.Telemetry, "api-server", version.Version, metrics.Registry, logger)
	if err != nil {
		log.Fatal("failed to init telemetry: ", err)
	}
	defer func() { _ = telemetryShutdown(context.Background()) }()

	registry, err := store.NewFromConfig(ctx, cfg, logger)
	if err != nil {
		log.Fatal("failed to build store registry: ", err)
	}
	defer func() { _ = registry.Close() }()

	server := api.NewServer(cfg.API, registry.Registration, registry.CallbackURLRegistry, registry.Health, logger)
	server.SetAllowPrivateCallbackIPs(cfg.Callback.AllowPrivateIPs)
	server.SetBackend(cfg.Store.Backend)

	var metricsSrv *metrics.Server
	if cfg.Metrics.Enabled {
		metricsSrv = metrics.NewServer(cfg.Metrics, logger)
		if initErr := metricsSrv.Init(nil); initErr != nil {
			log.Fatal("failed to init metrics server: ", initErr)
		}
		if startErr := metricsSrv.Start(ctx); startErr != nil {
			log.Fatal("failed to start metrics server: ", startErr)
		}
		defer func() {
			if stopErr := metricsSrv.Stop(); stopErr != nil {
				logger.Error("failed to stop metrics server", "error", stopErr)
			}
		}()
	}

	// /reprocess deps. The DataHub client honors the same SSRF posture as the
	// block-processor so /reprocess can't be coerced into probing
	// loopback/RFC1918 addresses unless the operator opted in via
	// datahub.allowPrivateIPs.
	blockProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.BlockTopic, logger)
	if err != nil {
		log.Fatal("failed to create block producer: ", err)
	}
	defer func() { _ = blockProducer.Close() }()

	dataHubClient := datahub.NewClientWithSSRFGuard(
		cfg.DataHub.TimeoutSec,
		cfg.DataHub.MaxRetries,
		cfg.DataHub.MaxBlockBytes,
		cfg.DataHub.MaxSubtreeBytes,
		cfg.DataHub.AllowPrivateIPs,
		logger,
	)
	dataHubClient.SetPeerHealth(datahub.NewPeerHealth(
		cfg.DataHub.PeerHealth.FailureThreshold,
		time.Duration(cfg.DataHub.PeerHealth.CooldownSec)*time.Second,
	))
	server.SetReprocessDeps(&api.ReprocessDeps{
		DataHubRegistry:     registry.DataHubRegistry,
		DataHubClient:       dataHubClient,
		BlockProducer:       blockProducer,
		FallbackDataHubURLs: cfg.DataHub.FallbackURLs,
		DedupStore:          registry.CallbackDedup,
	})

	if err := server.Init(nil); err != nil {
		log.Fatal("failed to init api server: ", err)
	}

	if err := server.Start(ctx); err != nil {
		log.Fatal("failed to start api server: ", err)
	}

	var base service.BaseService
	base.InitBase("api-server")
	base.WaitForShutdown(ctx)

	if err := server.Stop(); err != nil {
		logger.Error("failed to stop api server", "error", err)
	}
}
