package main

import (
	"context"
	"log"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/api"
	"github.com/bsv-blockchain/merkle-service/internal/block"
	"github.com/bsv-blockchain/merkle-service/internal/callback"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/p2p"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	_ "github.com/bsv-blockchain/merkle-service/internal/store/sql" // register SQL backend
	"github.com/bsv-blockchain/merkle-service/internal/subtree"
	"github.com/bsv-blockchain/merkle-service/internal/version"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config: ", err)
	}

	logger := service.NewLogger(config.ParseLogLevel(cfg.LogLevel))

	ctx := context.Background()

	telemetryShutdown, err := service.InitTelemetry(ctx, cfg.Telemetry, "all-in-one", version.Version, metrics.Registry, logger)
	if err != nil {
		log.Fatal("failed to init telemetry: ", err)
	}
	defer func() { _ = telemetryShutdown(context.Background()) }()

	registry, err := store.NewFromConfig(ctx, cfg, logger)
	if err != nil {
		log.Fatal("failed to build store registry: ", err)
	}
	defer func() { _ = registry.Close() }()

	subtreeProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.SubtreeTopic, logger)
	if err != nil {
		log.Fatal("failed to create subtree producer: ", err)
	}
	defer func() { _ = subtreeProducer.Close() }()

	blockProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.BlockTopic, logger)
	if err != nil {
		log.Fatal("failed to create block producer: ", err)
	}
	defer func() { _ = blockProducer.Close() }()

	apiServer := api.NewServer(cfg.API, registry.Registration, registry.CallbackURLRegistry, registry.Health, logger)
	apiServer.SetAllowPrivateCallbackIPs(cfg.Callback.AllowPrivateIPs)
	apiServer.SetBackend(cfg.Store.Backend)
	apiDataHubClient := datahub.NewClientWithSSRFGuard(
		cfg.DataHub.TimeoutSec,
		cfg.DataHub.MaxRetries,
		cfg.DataHub.MaxBlockBytes,
		cfg.DataHub.MaxSubtreeBytes,
		cfg.DataHub.AllowPrivateIPs,
		logger,
	)
	apiDataHubClient.SetPeerHealth(datahub.NewPeerHealth(
		cfg.DataHub.PeerHealth.FailureThreshold,
		time.Duration(cfg.DataHub.PeerHealth.CooldownSec)*time.Second,
	))
	apiServer.SetReprocessDeps(&api.ReprocessDeps{
		DataHubRegistry:     registry.DataHubRegistry,
		DataHubClient:       apiDataHubClient,
		BlockProducer:       blockProducer,
		FallbackDataHubURLs: cfg.DataHub.FallbackURLs,
		DedupStore:          registry.CallbackDedup,
	})
	p2pClient := p2p.NewClient(
		cfg.P2P,
		subtreeProducer,
		blockProducer,
		registry.DataHubRegistry,
		cfg.DataHub.AllowPrivateIPs,
		logger,
	)
	subtreeFetcher := subtree.NewProcessor(cfg, registry.Registration, registry.SeenCounter, registry.Subtree, logger)
	blockProcessor := block.NewProcessor(cfg.Kafka, cfg.Block, cfg.DataHub, registry.Registration, registry.Subtree, registry.CallbackURLRegistry, registry.DataHubRegistry, registry.SubtreeCounter, logger)
	subtreeWorker := block.NewSubtreeWorkerService(cfg.Kafka, cfg.Block, cfg.DataHub, registry.Registration, registry.Subtree, registry.Stump, registry.CallbackURLRegistry, registry.SubtreeCounter, registry.ExpectedStump, logger)
	callbackDelivery := callback.NewDeliveryService(cfg, registry.CallbackDedup, registry.Stump, registry.CallbackURLRegistry, logger)

	services := []service.Service{}
	if cfg.Metrics.Enabled {
		services = append(services, metrics.NewServer(cfg.Metrics, logger))
	}
	services = append(services, apiServer, p2pClient, subtreeFetcher, blockProcessor, subtreeWorker, callbackDelivery)
	for _, svc := range services {
		if err := svc.Init(nil); err != nil {
			log.Fatal("failed to init service: ", err)
		}
	}

	for _, svc := range services {
		if err := svc.Start(ctx); err != nil {
			log.Fatal("failed to start service: ", err)
		}
	}

	var base service.BaseService
	base.InitBase("merkle-service")
	base.WaitForShutdown(ctx)

	for i := len(services) - 1; i >= 0; i-- {
		if err := services[i].Stop(); err != nil {
			logger.Error("failed to stop service", "error", err)
		}
	}
}
