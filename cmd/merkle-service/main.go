package main

import (
	"context"
	"log"

	"github.com/bsv-blockchain/merkle-service/internal/api"
	"github.com/bsv-blockchain/merkle-service/internal/block"
	"github.com/bsv-blockchain/merkle-service/internal/callback"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/p2p"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	_ "github.com/bsv-blockchain/merkle-service/internal/store/sql" // register SQL backend
	"github.com/bsv-blockchain/merkle-service/internal/subtree"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config: ", err)
	}

	logger := service.NewLogger(config.ParseLogLevel(cfg.LogLevel))

	ctx := context.Background()
	registry, err := store.NewFromConfig(ctx, cfg, logger)
	if err != nil {
		log.Fatal("failed to build store registry: ", err)
	}
	defer registry.Close()

	subtreeProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.SubtreeTopic, logger)
	if err != nil {
		log.Fatal("failed to create subtree producer: ", err)
	}
	defer subtreeProducer.Close()

	blockProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.BlockTopic, logger)
	if err != nil {
		log.Fatal("failed to create block producer: ", err)
	}
	defer blockProducer.Close()

	apiServer := api.NewServer(cfg.API, registry.Registration, registry.CallbackURLRegistry, registry.Health, logger)
	p2pClient := p2p.NewClient(cfg.P2P, subtreeProducer, blockProducer, logger)
	subtreeFetcher := subtree.NewProcessor(cfg, registry.Registration, registry.SeenCounter, registry.Subtree)
	blockProcessor := block.NewProcessor(cfg.Kafka, cfg.Block, cfg.DataHub, registry.Registration, registry.Subtree, registry.CallbackURLRegistry, registry.SubtreeCounter, logger)
	subtreeWorker := block.NewSubtreeWorkerService(cfg.Kafka, cfg.Block, cfg.DataHub, registry.Registration, registry.Subtree, registry.Stump, registry.CallbackURLRegistry, registry.SubtreeCounter, logger)
	callbackDelivery := callback.NewDeliveryService(cfg, registry.CallbackDedup, registry.Stump)

	services := []service.Service{apiServer, p2pClient, subtreeFetcher, blockProcessor, subtreeWorker, callbackDelivery}
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
