package main

import (
	"context"
	"log"

	"github.com/bsv-blockchain/merkle-service/internal/block"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	_ "github.com/bsv-blockchain/merkle-service/internal/store/sql" // register SQL backend
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

	worker := block.NewSubtreeWorkerService(
		cfg.Kafka, cfg.Block, cfg.DataHub,
		registry.Registration, registry.Subtree, registry.Stump, registry.CallbackURLRegistry, registry.SubtreeCounter,
		logger,
	)

	if err := worker.Init(nil); err != nil {
		log.Fatal("failed to init subtree worker: ", err)
	}

	if err := worker.Start(ctx); err != nil {
		log.Fatal("failed to start subtree worker: ", err)
	}

	var base service.BaseService
	base.InitBase("subtree-worker")
	base.WaitForShutdown(ctx)

	if err := worker.Stop(); err != nil {
		logger.Error("failed to stop subtree worker", "error", err)
	}
}
