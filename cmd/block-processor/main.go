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

	processor := block.NewProcessor(
		cfg.Kafka, cfg.Block, cfg.DataHub,
		registry.Registration, registry.Subtree, registry.CallbackURLRegistry, registry.SubtreeCounter,
		logger,
	)

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
