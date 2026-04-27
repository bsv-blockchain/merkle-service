package main

import (
	"context"
	"log"

	"github.com/bsv-blockchain/merkle-service/internal/config"
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

	processor := subtree.NewProcessor(cfg, registry.Registration, registry.SeenCounter, registry.Subtree)

	if err := processor.Init(nil); err != nil {
		log.Fatal("failed to init subtree processor: ", err)
	}

	if err := processor.Start(ctx); err != nil {
		log.Fatal("failed to start subtree processor: ", err)
	}

	var base service.BaseService
	base.InitBase("subtree-fetcher")
	base.WaitForShutdown(ctx)

	if err := processor.Stop(); err != nil {
		logger.Error("failed to stop subtree processor", "error", err)
	}
}
