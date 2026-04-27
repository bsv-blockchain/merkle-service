package main

import (
	"context"
	"log"

	"github.com/bsv-blockchain/merkle-service/internal/callback"
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

	deliverySvc := callback.NewDeliveryService(cfg, registry.CallbackDedup, registry.Stump)

	if err := deliverySvc.Init(nil); err != nil {
		log.Fatal("failed to init callback delivery service: ", err)
	}

	if err := deliverySvc.Start(ctx); err != nil {
		log.Fatal("failed to start callback delivery service: ", err)
	}

	var base service.BaseService
	base.InitBase("callback-delivery")
	base.WaitForShutdown(ctx)

	if err := deliverySvc.Stop(); err != nil {
		logger.Error("failed to stop callback delivery service", "error", err)
	}
}
