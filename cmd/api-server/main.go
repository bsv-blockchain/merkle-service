package main

import (
	"context"
	"log"

	"github.com/bsv-blockchain/merkle-service/internal/api"
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

	server := api.NewServer(cfg.API, registry.Registration, registry.CallbackURLRegistry, registry.Health, logger)
	server.SetAllowPrivateCallbackIPs(cfg.Callback.AllowPrivateIPs)

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
