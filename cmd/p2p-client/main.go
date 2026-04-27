package main

import (
	"context"
	"log"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/p2p"
	"github.com/bsv-blockchain/merkle-service/internal/service"
)

func main() {
	// Load configuration.
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config: ", err)
	}

	logger := service.NewLogger(config.ParseLogLevel(cfg.LogLevel))

	// Create Kafka producers for subtree and block topics.
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

	// Create, init, and start the P2P client.
	client := p2p.NewClient(cfg.P2P, subtreeProducer, blockProducer, logger)

	if err := client.Init(nil); err != nil {
		log.Fatal("failed to init p2p client: ", err)
	}

	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		log.Fatal("failed to start p2p client: ", err)
	}

	// Wait for shutdown signal.
	var base service.BaseService
	base.InitBase("p2p-client")
	base.WaitForShutdown(ctx)

	if err := client.Stop(); err != nil {
		logger.Error("failed to stop p2p client", "error", err)
	}
}
