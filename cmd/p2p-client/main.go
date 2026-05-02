package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/p2p"
	"github.com/bsv-blockchain/merkle-service/internal/service"
)

// exit is overridable so tests can assert on the status code without
// terminating the test process.
var exit = os.Exit

func main() {
	if err := run(); err != nil {
		log.Printf("p2p-client terminating with error: %v", err)
		exit(1)
		return
	}
	exit(0)
}

func run() error {
	// Load configuration.
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := service.NewLogger(config.ParseLogLevel(cfg.LogLevel))

	// Create Kafka producers for subtree and block topics.
	subtreeProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.SubtreeTopic, logger)
	if err != nil {
		return err
	}
	defer func() { _ = subtreeProducer.Close() }()

	blockProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.BlockTopic, logger)
	if err != nil {
		return err
	}
	defer func() { _ = blockProducer.Close() }()

	// Create, init, and start the P2P client.
	client := p2p.NewClient(cfg.P2P, subtreeProducer, blockProducer, logger)

	if err := client.Init(nil); err != nil {
		return err
	}

	// Translate SIGTERM/SIGINT into a context cancel so the supervisor can
	// shut us down cleanly. A terminal error from Run (e.g. exhausted Kafka
	// publish retries) is propagated up so the process exits non-zero and
	// the orchestrator (k8s/Docker/systemd) restarts the pod from a fresh
	// P2P session.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	runErr := client.Run(ctx)
	if stopErr := client.Stop(); stopErr != nil {
		logger.Error("failed to stop p2p client", "error", stopErr)
	}
	return runErr
}
