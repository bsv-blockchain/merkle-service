package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/p2p"
	"github.com/bsv-blockchain/merkle-service/internal/service"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	_ "github.com/bsv-blockchain/merkle-service/internal/store/sql" // register SQL backend
	"github.com/bsv-blockchain/merkle-service/internal/version"
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

	telemetryShutdown, err := service.InitTelemetry(context.Background(), cfg.Telemetry, "p2p-client", version.Version, metrics.Registry, logger)
	if err != nil {
		return err
	}
	defer func() { _ = telemetryShutdown(context.Background()) }()

	// Create Kafka producers for subtree and block topics.
	subtreeProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.SubtreeTopic, cfg.Kafka.TopicRetention(), logger)
	if err != nil {
		return err
	}
	defer func() { _ = subtreeProducer.Close() }()

	blockProducer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.BlockTopic, cfg.Kafka.TopicRetention(), logger)
	if err != nil {
		return err
	}
	defer func() { _ = blockProducer.Close() }()

	// Build the shared store registry so the p2p client can persist
	// every peer's advertised DataHub URL into the DataHubRegistry. The
	// registry is consulted by /reprocess and the block-processor
	// failover path, so populating it from node_status broadcasts means
	// we see every peer the network has advertised — not just the
	// peer-of-record on whichever block was last announced to us.
	storeRegistry, err := store.NewFromConfig(context.Background(), cfg, logger)
	if err != nil {
		return err
	}
	defer func() { _ = storeRegistry.Close() }()

	// Create, init, and start the P2P client.
	client := p2p.NewClient(
		cfg.P2P,
		subtreeProducer,
		blockProducer,
		storeRegistry.DataHubRegistry,
		cfg.DataHub.AllowPrivateIPs,
		logger,
	)

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

	var metricsSrv *metrics.Server
	if cfg.Metrics.Enabled {
		metricsSrv = metrics.NewServer(cfg.Metrics, logger)
		if err := metricsSrv.Init(nil); err != nil {
			return err
		}
		if err := metricsSrv.Start(ctx); err != nil {
			return err
		}
		defer func() {
			if err := metricsSrv.Stop(); err != nil {
				logger.Error("failed to stop metrics server", "error", err)
			}
		}()
	}

	runErr := client.Run(ctx)
	if stopErr := client.Stop(); stopErr != nil {
		logger.Error("failed to stop p2p client", "error", stopErr)
	}
	return runErr
}
