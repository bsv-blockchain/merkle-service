//go:build scale

package scale

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/block"
	"github.com/bsv-blockchain/merkle-service/internal/callback"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

const (
	aerospikeHost = "localhost"
	aerospikePort = 3100
	kafkaBroker   = "localhost:9192"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// TestMain performs environment checks before running scale tests.
func TestMain(m *testing.M) {
	// Check Kafka reachability.
	p, err := kafka.NewProducer([]string{kafkaBroker}, "probe-topic", slog.Default())
	if err != nil {
		fmt.Fprintf(os.Stderr, "SKIP: Kafka not reachable at %s: %v\n", kafkaBroker, err)
		os.Exit(0)
	}
	p.Close()

	// Check Aerospike reachability.
	ns := findNamespace()
	if ns == "" {
		fmt.Fprintf(os.Stderr, "SKIP: Aerospike not reachable at %s:%d\n", aerospikeHost, aerospikePort)
		os.Exit(0)
	}

	os.Exit(m.Run())
}

func findNamespace() string {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	for _, ns := range []string{"test", "merkle"} {
		client, err := store.NewAerospikeClient(aerospikeHost, aerospikePort, ns, 1, 50, logger)
		if err != nil {
			continue
		}
		// Verify namespace is writable with a probe write.
		regStore := store.NewRegistrationStore(client, "ns_probe", 1, 50, 0, logger)
		if err := regStore.Add("probe_txid", "http://probe", ""); err != nil {
			client.Close()
			continue
		}
		client.Close()
		return ns
	}
	return ""
}

// TestFixturesLoadable validates that all fixture files exist and have correct counts.
func TestFixturesLoadable(t *testing.T) {
	manifest, txids, subtreeData, err := loadAllFixtures(testdataDir)
	if err != nil {
		t.Fatalf("failed to load fixtures: %v", err)
	}

	if len(txids) != manifest.TotalTxids {
		t.Errorf("expected %d txids, got %d", manifest.TotalTxids, len(txids))
	}

	if len(subtreeData) != len(manifest.Subtrees) {
		t.Errorf("expected %d subtrees, got %d", len(manifest.Subtrees), len(subtreeData))
	}

	for _, st := range manifest.Subtrees {
		data, ok := subtreeData[st.Hash]
		if !ok {
			t.Errorf("missing subtree data for hash %s", st.Hash)
			continue
		}
		expectedSize := len(st.TxidIndices) * 32
		if len(data) != expectedSize {
			t.Errorf("subtree %d: expected %d bytes, got %d", st.Index, expectedSize, len(data))
		}
	}

	// Verify arcade instance coverage.
	covered := make(map[int]bool)
	for _, a := range manifest.ArcadeInstances {
		for j := a.TxidStart; j < a.TxidEnd; j++ {
			covered[j] = true
		}
	}
	if len(covered) != manifest.TotalTxids {
		t.Errorf("arcade coverage: expected %d, got %d", manifest.TotalTxids, len(covered))
	}

	t.Logf("fixtures loaded: %d txids, %d subtrees, %d arcade instances",
		len(txids), len(subtreeData), len(manifest.ArcadeInstances))
}

// TestCallbackFleetStartStop verifies that all callback servers can start and receive HTTP requests.
func TestCallbackFleetStartStop(t *testing.T) {
	fleet := NewCallbackFleet(basePort, 50)
	if err := fleet.StartAll(); err != nil {
		t.Fatalf("failed to start fleet: %v", err)
	}
	defer fleet.StopAll()

	// Send one POST to each server.
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < fleet.Count(); i++ {
		url := fmt.Sprintf("http://127.0.0.1:%d/callback", basePort+i)
		resp, err := client.Post(url, "application/json", nil)
		if err != nil {
			t.Errorf("server %d: POST failed: %v", i, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("server %d: expected 200, got %d", i, resp.StatusCode)
		}
	}

	totalCallbacks := fleet.TotalCallbacks()
	if totalCallbacks != 50 {
		t.Errorf("expected 50 total callbacks, got %d", totalCallbacks)
	}
}

// runScaleTest is the core test logic shared between smoke and full-scale variants.
func runScaleTest(t *testing.T, fixtureDir string, instanceCount int, timeout time.Duration) {
	logger := testLogger()
	namespace := findNamespace()
	if namespace == "" {
		t.Fatal("Aerospike namespace not found")
	}

	// Load fixtures.
	manifest, txids, subtreeData, err := loadAllFixtures(fixtureDir)
	if err != nil {
		t.Fatalf("failed to load fixtures: %v", err)
	}

	// Use only the first N instances if smoke testing.
	if instanceCount < len(manifest.ArcadeInstances) {
		manifest.ArcadeInstances = manifest.ArcadeInstances[:instanceCount]
		manifest.TotalTxids = 0
		for _, a := range manifest.ArcadeInstances {
			manifest.TotalTxids += a.TxidEnd - a.TxidStart
		}
	}

	// Start callback fleet.
	fleet := NewCallbackFleet(basePort, len(manifest.ArcadeInstances))
	if err := fleet.StartAll(); err != nil {
		t.Fatalf("failed to start callback fleet: %v", err)
	}
	t.Cleanup(func() { fleet.StopAll() })

	// Create Aerospike client and stores.
	asClient, err := store.NewAerospikeClient(aerospikeHost, aerospikePort, namespace, 3, 100, logger)
	if err != nil {
		t.Fatalf("failed to create Aerospike client: %v", err)
	}
	t.Cleanup(func() { asClient.Close() })

	regSetName := fmt.Sprintf("scale_reg_%d", time.Now().UnixNano())
	regStore := store.NewRegistrationStore(asClient, regSetName, 3, 100, 0, logger)

	urlRegistrySetName := fmt.Sprintf("scale_urls_%d", time.Now().UnixNano())
	// 0 ttlSec → constructor falls back to the default 7-day window, which is
	// far longer than any scale-test run.
	urlRegistry := store.NewCallbackURLRegistry(asClient, urlRegistrySetName, 0, 3, 100, logger)

	blobStore := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blobStore, 100, logger)
	stumpStore := store.NewStumpStore(blobStore, 100, logger)

	// Pre-load registrations.
	logger.Info("pre-loading registrations", "count", manifest.TotalTxids)
	if err := preloadRegistrations(manifest, txids, regStore, logger); err != nil {
		t.Fatalf("failed to pre-load registrations: %v", err)
	}
	if err := preloadCallbackURLRegistry(manifest, urlRegistry); err != nil {
		t.Fatalf("failed to pre-load callback URL registry: %v", err)
	}
	if err := preloadSubtrees(manifest, subtreeData, subtreeStore); err != nil {
		t.Fatalf("failed to pre-load subtrees: %v", err)
	}
	logger.Info("pre-loading complete")

	// Spot-check registrations (task 7.4).
	spotCheckRegistrations(t, regStore, manifest, txids)

	// Start mock DataHub.
	dataHubServer, dataHubURL, err := startMockDataHub(manifest, subtreeData)
	if err != nil {
		t.Fatalf("failed to start mock DataHub: %v", err)
	}
	t.Cleanup(func() { dataHubServer.Shutdown(context.Background()) })

	// Create Kafka topics and producers.
	blockTopic := fmt.Sprintf("scale-block-%d", time.Now().UnixNano())
	stumpsTopic := fmt.Sprintf("scale-stumps-%d", time.Now().UnixNano())
	stumpsDLQTopic := stumpsTopic + "-dlq"
	subtreeWorkTopic := fmt.Sprintf("scale-subtree-work-%d", time.Now().UnixNano())

	blockProducer, err := kafka.NewProducer([]string{kafkaBroker}, blockTopic, logger)
	if err != nil {
		t.Fatalf("failed to create block producer: %v", err)
	}
	t.Cleanup(func() { blockProducer.Close() })

	counterSetName := fmt.Sprintf("scale_counter_%d", time.Now().UnixNano())
	subtreeCounter := store.NewSubtreeCounterStore(asClient, counterSetName, 600, 3, 100, logger)

	// Start block processor.
	kafkaCfg := config.KafkaConfig{
		Brokers:          []string{kafkaBroker},
		BlockTopic:       blockTopic,
		CallbackTopic:    stumpsTopic,
		CallbackDLQTopic: stumpsDLQTopic,
		SubtreeWorkTopic: subtreeWorkTopic,
		ConsumerGroup:    fmt.Sprintf("scale-test-%d", time.Now().UnixNano()),
	}
	blockCfg := config.BlockConfig{
		WorkerPoolSize: 10,
		PostMineTTLSec: 0,
		DedupCacheSize: 100,
	}
	datahubCfg := config.DataHubConfig{
		TimeoutSec: 30,
		MaxRetries: 2,
	}

	processor := block.NewProcessor(kafkaCfg, blockCfg, datahubCfg, regStore, subtreeStore, urlRegistry, subtreeCounter, logger)
	if err := processor.Init(nil); err != nil {
		t.Fatalf("failed to init block processor: %v", err)
	}

	ctx := context.Background()
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("failed to start block processor: %v", err)
	}
	t.Cleanup(func() { processor.Stop() })

	// Start subtree worker service.
	subtreeWorker := block.NewSubtreeWorkerService(kafkaCfg, blockCfg, datahubCfg, regStore, subtreeStore, stumpStore, urlRegistry, subtreeCounter, logger)
	if err := subtreeWorker.Init(nil); err != nil {
		t.Fatalf("failed to init subtree worker: %v", err)
	}
	if err := subtreeWorker.Start(ctx); err != nil {
		t.Fatalf("failed to start subtree worker: %v", err)
	}
	t.Cleanup(func() { subtreeWorker.Stop() })

	// Start callback delivery service.
	deliveryCfg := &config.Config{
		Kafka: kafkaCfg,
		Callback: config.CallbackConfig{
			MaxRetries:          5,
			BackoffBaseSec:      1,
			TimeoutSec:          10,
			DeliveryWorkers:     256,
			MaxConnsPerHost:     64,
			MaxIdleConnsPerHost: 32,
		},
	}
	deliveryService1 := callback.NewDeliveryService(deliveryCfg, nil, stumpStore)
	if err := deliveryService1.Init(nil); err != nil {
		t.Fatalf("failed to init delivery service 1: %v", err)
	}
	if err := deliveryService1.Start(ctx); err != nil {
		t.Fatalf("failed to start delivery service 1: %v", err)
	}
	t.Cleanup(func() { deliveryService1.Stop() })

	// Start second delivery service instance (same consumer group) for multi-instance validation.
	deliveryService2 := callback.NewDeliveryService(deliveryCfg, nil, stumpStore)
	if err := deliveryService2.Init(nil); err != nil {
		t.Fatalf("failed to init delivery service 2: %v", err)
	}
	if err := deliveryService2.Start(ctx); err != nil {
		t.Fatalf("failed to start delivery service 2: %v", err)
	}
	t.Cleanup(func() { deliveryService2.Stop() })

	// Allow consumers to settle.
	time.Sleep(2 * time.Second)

	// Inject block — T0.
	t0 := time.Now()
	logger.Info("injecting block message", "blockHash", manifest.BlockHash)
	if err := injectBlock(manifest, blockTopic, blockProducer, dataHubURL); err != nil {
		t.Fatalf("failed to inject block: %v", err)
	}

	// Wait for all callbacks.
	waitForAllCallbacks(t, fleet, manifest, timeout)

	// Collect metrics.
	report := collectMetrics(fleet, t0)
	printReport(t, report)

	// Run verifications.
	runAllVerifications(t, fleet, manifest, txids)
}

// spotCheckRegistrations verifies a sample of registrations loaded correctly.
func spotCheckRegistrations(t *testing.T, regStore *store.RegistrationStore, manifest *Manifest, txids [][]byte) {
	t.Helper()

	// Check 10 random txids spread across instances.
	for i := 0; i < 10; i++ {
		arcadeIdx := i * len(manifest.ArcadeInstances) / 10
		arcade := manifest.ArcadeInstances[arcadeIdx]
		txidIdx := arcade.TxidStart + (arcade.TxidEnd-arcade.TxidStart)/2
		txidStr := hashToTxidString(txids[txidIdx])

		regs, err := regStore.BatchGet([]string{txidStr})
		if err != nil {
			t.Errorf("spot check: BatchGet failed for txid %s: %v", txidStr, err)
			continue
		}
		if len(regs[txidStr]) == 0 {
			t.Errorf("spot check: txid %s (arcade %d) not found in registrations", txidStr, arcadeIdx)
		}
	}
}

// TestScaleSmoke runs a smaller variant for fast iteration (5 instances, ~5000 txids).
func TestScaleSmoke(t *testing.T) {
	runScaleTest(t, testdataDir, 5, 2*time.Minute)
}

// TestScaleEndToEnd runs the full-scale test (50 instances, 50000 txids).
func TestScaleEndToEnd(t *testing.T) {
	runScaleTest(t, testdataDir, 50, 5*time.Minute)
}

// TestScaleMega runs the mega-scale test (100 instances, 1000000 block txids, 244 subtrees).
func TestScaleMega(t *testing.T) {
	runScaleTest(t, "testdata-mega", 100, 10*time.Minute)
}
