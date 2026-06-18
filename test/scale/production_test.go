//go:build scale

package scale

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bsv-blockchain/merkle-service/internal/block"
	"github.com/bsv-blockchain/merkle-service/internal/callback"
	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	"github.com/bsv-blockchain/merkle-service/internal/subtree"
)

// Production-shaped end-to-end throughput test. Differences from the legacy
// runScaleTest, all matching how the service is actually deployed
// (deploy/k8s/README.md):
//
//   - Topics are PRE-CREATED with production-like partition counts (the
//     legacy test relied on broker auto-creation = 1 partition everywhere,
//     serializing every stage to a single consumer).
//   - The SEEN path runs: subtree announcements flow through the
//     subtree-fetcher (registration lookup, seen-counter updates, batched
//     SEEN_ON_NETWORK callbacks) BEFORE the block is mined — the legacy test
//     skipped this stage entirely, and at production scale it is the
//     binding constraint of the pipeline.
//   - Production-default config: post-mine TTL refresh on.
//   - Four callback-delivery instances drain the multi-partition callback
//     topic, as in the reference deployment.
//
// Phase timings are reported separately: SEEN (announce -> all SEEN
// callbacks delivered) and MINED (block inject -> all STUMP+BLOCK_PROCESSED
// delivered), each with txid throughput.
const (
	prodSubtreePartitions     = 8
	prodSubtreeWorkPartitions = 16
	prodCallbackPartitions    = 24
	prodDeliveryInstances     = 12
)

func TestScaleProductionShape(t *testing.T) {
	runProductionScaleTest(t, "testdata-mega", 20*time.Minute)
}

func createTopics(t *testing.T, topics map[string]int32) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaBroker))
	if err != nil {
		t.Fatalf("kafka admin client: %v", err)
	}
	defer client.Close()
	adm := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for topic, parts := range topics {
		if _, err := adm.CreateTopic(ctx, parts, 1, nil, topic); err != nil {
			t.Fatalf("creating topic %s (%d partitions): %v", topic, parts, err)
		}
	}
}

func runProductionScaleTest(t *testing.T, fixtureDir string, timeout time.Duration) {
	logger := testLogger()
	namespace := findNamespace()
	if namespace == "" {
		t.Fatal("Aerospike namespace not found")
	}

	manifest, txids, subtreeData, err := loadAllFixtures(fixtureDir)
	if err != nil {
		t.Fatalf("failed to load fixtures: %v", err)
	}

	fleet := NewCallbackFleet(basePort, len(manifest.ArcadeInstances))
	if err := fleet.StartAll(); err != nil {
		t.Fatalf("failed to start callback fleet: %v", err)
	}
	t.Cleanup(func() { fleet.StopAll() })

	asClient, err := store.NewAerospikeClient(aerospikeHost, aerospikePort, namespace, 3, 100, logger)
	if err != nil {
		t.Fatalf("failed to create Aerospike client: %v", err)
	}
	t.Cleanup(func() { asClient.Close() })

	stamp := time.Now().UnixNano()
	regStore := store.NewRegistrationStore(asClient, fmt.Sprintf("prod_reg_%d", stamp), 3, 100, 0, logger)
	urlRegistry := store.NewCallbackURLRegistry(asClient, fmt.Sprintf("prod_urls_%d", stamp), 0, 3, 100, logger)
	dataHubRegistry := store.NewDataHubRegistry(asClient, fmt.Sprintf("prod_datahub_%d", stamp), 0, 3, 100, logger)
	subtreeCounter := store.NewSubtreeCounterStore(asClient, fmt.Sprintf("prod_counter_%d", stamp), 600, 3, 100, logger)
	expectedStumps := store.NewExpectedStumpStore(asClient, fmt.Sprintf("prod_expstump_%d", stamp), 600, 3, 100, logger)
	// seenThreshold 3 = config.yaml default; with a single announcement per
	// subtree no SEEN_MULTIPLE_NODES fires, matching one-node observation.
	seenCounter := store.NewSeenCounterStore(asClient, fmt.Sprintf("prod_seen_%d", stamp), 3, 3, 100, logger)

	blobStore := store.NewMemoryBlobStore()
	subtreeStore := store.NewSubtreeStore(blobStore, 100, logger)
	stumpStore := store.NewStumpStore(blobStore, 100, logger)

	logger.Info("pre-loading registrations", "count", manifest.TotalTxids)
	if err := preloadRegistrations(manifest, txids, regStore, logger); err != nil {
		t.Fatalf("failed to pre-load registrations: %v", err)
	}
	if err := preloadCallbackURLRegistry(manifest, urlRegistry); err != nil {
		t.Fatalf("failed to pre-load callback URL registry: %v", err)
	}
	// NOTE: subtree blobs are deliberately NOT pre-loaded into the blob
	// store: the fetcher stage fetches them from the (mock) DataHub exactly
	// as production does, then persists them for the workers.

	dataHubServer, dataHubURL, err := startMockDataHub(manifest, subtreeData)
	if err != nil {
		t.Fatalf("failed to start mock DataHub: %v", err)
	}
	t.Cleanup(func() { dataHubServer.Shutdown(context.Background()) })

	// Pre-create topics at production-like partition counts.
	subtreeTopic := fmt.Sprintf("prod-subtree-%d", stamp)
	subtreeDLQTopic := subtreeTopic + "-dlq"
	blockTopic := fmt.Sprintf("prod-block-%d", stamp)
	callbackTopic := fmt.Sprintf("prod-callback-%d", stamp)
	callbackDLQTopic := callbackTopic + "-dlq"
	subtreeWorkTopic := fmt.Sprintf("prod-subtree-work-%d", stamp)
	subtreeWorkDLQ := subtreeWorkTopic + "-dlq"
	createTopics(t, map[string]int32{
		subtreeTopic:     prodSubtreePartitions,
		subtreeDLQTopic:  1,
		blockTopic:       1,
		callbackTopic:    prodCallbackPartitions,
		callbackDLQTopic: 1,
		subtreeWorkTopic: prodSubtreeWorkPartitions,
		subtreeWorkDLQ:   1,
	})

	kafkaCfg := config.KafkaConfig{
		Brokers:             []string{kafkaBroker},
		SubtreeTopic:        subtreeTopic,
		SubtreeDLQTopic:     subtreeDLQTopic,
		BlockTopic:          blockTopic,
		CallbackTopic:       callbackTopic,
		CallbackDLQTopic:    callbackDLQTopic,
		SubtreeWorkTopic:    subtreeWorkTopic,
		SubtreeWorkDLQTopic: subtreeWorkDLQ,
		ConsumerGroup:       fmt.Sprintf("prod-scale-%d", stamp),
	}
	datahubCfg := config.DataHubConfig{
		TimeoutSec:      30,
		MaxRetries:      2,
		AllowPrivateIPs: true, // mock DataHub runs on 127.0.0.1
	}

	ctx := context.Background()

	// --- Subtree fetcher (SEEN path) ---
	fetcherCfg := &config.Config{
		Kafka:   kafkaCfg,
		DataHub: datahubCfg,
		Subtree: config.SubtreeConfig{
			CacheMaxMB:     64,
			DedupCacheSize: 1024,
			MaxAttempts:    10,
		},
	}
	fetcher := subtree.NewProcessor(fetcherCfg, regStore, seenCounter, subtreeStore)
	if err := fetcher.Init(nil); err != nil {
		t.Fatalf("failed to init subtree fetcher: %v", err)
	}
	if err := fetcher.Start(ctx); err != nil {
		t.Fatalf("failed to start subtree fetcher: %v", err)
	}
	t.Cleanup(func() { fetcher.Stop() })

	// --- Block processor ---
	blockCfg := config.BlockConfig{
		WorkerPoolSize: 10,
		PostMineTTLSec: 1800, // production default (config.yaml postMineTTLSec)
		DedupCacheSize: 100,
	}
	processor := block.NewProcessor(kafkaCfg, blockCfg, datahubCfg, regStore, subtreeStore, urlRegistry, dataHubRegistry, subtreeCounter, logger)
	if err := processor.Init(nil); err != nil {
		t.Fatalf("failed to init block processor: %v", err)
	}
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("failed to start block processor: %v", err)
	}
	t.Cleanup(func() { processor.Stop() })

	// --- Subtree worker ---
	worker := block.NewSubtreeWorkerService(kafkaCfg, blockCfg, datahubCfg, regStore, subtreeStore, stumpStore, urlRegistry, subtreeCounter, expectedStumps, logger)
	if err := worker.Init(nil); err != nil {
		t.Fatalf("failed to init subtree worker: %v", err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatalf("failed to start subtree worker: %v", err)
	}
	t.Cleanup(func() { worker.Stop() })

	// --- Callback delivery fleet (production reference: scale by partitions) ---
	deliveryCfg := &config.Config{
		Kafka: kafkaCfg,
		Callback: config.CallbackConfig{
			AllowPrivateIPs:     true, // callback fleet runs on 127.0.0.1
			MaxRetries:          5,
			BackoffBaseSec:      1,
			TimeoutSec:          10,
			MaxConnsPerHost:     64,
			MaxIdleConnsPerHost: 32,
		},
	}
	for i := 0; i < prodDeliveryInstances; i++ {
		ds := callback.NewDeliveryService(deliveryCfg, nil, stumpStore)
		if err := ds.Init(nil); err != nil {
			t.Fatalf("failed to init delivery service %d: %v", i, err)
		}
		if err := ds.Start(ctx); err != nil {
			t.Fatalf("failed to start delivery service %d: %v", i, err)
		}
		t.Cleanup(func() { ds.Stop() })
	}

	// Expected per-arcade payload counts (one batched callback per subtree
	// containing the arcade's txids; SEEN and STUMP both follow this shape
	// because both chunk well above the per-(arcade,subtree) txid counts).
	perArcade := expectedMinedPayloadsPerArcade(manifest)
	var expectedPayloads int64
	for _, set := range perArcade {
		expectedPayloads += int64(len(set))
	}

	// ---- Phase A: SEEN (network observation) ----
	subtreeProducer, err := kafka.NewProducer([]string{kafkaBroker}, subtreeTopic, logger)
	if err != nil {
		t.Fatalf("failed to create subtree producer: %v", err)
	}
	t.Cleanup(func() { subtreeProducer.Close() })

	tSeen0 := time.Now()
	for _, st := range manifest.Subtrees {
		msg := &kafka.SubtreeMessage{Hash: st.Hash, DataHubURL: dataHubURL, PeerID: "scale-peer", ClientName: "scale"}
		data, encErr := msg.Encode()
		if encErr != nil {
			t.Fatalf("encoding subtree announcement: %v", encErr)
		}
		if pubErr := subtreeProducer.PublishWithHashKey(st.Hash, data); pubErr != nil {
			t.Fatalf("publishing subtree announcement: %v", pubErr)
		}
	}
	logger.Info("announced subtrees", "count", len(manifest.Subtrees))

	waitSeen := func() (int64, int64) {
		var payloads, seenTxids int64
		for i := 0; i < fleet.Count(); i++ {
			p, x := fleet.GetServer(i).SeenCounts()
			payloads += int64(p)
			seenTxids += int64(x)
		}
		return payloads, seenTxids
	}
	deadline := time.After(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	for done := false; !done; {
		select {
		case <-deadline:
			p, x := waitSeen()
			t.Fatalf("timeout waiting for SEEN callbacks: got %d/%d payloads (%d txids)", p, expectedPayloads, x)
		case <-tick.C:
			if p, _ := waitSeen(); p >= expectedPayloads {
				done = true
			}
		}
	}
	tick.Stop()
	seenDur := time.Since(tSeen0)
	_, seenTxids := waitSeen()

	// ---- Phase B: MINED (block processing) ----
	blockProducer, err := kafka.NewProducer([]string{kafkaBroker}, blockTopic, logger)
	if err != nil {
		t.Fatalf("failed to create block producer: %v", err)
	}
	t.Cleanup(func() { blockProducer.Close() })

	tMined0 := time.Now()
	if err := injectBlock(manifest, blockTopic, blockProducer, dataHubURL); err != nil {
		t.Fatalf("failed to inject block: %v", err)
	}
	waitForAllCallbacks(t, fleet, manifest, timeout)
	minedDur := time.Since(tMined0)

	// ---- Report ----
	seenRate := float64(seenTxids) / seenDur.Seconds()
	minedRate := float64(manifest.TotalTxids) / minedDur.Seconds()
	totalDur := seenDur + minedDur
	totalRate := float64(manifest.TotalTxids) / totalDur.Seconds()
	t.Logf(
		"\n"+
			"╔══════════════════════════════════════════════════════════════╗\n"+
			"║          PRODUCTION-SHAPE SCALE REPORT                       ║\n"+
			"╠══════════════════════════════════════════════════════════════╣\n"+
			"║ Topology: subtree=%dp, subtree-work=%dp, callback=%dp, delivery=%d ║\n"+
			"║ SEEN phase (announce→delivered):  %-12s %9.0f txids/s ║\n"+
			"║ MINED phase (inject→delivered):   %-12s %9.0f txids/s ║\n"+
			"║ Total (both phases, %7d txids): %-12s %8.0f txids/s ║\n"+
			"╚══════════════════════════════════════════════════════════════╝",
		prodSubtreePartitions, prodSubtreeWorkPartitions, prodCallbackPartitions, prodDeliveryInstances,
		seenDur.Round(time.Millisecond), seenRate,
		minedDur.Round(time.Millisecond), minedRate,
		manifest.TotalTxids, totalDur.Round(time.Millisecond), totalRate,
	)

	runAllVerifications(t, fleet, manifest, txids)
}
