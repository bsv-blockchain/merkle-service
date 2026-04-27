package p2p

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/service"
)

const (
	// maxPublishRetries is the maximum number of retries for Kafka publish failures.
	maxPublishRetries = 5
	// baseRetryDelay is the initial delay between publish retries.
	baseRetryDelay = 500 * time.Millisecond
)

// Client is a P2P client service that connects to the Teranode P2P network
// via go-teranode-p2p-client, subscribes to subtree and block topics, and publishes
// received messages to Kafka.
type Client struct {
	service.BaseService

	cfg       config.P2PConfig
	p2pClient *p2p.Client

	subtreeProducer *kafka.Producer
	blockProducer   *kafka.Producer

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.RWMutex
	connected bool
}

// NewClient creates a new P2P client with the given configuration and Kafka producers.
func NewClient(
	cfg config.P2PConfig,
	subtreeProducer *kafka.Producer,
	blockProducer *kafka.Producer,
	logger *slog.Logger,
) *Client {
	c := &Client{
		cfg:             cfg,
		subtreeProducer: subtreeProducer,
		blockProducer:   blockProducer,
	}
	c.InitBase("p2p-client")
	if logger != nil {
		c.Logger = logger
	}
	return c
}

// Init validates the configuration and prepares the client for startup.
func (c *Client) Init(_ interface{}) error {
	if c.subtreeProducer == nil {
		return fmt.Errorf("subtree kafka producer is required")
	}
	if c.blockProducer == nil {
		return fmt.Errorf("block kafka producer is required")
	}

	c.Logger.Info("p2p client initialized",
		"network", c.cfg.Network,
		"storagePath", c.cfg.StoragePath,
	)
	return nil
}

// Start creates the p2p client via go-teranode-p2p-client, subscribes to topics,
// and begins processing incoming messages.
func (c *Client) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

	network := c.cfg.Network
	if network == "" {
		network = "main"
	}

	p2pCfg := p2p.Config{
		Network:     network,
		StoragePath: c.cfg.StoragePath,
	}

	mb := &p2pCfg.MsgBus
	mb.DHTMode = c.cfg.MsgBus.DHTMode
	mb.Port = c.cfg.MsgBus.Port
	mb.AnnounceAddrs = c.cfg.MsgBus.AnnounceAddrs
	mb.BootstrapPeers = c.cfg.MsgBus.BootstrapPeers
	mb.MaxConnections = c.cfg.MsgBus.MaxConnections
	mb.MinConnections = c.cfg.MsgBus.MinConnections
	mb.EnableNAT = c.cfg.MsgBus.EnableNAT
	mb.EnableMDNS = c.cfg.MsgBus.EnableMDNS

	client, err := p2pCfg.Initialize(ctx, "merkle-service")
	if err != nil {
		return fmt.Errorf("failed to initialize p2p client: %w", err)
	}
	c.p2pClient = client

	c.Logger.Info("p2p client created",
		"peerID", client.GetID(),
		"network", client.GetNetwork(),
		"dhtMode", c.cfg.MsgBus.DHTMode,
		"port", c.cfg.MsgBus.Port,
	)

	// Subscribe to typed channels.
	subtreeCh := c.p2pClient.SubscribeSubtrees(ctx)
	blockCh := c.p2pClient.SubscribeBlocks(ctx)

	// Start message processing goroutines.
	c.wg.Add(2)
	go c.processSubtreeMessages(ctx, subtreeCh)
	go c.processBlockMessages(ctx, blockCh)

	c.SetStarted(true)
	c.setConnected(true)

	c.Logger.Info("p2p client started")
	return nil
}

// Stop gracefully shuts down the P2P client, closing the message bus and
// waiting for goroutines to complete.
func (c *Client) Stop() error {
	c.Logger.Info("stopping p2p client")

	if c.cancel != nil {
		c.cancel()
	}

	// Wait for goroutines to finish with a timeout.
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		c.Logger.Warn("timed out waiting for p2p goroutines to stop")
	}

	if c.p2pClient != nil {
		if err := c.p2pClient.Close(); err != nil {
			c.Logger.Error("error closing p2p client", "error", err)
		}
	}

	c.SetStarted(false)
	c.setConnected(false)

	c.Logger.Info("p2p client stopped")
	return nil
}

// Health returns the current health status of the P2P client.
func (c *Client) Health() service.HealthStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	peerCount := 0
	if c.p2pClient != nil {
		peerCount = len(c.p2pClient.GetPeers())
	}

	status := "healthy"
	details := map[string]string{
		"peerCount": fmt.Sprintf("%d", peerCount),
	}

	if !c.connected {
		status = "unhealthy"
		details["connection"] = "disconnected"
	} else {
		details["connection"] = "connected"
	}

	if peerCount == 0 {
		status = "degraded"
		details["peers"] = "no peers connected"
	}

	return service.HealthStatus{
		Name:    "p2p-client",
		Status:  status,
		Details: details,
	}
}

// processSubtreeMessages reads typed subtree messages and publishes them to Kafka.
func (c *Client) processSubtreeMessages(ctx context.Context, ch <-chan teranode.SubtreeMessage) {
	defer c.wg.Done()

	c.Logger.Info("starting subtree message processing loop")

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				c.Logger.Info("subtree message channel closed")
				return
			}
			c.handleSubtreeMessage(msg)
		case <-ctx.Done():
			c.Logger.Info("subtree message loop exiting: context cancelled")
			return
		}
	}
}

// processBlockMessages reads typed block messages and publishes them to Kafka.
func (c *Client) processBlockMessages(ctx context.Context, ch <-chan teranode.BlockMessage) {
	defer c.wg.Done()

	c.Logger.Info("starting block message processing loop")

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				c.Logger.Info("block message channel closed")
				return
			}
			c.handleBlockMessage(msg)
		case <-ctx.Done():
			c.Logger.Info("block message loop exiting: context cancelled")
			return
		}
	}
}

// handleSubtreeMessage maps a teranode SubtreeMessage to a Kafka SubtreeMessage and publishes it.
func (c *Client) handleSubtreeMessage(msg teranode.SubtreeMessage) {
	c.Logger.Debug("received subtree announcement",
		"hash", msg.Hash,
		"dataHubUrl", msg.DataHubURL,
	)

	kafkaMsg := kafka.SubtreeMessage{
		Hash:       msg.Hash,
		DataHubURL: msg.DataHubURL,
		PeerID:     msg.PeerID,
		ClientName: msg.ClientName,
	}

	encoded, err := kafkaMsg.Encode()
	if err != nil {
		c.Logger.Error("failed to encode subtree message for kafka",
			"hash", msg.Hash,
			"error", err,
		)
		return
	}

	c.publishWithRetry(c.subtreeProducer, msg.Hash, encoded, "subtree")
}

// handleBlockMessage maps a teranode BlockMessage to a Kafka BlockMessage and publishes it.
func (c *Client) handleBlockMessage(msg teranode.BlockMessage) {
	c.Logger.Debug("received block announcement",
		"hash", msg.Hash,
		"height", msg.Height,
		"dataHubUrl", msg.DataHubURL,
	)

	kafkaMsg := kafka.BlockMessage{
		Hash:       msg.Hash,
		Height:     msg.Height,
		Header:     msg.Header,
		Coinbase:   msg.Coinbase,
		DataHubURL: msg.DataHubURL,
		PeerID:     msg.PeerID,
		ClientName: msg.ClientName,
	}

	encoded, err := kafkaMsg.Encode()
	if err != nil {
		c.Logger.Error("failed to encode block message for kafka",
			"hash", msg.Hash,
			"error", err,
		)
		return
	}

	c.publishWithRetry(c.blockProducer, msg.Hash, encoded, "block")
}

// publishWithRetry attempts to publish a message to Kafka with exponential backoff retries.
func (c *Client) publishWithRetry(producer *kafka.Producer, key string, value []byte, msgType string) {
	delay := baseRetryDelay

	for attempt := 1; attempt <= maxPublishRetries; attempt++ {
		err := producer.Publish(key, value)
		if err == nil {
			return
		}

		c.Logger.Error("kafka publish failed",
			"type", msgType,
			"key", key,
			"attempt", attempt,
			"maxAttempts", maxPublishRetries,
			"error", err,
		)

		if attempt == maxPublishRetries {
			c.Logger.Error("kafka publish exhausted all retries, message dropped",
				"type", msgType,
				"key", key,
				"valueLen", len(value),
			)
			return
		}

		time.Sleep(delay)
		delay = time.Duration(math.Min(float64(delay)*2, float64(10*time.Second)))
	}
}

// setConnected updates the connected state in a thread-safe manner.
func (c *Client) setConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
}
