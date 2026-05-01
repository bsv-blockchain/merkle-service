package p2p

import (
	"context"
	"errors"
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
	// defaultBaseRetryDelay is the initial delay between publish retries.
	defaultBaseRetryDelay = 500 * time.Millisecond
)

// baseRetryDelay is the initial delay between publish retries. It is a var
// (rather than a const) so tests can shrink it; production code should not
// reassign it.
var baseRetryDelay = defaultBaseRetryDelay

// ErrPublishExhausted is returned by the publish helper when all Kafka publish
// retries have been exhausted. It is treated as a terminal/fatal condition by
// the P2P client: the announcement cannot be re-observed from the network
// (peers are unlikely to re-broadcast one-shot subtree gossip), so the process
// exits non-zero and relies on the orchestrator (Kubernetes restartPolicy:
// Always, Docker restart: always, systemd, etc.) to restart the pod and
// re-establish a fresh P2P session against a recovered Kafka.
var ErrPublishExhausted = errors.New("kafka publish retries exhausted")

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

	// fatalErr is signalled (non-blocking, buffered=1) when a publish loop hits
	// a terminal condition (e.g. ErrPublishExhausted). Run reads from it to
	// propagate the failure to the caller, which is expected to exit the
	// process so an orchestrator-managed restart can re-establish state.
	fatalErr chan error

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
		fatalErr:        make(chan error, 1),
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

// Run starts the client (if it has not already been started) and blocks until
// either the supplied context is cancelled or a terminal/fatal error is
// signalled by the publish path (e.g. ErrPublishExhausted).
//
// On terminal error the returned error is non-nil; callers (typically a
// process entry point) should log it and exit the process with a non-zero
// status so the orchestrator restarts the pod. A clean context cancellation
// returns nil.
func (c *Client) Run(ctx context.Context) error {
	if !c.IsStarted() {
		if err := c.Start(ctx); err != nil {
			return err
		}
	}

	select {
	case err := <-c.fatalErr:
		c.Logger.Error("p2p client terminating due to fatal error", "error", err)
		return err
	case <-ctx.Done():
		return nil
	}
}

// signalFatal records a terminal error and cancels the client's internal
// context so processing goroutines unwind. Only the first terminal error is
// reported to Run; subsequent calls are best-effort no-ops.
func (c *Client) signalFatal(err error) {
	if err == nil {
		return
	}
	select {
	case c.fatalErr <- err:
	default:
		// A fatal error has already been reported; drop the duplicate but
		// still ensure the context is cancelled.
	}
	if c.cancel != nil {
		c.cancel()
	}
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
			if err := c.handleSubtreeMessage(ctx, msg); err != nil {
				c.signalFatal(err)
				return
			}
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
			if err := c.handleBlockMessage(ctx, msg); err != nil {
				c.signalFatal(err)
				return
			}
		case <-ctx.Done():
			c.Logger.Info("block message loop exiting: context cancelled")
			return
		}
	}
}

// handleSubtreeMessage maps a teranode SubtreeMessage to a Kafka SubtreeMessage and publishes it.
//
// Returns a non-nil error only for terminal/fatal conditions (currently
// ErrPublishExhausted). Encoding errors and transient publish errors are
// logged and swallowed so the loop can continue on subsequent messages.
func (c *Client) handleSubtreeMessage(ctx context.Context, msg teranode.SubtreeMessage) error {
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
		return nil
	}

	return c.publishWithRetry(ctx, c.subtreeProducer, msg.Hash, encoded, "subtree")
}

// handleBlockMessage maps a teranode BlockMessage to a Kafka BlockMessage and publishes it.
//
// Returns a non-nil error only for terminal/fatal conditions (currently
// ErrPublishExhausted). Encoding errors and transient publish errors are
// logged and swallowed so the loop can continue on subsequent messages.
func (c *Client) handleBlockMessage(ctx context.Context, msg teranode.BlockMessage) error {
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
		return nil
	}

	return c.publishWithRetry(ctx, c.blockProducer, msg.Hash, encoded, "block")
}

// publishWithRetry attempts to publish a message to Kafka with exponential
// backoff retries.
//
// Returns ErrPublishExhausted (wrapped) if all attempts fail. Callers are
// expected to treat this as a terminal/fatal condition: the announcement
// cannot be reconstructed from the network (peers are unlikely to re-broadcast
// one-shot subtree gossip and there is no on-disk outbox), so silently
// continuing would cause permanent loss of network observations the longer a
// Kafka outage lasts. Instead, the client signals shutdown to its supervisor
// (see Client.Run) which exits non-zero so the process orchestrator can
// restart the pod from a fresh, durable P2P session.
//
// A nil return means the message was successfully published.
func (c *Client) publishWithRetry(ctx context.Context, producer *kafka.Producer, key string, value []byte, msgType string) error {
	delay := baseRetryDelay

	for attempt := 1; attempt <= maxPublishRetries; attempt++ {
		err := producer.Publish(key, value)
		if err == nil {
			return nil
		}

		c.Logger.Error("kafka publish failed",
			"type", msgType,
			"key", key,
			"attempt", attempt,
			"maxAttempts", maxPublishRetries,
			"error", err,
		)

		if attempt == maxPublishRetries {
			c.Logger.Error("kafka publish exhausted all retries, signalling fatal shutdown",
				"type", msgType,
				"key", key,
				"valueLen", len(value),
			)
			return fmt.Errorf("%w: type=%s key=%s: %v", ErrPublishExhausted, msgType, key, err)
		}

		select {
		case <-ctx.Done():
			// Treat shutdown during retry as non-fatal; the supervising
			// process is already tearing things down.
			return nil
		case <-time.After(delay):
		}
		delay = time.Duration(math.Min(float64(delay)*2, float64(10*time.Second)))
	}
	return nil
}

// setConnected updates the connected state in a thread-safe manner.
func (c *Client) setConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
}
