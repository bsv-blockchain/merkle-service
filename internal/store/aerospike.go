package store

import (
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// AerospikeClient wraps the Aerospike client with connection pool, retry policy, and health check.
type AerospikeClient struct {
	client          *as.Client
	namespace       string
	logger          *slog.Logger
	policy          *as.ClientPolicy
	socketTimeoutMs int
	totalTimeoutMs  int
}

// NewAerospikeClient creates a new Aerospike client wrapper from a single seed.
// Prefer NewAerospikeClientFromConfig for production: it picks up the connection
// pool sizing and batch-policy timeouts that prevent the
// "connection pool is empty" failure mode under bursty BatchGet load.
func NewAerospikeClient(host string, port int, namespace string, maxRetries int, retryBaseMs int, logger *slog.Logger) (*AerospikeClient, error) {
	policy := as.NewClientPolicy()
	policy.Timeout = 5 * time.Second

	client, err := as.NewClientWithPolicy(policy, host, port)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Aerospike: %w", err)
	}

	return &AerospikeClient{
		client:    client,
		namespace: namespace,
		logger:    logger,
		policy:    policy,
	}, nil
}

// NewAerospikeClientFromConfig builds a client using the full AerospikeConfig
// — multiple seed nodes, raised connection-queue size, and operation-level
// socket/total timeouts that get applied to BatchPolicy.
//
// Connection-queue size is intentionally the only ClientPolicy knob we change
// from defaults: production Aerospike defaults (Timeout=30s, MinConnections=0,
// LimitConnectionsToQueueSize=true) are sane, and overriding them turns
// cluster bootstrap into a flaky operation under load. The fix for the
// "connection pool is empty" failure mode is the queue size + the per-batch
// SocketTimeout/TotalTimeout we set on BatchPolicy below, NOT churn at the
// client-policy layer.
func NewAerospikeClientFromConfig(cfg config.AerospikeConfig, logger *slog.Logger) (*AerospikeClient, error) {
	seeds := cfg.SeedHosts()
	if len(seeds) == 0 {
		return nil, fmt.Errorf("aerospike: no seeds configured (set aerospike.seeds or aerospike.host)")
	}

	hosts := make([]*as.Host, 0, len(seeds))
	for _, h := range seeds {
		hosts = append(hosts, as.NewHost(h, cfg.Port))
	}

	policy := as.NewClientPolicy()
	if cfg.ConnectionQueueSize > 0 {
		policy.ConnectionQueueSize = cfg.ConnectionQueueSize
	}
	// MinConnectionsPerNode and LimitConnectionsToQueueSize are intentionally
	// left at Aerospike defaults — overriding either has triggered intermittent
	// bootstrap timeouts in microservice deployments. The connection pool
	// fix lives in ConnectionQueueSize + BatchPolicy timeouts.

	client, err := as.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Aerospike (seeds=%v): %w", seeds, err)
	}

	logger.Info("aerospike client initialized",
		"seeds", seeds,
		"port", cfg.Port,
		"connectionQueueSize", policy.ConnectionQueueSize,
		"socketTimeoutMs", cfg.SocketTimeoutMs,
		"totalTimeoutMs", cfg.TotalTimeoutMs,
	)

	return &AerospikeClient{
		client:          client,
		namespace:       cfg.Namespace,
		logger:          logger,
		policy:          policy,
		socketTimeoutMs: cfg.SocketTimeoutMs,
		totalTimeoutMs:  cfg.TotalTimeoutMs,
	}, nil
}

func (c *AerospikeClient) Client() *as.Client { return c.client }
func (c *AerospikeClient) Namespace() string  { return c.namespace }
func (c *AerospikeClient) Close()             { c.client.Close() }

func (c *AerospikeClient) Healthy() bool {
	return c.client.IsConnected()
}

// WritePolicy returns a write policy with retry settings.
func (c *AerospikeClient) WritePolicy(maxRetries int, retryBaseMs int) *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.MaxRetries = maxRetries
	wp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	if c.socketTimeoutMs > 0 {
		wp.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	}
	if c.totalTimeoutMs > 0 {
		wp.TotalTimeout = time.Duration(c.totalTimeoutMs) * time.Millisecond
	}
	return wp
}

// BatchPolicy returns a batch policy with retry + timeout settings.
// SocketTimeout / TotalTimeout default to the values supplied via
// NewAerospikeClientFromConfig; without them, Aerospike's default batch policy
// has SocketTimeout=30s but TotalTimeout=1s, which combined with no socket cap
// makes "connection pool is empty" stalls drag on far longer than the per-call
// SLA arcade expects.
func (c *AerospikeClient) BatchPolicy(maxRetries int, retryBaseMs int) *as.BatchPolicy {
	bp := as.NewBatchPolicy()
	bp.MaxRetries = maxRetries
	bp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	if c.socketTimeoutMs > 0 {
		bp.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	}
	if c.totalTimeoutMs > 0 {
		bp.TotalTimeout = time.Duration(c.totalTimeoutMs) * time.Millisecond
	}
	return bp
}
