package store

import (
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// AerospikeClient wraps the Aerospike client with per-operation policy
// builders. The shape mirrors what arcade-v2 does: a single client per process,
// short bounded timeouts on every operation, and a small MaxRetries so a single
// slow node can't snowball into a connection-pool stall across the cluster.
type AerospikeClient struct {
	client          *as.Client
	namespace       string
	logger          *slog.Logger
	policy          *as.ClientPolicy
	readTimeoutMs   int
	writeTimeoutMs  int
	batchTimeoutMs  int
	socketTimeoutMs int
}

// defaults applied when the corresponding cfg.* field is 0.
const (
	defaultReadTimeoutMs   = 3000  // single-record reads should be sub-second under healthy load
	defaultWriteTimeoutMs  = 5000  // writes include replication round-trips
	defaultBatchTimeoutMs  = 15000 // batch ops legitimately take longer
	defaultSocketTimeoutMs = 5000  // per-attempt socket cap
	defaultIdleTimeoutSec  = 55    // < server proto-fd-idle-ms so client reaps first
)

// NewAerospikeClient creates a new Aerospike client wrapper from a single seed.
// Prefer NewAerospikeClientFromConfig for production: it picks up the connection
// pool sizing and per-operation timeouts that prevent the
// "connection pool is empty" failure mode under bursty load.
func NewAerospikeClient(host string, port int, namespace string, maxRetries int, retryBaseMs int, logger *slog.Logger) (*AerospikeClient, error) {
	policy := as.NewClientPolicy()
	policy.IdleTimeout = defaultIdleTimeoutSec * time.Second

	client, err := as.NewClientWithPolicy(policy, host, port)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Aerospike: %w", err)
	}

	return &AerospikeClient{
		client:          client,
		namespace:       namespace,
		logger:          logger,
		policy:          policy,
		readTimeoutMs:   defaultReadTimeoutMs,
		writeTimeoutMs:  defaultWriteTimeoutMs,
		batchTimeoutMs:  defaultBatchTimeoutMs,
		socketTimeoutMs: defaultSocketTimeoutMs,
	}, nil
}

// NewAerospikeClientFromConfig builds a client using the full AerospikeConfig.
// The two settings that actually move the needle on the
// "connection pool is empty" failure mode are:
//
//   - ConnectionQueueSize: per-node pool size. Aerospike Go default is 100,
//     trivially exhausted by bursty BatchGet fan-out. Bumped to ~256+.
//   - IdleTimeout: client-side reaping of idle pooled connections. Required
//     when the server is configured with proto-fd-idle-ms=0 (no idle reap),
//     otherwise a transient burst leaves connections piled up forever and
//     starves new requests.
//
// Per-operation TotalTimeout/SocketTimeout are applied via {Read,Write,Batch}Policy
// so a single slow node can't hold connections across many client-side retries.
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
	idleSec := cfg.IdleTimeoutSec
	if idleSec <= 0 {
		idleSec = defaultIdleTimeoutSec
	}
	policy.IdleTimeout = time.Duration(idleSec) * time.Second

	// MinConnectionsPerNode and LimitConnectionsToQueueSize are intentionally
	// left at Aerospike defaults — overriding either has triggered intermittent
	// bootstrap timeouts in microservice deployments. The pool-pressure fix
	// lives in ConnectionQueueSize + per-operation TotalTimeout.

	client, err := as.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Aerospike (seeds=%v): %w", seeds, err)
	}

	c := &AerospikeClient{
		client:          client,
		namespace:       cfg.Namespace,
		logger:          logger,
		policy:          policy,
		readTimeoutMs:   nz(cfg.ReadTimeoutMs, defaultReadTimeoutMs),
		writeTimeoutMs:  nz(cfg.WriteTimeoutMs, defaultWriteTimeoutMs),
		batchTimeoutMs:  nz(cfg.BatchTimeoutMs, defaultBatchTimeoutMs),
		socketTimeoutMs: nz(cfg.SocketTimeoutMs, defaultSocketTimeoutMs),
	}

	logger.Info("aerospike client initialized",
		"seeds", seeds,
		"port", cfg.Port,
		"connectionQueueSize", policy.ConnectionQueueSize,
		"idleTimeoutSec", idleSec,
		"readTimeoutMs", c.readTimeoutMs,
		"writeTimeoutMs", c.writeTimeoutMs,
		"batchTimeoutMs", c.batchTimeoutMs,
		"socketTimeoutMs", c.socketTimeoutMs,
	)

	return c, nil
}

func nz(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func (c *AerospikeClient) Client() *as.Client { return c.client }
func (c *AerospikeClient) Namespace() string  { return c.namespace }
func (c *AerospikeClient) Close()             { c.client.Close() }

func (c *AerospikeClient) Healthy() bool {
	return c.client.IsConnected()
}

// ReadPolicy builds a BasePolicy for single-record reads. The Aerospike Go
// client's default TotalTimeout is 1s with MaxRetries=2 — way too tight under
// any contention, and the retries hold connections for ~3s on failure. Use
// this in place of `nil` policy on every Get/Exists call.
func (c *AerospikeClient) ReadPolicy() *as.BasePolicy {
	p := as.NewPolicy()
	p.TotalTimeout = time.Duration(c.readTimeoutMs) * time.Millisecond
	p.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	p.MaxRetries = 2
	return p
}

// WritePolicy returns a write policy with retry + timeout settings.
func (c *AerospikeClient) WritePolicy(maxRetries int, retryBaseMs int) *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.MaxRetries = maxRetries
	wp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	wp.TotalTimeout = time.Duration(c.writeTimeoutMs) * time.Millisecond
	wp.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	return wp
}

// BatchPolicy returns a batch policy with conservative retry + bounded timeouts.
// MaxRetries is intentionally capped at 1 regardless of the legacy maxRetries
// argument — batch failures amplify connection pressure (every retry re-fans
// out to every node), and the application layer handles retries through the
// subtree-work Kafka topic instead.
func (c *AerospikeClient) BatchPolicy(maxRetries int, retryBaseMs int) *as.BatchPolicy {
	bp := as.NewBatchPolicy()
	if maxRetries > 1 {
		maxRetries = 1
	}
	bp.MaxRetries = maxRetries
	bp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	bp.TotalTimeout = time.Duration(c.batchTimeoutMs) * time.Millisecond
	bp.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	return bp
}
