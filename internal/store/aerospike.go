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
	defaultReadTimeoutMs   = 1000 // sub-second by design — fail fast and let the app retry
	defaultWriteTimeoutMs  = 2000 // writes include replication round-trips
	defaultBatchTimeoutMs  = 3000 // batch fan-out across the cluster; one slow node shouldn't block 15s
	defaultSocketTimeoutMs = 1000 // per-attempt socket cap
	defaultIdleTimeoutSec  = 55   // < server proto-fd-idle-ms so client reaps first
	// Circuit-breaker defaults. With Aerospike's stock MaxErrorRate=100/s a
	// transiently-sick node never trips the breaker — every client keeps
	// pounding it, holding pool slots open until each individual op times out.
	// 5 errors/window flips the breaker fast enough that we route around the
	// node instead of stacking timeouts on it.
	defaultMaxErrorRate    = 5
	defaultErrorRateWindow = 1
)

// NewAerospikeClient creates a new Aerospike client wrapper from a single seed.
// Prefer NewAerospikeClientFromConfig for production: it picks up the connection
// pool sizing and per-operation timeouts that prevent the
// "connection pool is empty" failure mode under bursty load.
func NewAerospikeClient(host string, port int, namespace string, maxRetries, retryBaseMs int, logger *slog.Logger) (*AerospikeClient, error) {
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

	// Aggressive node-level circuit breaker. When one node in a multi-tenant
	// cluster is squeezed by a different namespace's traffic, our requests
	// would otherwise pile timeouts on it for as long as our app-level retries
	// keep firing. Tripping the breaker after a handful of errors makes the
	// client return MAX_ERROR_RATE immediately for affected partitions so the
	// pool isn't held hostage by the bad node.
	policy.MaxErrorRate = nz(cfg.MaxErrorRate, defaultMaxErrorRate)
	policy.ErrorRateWindow = nz(cfg.ErrorRateWindow, defaultErrorRateWindow)

	// MinConnectionsPerNode and LimitConnectionsToQueueSize are intentionally
	// left at Aerospike defaults — overriding either has triggered intermittent
	// bootstrap timeouts in microservice deployments. The pool-pressure fix
	// lives in ConnectionQueueSize + per-operation TotalTimeout + circuit breaker.

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
		"maxErrorRate", policy.MaxErrorRate,
		"errorRateWindow", policy.ErrorRateWindow,
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
func (c *AerospikeClient) WritePolicy(maxRetries, retryBaseMs int) *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.MaxRetries = maxRetries
	wp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	wp.TotalTimeout = time.Duration(c.writeTimeoutMs) * time.Millisecond
	wp.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	return wp
}

// BatchPolicy returns a batch policy with NO in-client retries and tight
// bounded timeouts. Batch failures amplify connection pressure exponentially
// — every retry re-fans out to every node, and a single sick partition
// owner means we'd retry against the same dead node, holding pool slots open
// for the full timeout × retry count. The application handles retries via
// Kafka (subtree-work / subtree topic) which is rate-limited, idempotent,
// and doesn't pile up on the unhealthy node.
func (c *AerospikeClient) BatchPolicy(maxRetries, retryBaseMs int) *as.BatchPolicy {
	bp := as.NewBatchPolicy()
	bp.MaxRetries = 0
	bp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	bp.TotalTimeout = time.Duration(c.batchTimeoutMs) * time.Millisecond
	bp.SocketTimeout = time.Duration(c.socketTimeoutMs) * time.Millisecond
	return bp
}
