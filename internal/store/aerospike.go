package store

import (
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
)

// AerospikeClient wraps the Aerospike client with connection pool, retry policy, and health check.
type AerospikeClient struct {
	client    *as.Client
	namespace string
	logger    *slog.Logger
	policy    *as.ClientPolicy
}

// NewAerospikeClient creates a new Aerospike client wrapper.
func NewAerospikeClient(host string, port int, namespace string, maxRetries int, retryBaseMs int, logger *slog.Logger) (*AerospikeClient, error) {
	// Create client policy with retry settings
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
	return wp
}

// BatchPolicy returns a batch policy with retry settings.
func (c *AerospikeClient) BatchPolicy(maxRetries int, retryBaseMs int) *as.BatchPolicy {
	bp := as.NewBatchPolicy()
	bp.MaxRetries = maxRetries
	bp.SleepBetweenRetries = time.Duration(retryBaseMs) * time.Millisecond
	return bp
}
