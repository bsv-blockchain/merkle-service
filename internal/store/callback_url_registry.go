package store

import (
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v7"
)

const (
	callbackURLRegistryKey = "__all_urls__"
	callbackURLsBin        = "urls"
)

// aerospikeCallbackURLRegistry is the Aerospike-backed CallbackURLRegistry
// implementation. Uses a single record with a CDT list for efficient enumeration.
type aerospikeCallbackURLRegistry struct {
	client      *AerospikeClient
	setName     string
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
}

var _ CallbackURLRegistry = (*aerospikeCallbackURLRegistry)(nil)

// NewCallbackURLRegistry creates a new Aerospike-backed callback URL registry.
func NewCallbackURLRegistry(client *AerospikeClient, setName string, maxRetries int, retryBaseMs int, logger *slog.Logger) CallbackURLRegistry {
	return &aerospikeCallbackURLRegistry{
		client:      client,
		setName:     setName,
		logger:      logger,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
	}
}

// Add registers a callback URL in the registry. Duplicates are silently ignored.
func (r *aerospikeCallbackURLRegistry) Add(callbackURL string) error {
	key, err := as.NewKey(r.client.Namespace(), r.setName, callbackURLRegistryKey)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := r.client.WritePolicy(r.maxRetries, r.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE

	listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
	ops := []*as.Operation{
		as.ListAppendWithPolicyOp(listPolicy, callbackURLsBin, callbackURL),
	}

	_, err = r.client.Client().Operate(wp, key, ops...)
	if err != nil {
		return fmt.Errorf("failed to add callback URL to registry: %w", err)
	}

	return nil
}

// GetAll returns all registered callback URLs.
func (r *aerospikeCallbackURLRegistry) GetAll() ([]string, error) {
	key, err := as.NewKey(r.client.Namespace(), r.setName, callbackURLRegistryKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	record, err := r.client.Client().Get(r.client.ReadPolicy(), key, callbackURLsBin)
	if err != nil {
		return nil, fmt.Errorf("failed to get callback URLs: %w", err)
	}
	if record == nil {
		return nil, nil
	}

	binVal := record.Bins[callbackURLsBin]
	if binVal == nil {
		return nil, nil
	}

	list, ok := binVal.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected bin type for callback URLs: %T", binVal)
	}

	urls := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			urls = append(urls, s)
		}
	}
	return urls, nil
}
