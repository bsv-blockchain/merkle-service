package store

import (
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
)

const (
	callbacksBin = "callbacks"
)

// aerospikeRegistration is the Aerospike-backed RegistrationStore implementation.
type aerospikeRegistration struct {
	client      *AerospikeClient
	setName     string
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
}

// Compile-time check: aerospikeRegistration satisfies RegistrationStore.
var _ RegistrationStore = (*aerospikeRegistration)(nil)

// NewRegistrationStore constructs an Aerospike-backed RegistrationStore.
func NewRegistrationStore(client *AerospikeClient, setName string, maxRetries int, retryBaseMs int, logger *slog.Logger) RegistrationStore {
	return &aerospikeRegistration{
		client:      client,
		setName:     setName,
		logger:      logger,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
	}
}

// Add registers a callback URL for a txid using CDT list operations with UNIQUE flag to ensure set semantics.
func (s *aerospikeRegistration) Add(txid string, callbackURL string) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE

	// Use ListAppendWithPolicyOp with REPLACE to simulate set behavior
	// (ListOrderOrdered + ADD_UNIQUE flag)
	listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
	ops := []*as.Operation{
		as.ListAppendWithPolicyOp(listPolicy, callbacksBin, callbackURL),
	}

	_, err = s.client.Client().Operate(wp, key, ops...)
	if err != nil {
		return fmt.Errorf("failed to add registration: %w", err)
	}

	return nil
}

// Get returns all callback URLs registered for a txid.
func (s *aerospikeRegistration) Get(txid string) ([]string, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	record, err := s.client.Client().Get(nil, key, callbacksBin)
	if err != nil {
		return nil, fmt.Errorf("failed to get registration: %w", err)
	}
	if record == nil {
		return nil, nil
	}

	binVal := record.Bins[callbacksBin]
	if binVal == nil {
		return nil, nil
	}

	list, ok := binVal.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected bin type for callbacks")
	}

	urls := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			urls = append(urls, s)
		}
	}
	return urls, nil
}

// BatchGet returns callback URLs for multiple txids in a single batch call.
func (s *aerospikeRegistration) BatchGet(txids []string) (map[string][]string, error) {
	if len(txids) == 0 {
		return make(map[string][]string), nil
	}

	keys := make([]*as.Key, len(txids))
	for i, txid := range txids {
		key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
		if err != nil {
			return nil, fmt.Errorf("failed to create key for %s: %w", txid, err)
		}
		keys[i] = key
	}

	bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
	records, err := s.client.Client().BatchGet(bp, keys, callbacksBin)
	if err != nil {
		return nil, fmt.Errorf("batch get failed: %w", err)
	}

	result := make(map[string][]string)
	for i, record := range records {
		if record == nil {
			continue
		}
		binVal := record.Bins[callbacksBin]
		if binVal == nil {
			continue
		}
		list, ok := binVal.([]interface{})
		if !ok {
			continue
		}
		urls := make([]string, 0, len(list))
		for _, v := range list {
			if s, ok := v.(string); ok {
				urls = append(urls, s)
			}
		}
		if len(urls) > 0 {
			result[txids[i]] = urls
		}
	}

	return result, nil
}

// UpdateTTL updates the TTL of a registration record.
func (s *aerospikeRegistration) UpdateTTL(txid string, ttl time.Duration) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.Expiration = uint32(ttl.Seconds())

	ops := []*as.Operation{
		as.TouchOp(),
	}

	_, err = s.client.Client().Operate(wp, key, ops...)
	if err != nil {
		return fmt.Errorf("failed to update TTL: %w", err)
	}
	return nil
}

// BatchUpdateTTL updates TTL for multiple txids in batch.
func (s *aerospikeRegistration) BatchUpdateTTL(txids []string, ttl time.Duration) error {
	if len(txids) == 0 {
		return nil
	}

	for _, txid := range txids {
		if err := s.UpdateTTL(txid, ttl); err != nil {
			s.logger.Warn("failed to update TTL (check Aerospike nsup-period config)", "txid", txid, "error", err)
		}
	}
	return nil
}
