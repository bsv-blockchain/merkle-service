package store

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

const (
	callbacksBin = "callbacks"
)

// ErrMaxCallbacksPerTxIDExceeded is returned by RegistrationStore.Add when
// the configured per-txid callback URL cap (RegistryConfig.MaxCallbacksPerTxID)
// has already been reached for the txid and the URL being registered is not
// already present. Adding a URL that is already registered is treated as a
// no-op success and never produces this error. F-050.
var ErrMaxCallbacksPerTxIDExceeded = errors.New("max callbacks per txid exceeded")

// aerospikeRegistration is the Aerospike-backed RegistrationStore implementation.
type aerospikeRegistration struct {
	client              *AerospikeClient
	setName             string
	logger              *slog.Logger
	maxRetries          int
	retryBaseMs         int
	maxCallbacksPerTxID int
}

// Compile-time check: aerospikeRegistration satisfies RegistrationStore.
var _ RegistrationStore = (*aerospikeRegistration)(nil)

// NewRegistrationStore constructs an Aerospike-backed RegistrationStore.
//
// maxCallbacksPerTxID caps how many distinct callback URLs may be registered
// against a single txid. 0 disables the cap (legacy unbounded behavior —
// strongly discouraged; see F-050). Adds that would exceed the cap return
// ErrMaxCallbacksPerTxIDExceeded; idempotent re-adds of an already-registered
// URL succeed regardless of the cap.
func NewRegistrationStore(client *AerospikeClient, setName string, maxRetries int, retryBaseMs int, maxCallbacksPerTxID int, logger *slog.Logger) RegistrationStore {
	if maxCallbacksPerTxID < 0 {
		maxCallbacksPerTxID = 0
	}
	return &aerospikeRegistration{
		client:              client,
		setName:             setName,
		logger:              logger,
		maxRetries:          maxRetries,
		retryBaseMs:         retryBaseMs,
		maxCallbacksPerTxID: maxCallbacksPerTxID,
	}
}

// addCASMaxAttempts caps how many times Add will retry the optimistic
// generation-CAS append cycle when a concurrent writer changes the record
// between our read and write. A small bound is sufficient because the only
// contention is multiple registrations for the same txid in flight at once —
// in practice we rarely see more than two or three such collisions before the
// loser finds the URL already present (idempotent success).
const addCASMaxAttempts = 5

// Add registers a callback URL for a txid using CDT list operations with the
// UNIQUE flag for set semantics. When maxCallbacksPerTxID > 0, the read-and-
// write is gated by an optimistic generation CAS so concurrent registrations
// can't both observe (count == max-1) and both succeed past the cap.
func (s *aerospikeRegistration) Add(txid string, callbackURL string) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	// Cap disabled: preserve the original best-effort append, which already
	// honors set semantics via ListWriteFlagsAddUnique|NoFail.
	if s.maxCallbacksPerTxID <= 0 {
		wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
		wp.RecordExistsAction = as.UPDATE
		listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
		ops := []*as.Operation{as.ListAppendWithPolicyOp(listPolicy, callbacksBin, callbackURL)}
		if _, err := s.client.Client().Operate(wp, key, ops...); err != nil {
			return fmt.Errorf("failed to add registration: %w", err)
		}
		return nil
	}

	// Cap enabled: read current list + record generation, decide, then write
	// under EXPECT_GEN_EQUAL. Loop on generation mismatch.
	for attempt := 0; attempt < addCASMaxAttempts; attempt++ {
		record, err := s.client.Client().Get(s.client.ReadPolicy(), key, callbacksBin)
		if err != nil {
			var asErr *as.AerospikeError
			if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
				record = nil
			} else {
				return fmt.Errorf("failed to read registration: %w", err)
			}
		}

		var existing []interface{}
		var generation uint32
		if record != nil {
			generation = record.Generation
			if v, ok := record.Bins[callbacksBin].([]interface{}); ok {
				existing = v
			}
		}

		// Idempotent: already registered, nothing to do (and the cap doesn't apply).
		for _, v := range existing {
			if s, ok := v.(string); ok && s == callbackURL {
				return nil
			}
		}

		if len(existing) >= s.maxCallbacksPerTxID {
			return ErrMaxCallbacksPerTxIDExceeded
		}

		wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
		if record == nil {
			// Create-or-fail-if-exists: another writer beating us to record creation
			// will trip a generation/exists error; we'll retry and re-evaluate.
			wp.RecordExistsAction = as.CREATE_ONLY
		} else {
			wp.RecordExistsAction = as.UPDATE
			wp.GenerationPolicy = as.EXPECT_GEN_EQUAL
			wp.Generation = generation
		}

		listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
		ops := []*as.Operation{as.ListAppendWithPolicyOp(listPolicy, callbacksBin, callbackURL)}

		if _, err := s.client.Client().Operate(wp, key, ops...); err != nil {
			var asErr *as.AerospikeError
			if errors.As(err, &asErr) {
				if asErr.Matches(astypes.GENERATION_ERROR, astypes.KEY_EXISTS_ERROR) {
					// Concurrent writer beat us — re-read and re-decide.
					continue
				}
			}
			return fmt.Errorf("failed to add registration: %w", err)
		}
		return nil
	}
	// Persistent contention: surface as a transient error rather than silently
	// dropping the registration. The caller may retry.
	return fmt.Errorf("failed to add registration: generation contention after %d attempts", addCASMaxAttempts)
}

// Get returns all callback URLs registered for a txid.
func (s *aerospikeRegistration) Get(txid string) ([]string, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	record, err := s.client.Client().Get(s.client.ReadPolicy(), key, callbacksBin)
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
