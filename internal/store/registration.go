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

	// callbackEntryURLKey / callbackEntryTokenKey are the Aerospike CDT-map
	// keys used by the new (url, token) entry shape stored in callbacksBin.
	// Short single-character keys keep the on-wire payload small.
	callbackEntryURLKey   = "u"
	callbackEntryTokenKey = "t"
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
func NewRegistrationStore(client *AerospikeClient, setName string, maxRetries, retryBaseMs, maxCallbacksPerTxID int, logger *slog.Logger) RegistrationStore {
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

// Add registers a (callbackURL, callbackToken) entry for a txid.
//
// Storage shape: callbacksBin holds an Aerospike CDT list of map entries
// {u: url, t: token}. Set-semantics (one entry per URL) are enforced via a
// read-modify-write under generation CAS rather than the previous
// ListWriteFlagsAddUnique trick — Aerospike's UNIQUE flag matches on whole-
// element equality, which broke once we promoted entries from bare strings
// to maps (different tokens for the same URL would each be considered
// distinct elements). The CAS loop keeps idempotent-on-URL semantics and
// also lets a re-registration refresh the token.
//
// Backwards compatibility: the reader (Get / BatchGet) still accepts legacy
// bare-string entries written by older deployments — those decode to a
// CallbackEntry with Token = "". A re-registration of an existing URL
// rewrites the entire list in the new map shape, migrating the record on
// next /watch.
//
// Concurrency: the count + idempotency check + write all run under
// EXPECT_GEN_EQUAL. A concurrent writer that wins our race trips
// GENERATION_ERROR / KEY_EXISTS_ERROR and we re-read and re-decide.
func (s *aerospikeRegistration) Add(txid, callbackURL, callbackToken string) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

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

		entries := parseCallbackEntries(existing)

		// Build the next list. If the URL is already present, refresh its
		// token (idempotent re-registration may rotate the token); otherwise
		// append. This both migrates legacy bare-string entries to the new
		// map shape and keeps set-on-URL semantics.
		next := make([]interface{}, 0, len(entries)+1)
		found := false
		for _, e := range entries {
			if e.URL == callbackURL {
				found = true
				next = append(next, encodeCallbackEntry(callbackURL, callbackToken))
			} else {
				next = append(next, encodeCallbackEntry(e.URL, e.Token))
			}
		}
		if !found {
			if s.maxCallbacksPerTxID > 0 && len(entries) >= s.maxCallbacksPerTxID {
				return ErrMaxCallbacksPerTxIDExceeded
			}
			next = append(next, encodeCallbackEntry(callbackURL, callbackToken))
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

		bins := as.BinMap{callbacksBin: next}
		if err := s.client.Client().Put(wp, key, bins); err != nil {
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

// encodeCallbackEntry produces the Aerospike map-shape representation for a
// (url, token) pair. Tokens are stored unconditionally (including ""); the
// reader treats a missing or empty token field as Token = "".
func encodeCallbackEntry(url, token string) map[interface{}]interface{} {
	return map[interface{}]interface{}{
		callbackEntryURLKey:   url,
		callbackEntryTokenKey: token,
	}
}

// parseCallbackEntries decodes a callbacksBin list into CallbackEntry values.
// Accepts both the legacy bare-string shape (token = "") and the new map
// shape {u: url, t: token}. Anything that doesn't match either shape is
// skipped — a defensive choice for forward-compat with future schema changes.
func parseCallbackEntries(list []interface{}) []CallbackEntry {
	if len(list) == 0 {
		return nil
	}
	entries := make([]CallbackEntry, 0, len(list))
	for _, v := range list {
		switch tv := v.(type) {
		case string:
			// Legacy bare-string entry: no token.
			entries = append(entries, CallbackEntry{URL: tv})
		case map[interface{}]interface{}:
			url, _ := tv[callbackEntryURLKey].(string)
			token, _ := tv[callbackEntryTokenKey].(string)
			if url == "" {
				continue
			}
			entries = append(entries, CallbackEntry{URL: url, Token: token})
		}
	}
	return entries
}

// Get returns all (url, token) registrations for a txid. Accepts both the
// new {u, t} map entry shape and the legacy bare-string shape (token = "")
// so an in-flight rolling deploy never 401s a callback that hasn't been
// rewritten yet.
func (s *aerospikeRegistration) Get(txid string) ([]CallbackEntry, error) {
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

	return parseCallbackEntries(list), nil
}

// BatchGet returns (url, token) registrations for multiple txids in a single
// batch call. Same dual-shape parsing as Get.
func (s *aerospikeRegistration) BatchGet(txids []string) (map[string][]CallbackEntry, error) {
	if len(txids) == 0 {
		return make(map[string][]CallbackEntry), nil
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

	result := make(map[string][]CallbackEntry)
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
		entries := parseCallbackEntries(list)
		if len(entries) > 0 {
			result[txids[i]] = entries
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
