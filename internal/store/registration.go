package store

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
	"golang.org/x/sync/errgroup"

	"github.com/bsv-blockchain/merkle-service/internal/logfields"
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

// aerospikeBatchChunkSize caps the number of keys sent in a single Aerospike
// batch call. Aerospike rejects any batch that lands more than
// batch-max-requests keys (server default: 5000) on a single node with
// BATCH_MAX_REQUESTS_EXCEEDED — deterministically, on every retry. A
// teranode-default subtree carries ~1M txids, so an unchunked whole-subtree
// BatchGet can never succeed on clusters smaller than ~200 nodes. Chunking to
// the per-node default is safe for any cluster size (a chunk's keys can at
// worst all hash to one node).
const aerospikeBatchChunkSize = 5000

// chunkSlice splits items into consecutive sub-slices of at most size
// elements. The sub-slices share the backing array (no copying).
func chunkSlice(items []string, size int) [][]string {
	if len(items) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(items)+size-1)/size)
	for start := 0; start < len(items); start += size {
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[start:end])
	}
	return chunks
}

// forEachChunkConcurrent splits items into <=aerospikeBatchChunkSize chunks and invokes fn on
// each, running up to `concurrency` chunks at once. concurrency<=1 (or a single
// chunk) runs them serially with no goroutine overhead. Every chunk is attempted
// regardless of errors — matching the best-effort batch loops this replaces —
// and the first error any fn reports is returned.
//
// fn MUST be safe for concurrent invocation: confine its writes to chunk-disjoint
// state or synchronize them. Keys are disjoint across chunks, so callers merge
// per-chunk results under a mutex.
func forEachChunkConcurrent(items []string, concurrency int, fn func(chunk []string) error) error {
	chunks := chunkSlice(items, aerospikeBatchChunkSize)
	if concurrency <= 1 || len(chunks) <= 1 {
		var firstErr error
		for _, chunk := range chunks {
			if err := fn(chunk); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	g := new(errgroup.Group)
	g.SetLimit(concurrency)
	for _, chunk := range chunks {
		g.Go(func() error { return fn(chunk) })
	}
	return g.Wait()
}

// BatchGet returns (url, token) registrations for multiple txids, issuing one
// Aerospike batch call per aerospikeBatchChunkSize keys, up to
// batchChunkConcurrency chunks concurrently. Same dual-shape parsing as Get.
// Any chunk error fails the whole call (the caller redelivers; all ops are
// idempotent reads).
func (s *aerospikeRegistration) BatchGet(txids []string) (map[string][]CallbackEntry, error) {
	result := make(map[string][]CallbackEntry)
	var mu sync.Mutex
	err := forEachChunkConcurrent(txids, s.client.batchChunkConcurrency,
		func(chunk []string) error {
			// Per-chunk local map: chunk key sets are disjoint, so we merge into
			// the shared result under a mutex rather than write the map concurrently.
			local := make(map[string][]CallbackEntry, len(chunk))
			if cErr := s.batchGetChunk(chunk, local); cErr != nil {
				return cErr
			}
			mu.Lock()
			for k, v := range local {
				result[k] = v
			}
			mu.Unlock()
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// batchGetChunk issues one Aerospike BatchGet for a chunk of at most
// aerospikeBatchChunkSize txids and merges positive results into result.
func (s *aerospikeRegistration) batchGetChunk(txids []string, result map[string][]CallbackEntry) error {
	keys := make([]*as.Key, len(txids))
	for i, txid := range txids {
		key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
		if err != nil {
			return fmt.Errorf("failed to create key for %s: %w", txid, err)
		}
		keys[i] = key
	}

	bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
	records, err := s.client.Client().BatchGet(bp, keys, callbacksBin)
	if err != nil {
		return fmt.Errorf("batch get failed: %w", err)
	}

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

	return nil
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

// BatchUpdateTTL updates TTL for multiple txids using chunked batch Touch
// operations — one round-trip per node per aerospikeBatchChunkSize keys
// instead of one Operate RTT per txid (throughput review F-8: the serial loop
// blocked the subtree-worker hot path ~5-10s per 10k-txid subtree). Failures
// remain warn-only, preserving the prior best-effort contract.
func (s *aerospikeRegistration) BatchUpdateTTL(txids []string, ttl time.Duration) error {
	if len(txids) == 0 {
		return nil
	}

	wpol := as.NewBatchWritePolicy()
	wpol.Expiration = uint32(ttl.Seconds())

	for _, chunk := range chunkSlice(txids, aerospikeBatchChunkSize) {
		batchRecs := make([]as.BatchRecordIfc, 0, len(chunk))
		batchTxids := make([]string, 0, len(chunk)) // index-aligned with batchRecs
		for _, txid := range chunk {
			key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
			if err != nil {
				s.logger.Warn("failed to create key for TTL update", logfields.TxID(txid), "error", err)
				continue
			}
			batchRecs = append(batchRecs, as.NewBatchWrite(wpol, key, as.TouchOp()))
			batchTxids = append(batchTxids, txid)
		}
		if len(batchRecs) == 0 {
			continue
		}

		bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
		if err := s.client.Client().BatchOperate(bp, batchRecs); err != nil {
			s.logger.Warn("batch TTL update failed (check Aerospike nsup-period config)",
				"keys", len(batchRecs), "error", err)
			continue
		}
		for i, br := range batchRecs {
			if err := br.BatchRec().Err; err != nil {
				s.logger.Warn("failed to update TTL (check Aerospike nsup-period config)",
					logfields.TxID(batchTxids[i]), "error", err)
			}
		}
	}
	return nil
}
