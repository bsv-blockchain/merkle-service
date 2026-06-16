package store

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	seenSubtreesBin    = "subtrees"
	seenThresholdFired = "tfired"

	// seenCASMaxAttempts caps the generation-checked retry loop. Real-world
	// contention is bounded by the number of concurrent subtree workers per
	// txid; 32 is generous and keeps a runaway loop from holding a connection.
	seenCASMaxAttempts = 32
)

// aerospikeSeenCounter is the Aerospike-backed SeenCounterStore implementation.
type aerospikeSeenCounter struct {
	client      *AerospikeClient
	setName     string
	threshold   int
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
}

var _ SeenCounterStore = (*aerospikeSeenCounter)(nil)

func NewSeenCounterStore(client *AerospikeClient, setName string, threshold, maxRetries, retryBaseMs int, logger *slog.Logger) SeenCounterStore {
	return &aerospikeSeenCounter{
		client:      client,
		setName:     setName,
		threshold:   threshold,
		logger:      logger,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
	}
}

// Increment idempotently records that a txid was seen in a specific subtree
// and atomically transitions the threshold-fired flag from false to true the
// first time the unique-subtree count reaches the threshold. ThresholdReached
// is true only on the single call that observes that 0->1 transition.
//
// The atomic transition is implemented as a generation-checked
// read-modify-write: each attempt reads the record (capturing generation),
// computes the next state locally, and issues an Operate with
// GenerationPolicy=EXPECT_GEN_EQUAL so a concurrent writer that bumped the
// generation in between forces a retry. F-045: previously the threshold check
// and the marker write were two unrelated operations, so two concurrent
// observations could both pass `alreadyFired == false` and emit duplicate
// SEEN_MULTIPLE_NODES callbacks.
func (s *aerospikeSeenCounter) Increment(txid, subtreeID string) (*IncrementResult, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	for attempt := 0; attempt < seenCASMaxAttempts; attempt++ {
		// Step 1: read current state + generation. Treat KEY_NOT_FOUND as a
		// brand-new record (generation 0, empty subtree list, fired=0).
		readPolicy := s.client.ReadPolicy()
		current, err := s.client.Client().Get(readPolicy, key, seenSubtreesBin, seenThresholdFired)
		if err != nil {
			var asErr as.Error
			if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
				current = nil
			} else {
				return nil, fmt.Errorf("failed to read seen counter: %w", err)
			}
		}

		var (
			gen           uint32
			priorFired    bool
			currentMember bool
			currentSize   int
		)
		if current != nil {
			gen = current.Generation
			if firedVal, ok := current.Bins[seenThresholdFired].(int); ok && firedVal == 1 {
				priorFired = true
			}
			if list, ok := current.Bins[seenSubtreesBin].([]interface{}); ok {
				currentSize = len(list)
				for _, v := range list {
					if str, ok := v.(string); ok && str == subtreeID {
						currentMember = true
						break
					}
				}
			}
		}

		// Step 2: compute next state locally. AddUnique semantics: only count
		// distinct subtreeIDs.
		newSize := currentSize
		if !currentMember {
			newSize++
		}
		shouldFire := !priorFired && newSize >= s.threshold

		// Step 3: write next state with EXPECT_GEN_EQUAL. We always update the
		// subtree list (idempotent ListAppend with AddUnique|NoFail handles
		// re-runs) and only set tfired=1 when this attempt observed the
		// 0->threshold transition. New records skip the generation check via
		// CREATE_ONLY so two concurrent first-writers also resolve cleanly.
		wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
		if current == nil {
			wp.RecordExistsAction = as.CREATE_ONLY
		} else {
			wp.RecordExistsAction = as.UPDATE
			wp.GenerationPolicy = as.EXPECT_GEN_EQUAL
			wp.Generation = gen
		}

		listPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
		ops := []*as.Operation{
			as.ListAppendWithPolicyOp(listPolicy, seenSubtreesBin, subtreeID),
		}
		if shouldFire {
			ops = append(ops, as.PutOp(as.NewBin(seenThresholdFired, 1)))
		}

		_, err = s.client.Client().Operate(wp, key, ops...)
		if err != nil {
			var asErr as.Error
			if errors.As(err, &asErr) {
				// Generation mismatch (concurrent writer beat us) or
				// CREATE_ONLY collision (two concurrent first-writers): retry
				// with the now-current state.
				if asErr.Matches(astypes.GENERATION_ERROR) || asErr.Matches(astypes.KEY_EXISTS_ERROR) {
					// Tiny backoff to avoid hot-spinning on pathological contention.
					if s.retryBaseMs > 0 {
						time.Sleep(time.Duration(s.retryBaseMs) * time.Millisecond)
					}
					continue
				}
			}
			return nil, fmt.Errorf("failed to write seen counter: %w", err)
		}

		return &IncrementResult{
			NewCount:         newSize,
			ThresholdReached: shouldFire,
		}, nil
	}

	return nil, fmt.Errorf("seen counter CAS exhausted after %d attempts (txid=%s)", seenCASMaxAttempts, txid)
}

// Threshold returns the configured threshold.
func (s *aerospikeSeenCounter) Threshold() int {
	return s.threshold
}

// BatchIncrement records that every txid in txids was seen in subtreeID using
// two batched phases instead of 2 serial RTTs per txid (throughput review
// F-4: the sequential Increment loop capped the SEEN path at ~500-1000
// txids/s per instance):
//
//	Phase 1 (bulk, common case): one chunked BatchOperate appends subtreeID to
//	every txid's unique-subtree list (idempotent: AddUnique|NoFail), then one
//	chunked BatchGet reads back each record's list size and threshold-fired
//	flag. Two batch round-trips per 5000 txids.
//
//	Phase 2 (rare): only txids whose count has reached the threshold WITHOUT
//	the fired flag set are candidates for the exactly-once 0->1 transition.
//	Each candidate goes through the existing generation-CAS Increment, which
//	preserves the F-045 guarantee: when several workers race, exactly one
//	observes ThresholdReached=true. A txid crosses the threshold at most once
//	in its lifetime, so phase 2 is empty in steady state.
//
// Returns a result for every txid that succeeded plus the first error
// encountered (F-058 partial-success contract: the caller emits callbacks for
// returned results and surfaces the error so the subtree is redelivered; all
// operations here are idempotent under re-runs).
func (s *aerospikeSeenCounter) BatchIncrement(txids []string, subtreeID string) (map[string]*IncrementResult, error) {
	results := make(map[string]*IncrementResult, len(txids))
	if len(txids) == 0 {
		return results, nil
	}

	var firstErr error
	saveErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	for _, chunk := range chunkSlice(txids, aerospikeBatchChunkSize) {
		if err := s.batchIncrementChunk(chunk, subtreeID, results); err != nil {
			saveErr(err)
		}
	}
	return results, firstErr
}

// batchIncrementChunk processes one <=aerospikeBatchChunkSize chunk in a SINGLE
// batch round-trip. Per txid it issues a BatchOperate that both (a) idempotently
// appends subtreeID to the unique-subtree list and (b) reads the threshold-fired
// flag. The list-append operation itself returns the resulting list size
// ("Server returns list size on bin name"), so the post-count is known without a
// second read-back BatchGet — halving the per-chunk RTTs versus the prior
// append-then-BatchGet pair.
//
// Phase 2 (the exactly-once 0->threshold transition) is unchanged: any txid that
// has just reached the threshold without the fired flag set is delegated to the
// generation-CAS Increment, preserving the F-045 guarantee that exactly one
// concurrent observer reports ThresholdReached=true.
func (s *aerospikeSeenCounter) batchIncrementChunk(txids []string, subtreeID string, results map[string]*IncrementResult) error {
	listPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
	batchRecs := make([]as.BatchRecordIfc, len(txids))
	for i, txid := range txids {
		key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
		if err != nil {
			return fmt.Errorf("failed to create key for %s: %w", txid, err)
		}
		// Two ops in one record transaction: the append returns the new
		// unique-subtree count on seenSubtreesBin; GetBinOp reads only the fired
		// flag (a different bin, so its result never collides with the append's).
		batchRecs[i] = as.NewBatchWrite(nil, key,
			as.ListAppendWithPolicyOp(listPolicy, seenSubtreesBin, subtreeID),
			as.GetBinOp(seenThresholdFired),
		)
	}

	bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
	if err := s.client.Client().BatchOperate(bp, batchRecs); err != nil {
		return fmt.Errorf("batch append/read seen counters: %w", err)
	}

	// Per-key failures: skip those txids (no result entry) and surface the first
	// error; the caller's redelivery re-runs the whole idempotent batch.
	var firstErr error
	for i, br := range batchRecs {
		rec := br.BatchRec()
		if rec.Err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("batch seen counter for %s: %w", txids[i], rec.Err)
			}
			continue
		}
		if rec.Record == nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("seen counter for %s missing after batch operate", txids[i])
			}
			continue
		}

		// AddUnique|NoFail: a duplicate subtreeID leaves the count unchanged, so
		// the returned size is the current unique-subtree count either way.
		size := 0
		if v, ok := rec.Record.Bins[seenSubtreesBin].(int); ok {
			size = v
		}
		fired := false
		if firedVal, ok := rec.Record.Bins[seenThresholdFired].(int); ok && firedVal == 1 {
			fired = true
		}

		if size >= s.threshold && !fired {
			// Phase 2: candidate for the exactly-once threshold transition.
			// Delegate to the generation-CAS Increment (idempotent re-append +
			// atomic 0->1 flip); if a concurrent worker fires first, this call
			// returns ThresholdReached=false (F-045).
			res, incErr := s.Increment(txids[i], subtreeID)
			if incErr != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("firing threshold for %s: %w", txids[i], incErr)
				}
				continue
			}
			results[txids[i]] = res
			continue
		}

		results[txids[i]] = &IncrementResult{NewCount: size, ThresholdReached: false}
	}

	return firstErr
}
