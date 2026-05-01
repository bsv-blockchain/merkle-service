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

func NewSeenCounterStore(client *AerospikeClient, setName string, threshold int, maxRetries int, retryBaseMs int, logger *slog.Logger) SeenCounterStore {
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
func (s *aerospikeSeenCounter) Increment(txid string, subtreeID string) (*IncrementResult, error) {
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
