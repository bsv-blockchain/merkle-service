package store

import (
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v7"
)

const (
	seenSubtreesBin    = "subtrees"
	seenThresholdFired = "tfired"
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

// Increment idempotently records that a txid was seen in a specific subtree.
// Uses Aerospike CDT list with AddUnique to ensure each subtreeID is counted only once.
// ThresholdReached is true only once: when the unique count first reaches the threshold.
func (s *aerospikeSeenCounter) Increment(txid string, subtreeID string) (*IncrementResult, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE

	// AddUnique + NoFail: appends subtreeID only if not already present, no error on duplicate.
	listPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
	ops := []*as.Operation{
		as.ListAppendWithPolicyOp(listPolicy, seenSubtreesBin, subtreeID),
		as.ListSizeOp(seenSubtreesBin),
		as.GetBinOp(seenThresholdFired),
	}

	record, err := s.client.Client().Operate(wp, key, ops...)
	if err != nil {
		return nil, fmt.Errorf("failed to increment seen counter: %w", err)
	}

	// When multiple operations target the same bin, Aerospike returns results
	// as []interface{}. Index 0 = ListAppend result, index 1 = ListSize result.
	var newSize int
	switch v := record.Bins[seenSubtreesBin].(type) {
	case int:
		newSize = v
	case []interface{}:
		if len(v) < 2 {
			return nil, fmt.Errorf("expected at least 2 results for seen counter ops, got %d", len(v))
		}
		size, ok := v[1].(int)
		if !ok {
			return nil, fmt.Errorf("unexpected type for list size result: %T", v[1])
		}
		newSize = size
	default:
		return nil, fmt.Errorf("unexpected type for seen counter list size: %T", v)
	}

	// Check if threshold was already fired previously.
	alreadyFired := false
	if firedVal := record.Bins[seenThresholdFired]; firedVal != nil {
		if v, ok := firedVal.(int); ok && v == 1 {
			alreadyFired = true
		}
	}

	thresholdReached := false
	if newSize >= s.threshold && !alreadyFired {
		// Mark threshold as fired so it won't fire again.
		thresholdReached = true
		markWP := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
		markWP.RecordExistsAction = as.UPDATE
		_ = s.client.Client().Put(markWP, key, as.BinMap{seenThresholdFired: 1})
	}

	return &IncrementResult{
		NewCount:         newSize,
		ThresholdReached: thresholdReached,
	}, nil
}

// Threshold returns the configured threshold.
func (s *aerospikeSeenCounter) Threshold() int {
	return s.threshold
}
