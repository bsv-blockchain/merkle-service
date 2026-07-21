package store

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	seenPeersBin       = "peers" // CDT map peerID → weight
	seenThresholdFired = "tfired"

	// seenCASMaxAttempts caps the generation-checked retry loop for the
	// exactly-once threshold transition (F-045).
	seenCASMaxAttempts = 32
)

// aerospikeSeenCounter is the Aerospike-backed peer-weighted SeenCounterStore.
//
// Hot path (BatchAddPeer): one BatchOperate per chunk that MapPut(create-only)
// the peer and reads back the peers map + tfired. Score is summed client-side
// from the tiny peer map (≈5 miners). Phase-2 generation-CAS only runs for
// candidates that just crossed the score threshold (F-045).
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

func (s *aerospikeSeenCounter) Threshold() int { return s.threshold }

// AddPeer records peerID with weight if not already present, with generation-CAS
// for the exactly-once threshold transition.
func (s *aerospikeSeenCounter) AddPeer(txid, peerID string, weight int) (*IncrementResult, error) {
	if weight <= 0 || peerID == "" {
		return &IncrementResult{NewCount: 0, ThresholdReached: false}, nil
	}

	key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	for attempt := 0; attempt < seenCASMaxAttempts; attempt++ {
		readPolicy := s.client.ReadPolicy()
		current, err := s.client.Client().Get(readPolicy, key, seenPeersBin, seenThresholdFired)
		if err != nil {
			var asErr as.Error
			if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
				current = nil
			} else {
				return nil, fmt.Errorf("failed to read seen counter: %w", err)
			}
		}

		var (
			gen        uint32
			priorFired bool
			peers      map[string]int
		)
		if current != nil {
			gen = current.Generation
			if firedVal, ok := current.Bins[seenThresholdFired].(int); ok && firedVal == 1 {
				priorFired = true
			}
			peers = peersMapFromBin(current.Bins[seenPeersBin])
		}
		if peers == nil {
			peers = make(map[string]int, 4)
		}
		if _, ok := peers[peerID]; !ok {
			peers[peerID] = weight
		}
		score := sumPeerWeights(peers)
		shouldFire := !priorFired && score >= s.threshold

		wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
		if current == nil {
			wp.RecordExistsAction = as.CREATE_ONLY
		} else {
			wp.RecordExistsAction = as.UPDATE
			wp.GenerationPolicy = as.EXPECT_GEN_EQUAL
			wp.Generation = gen
		}

		bins := as.BinMap{seenPeersBin: peersToASMap(peers)}
		if shouldFire {
			bins[seenThresholdFired] = 1
		} else if priorFired {
			bins[seenThresholdFired] = 1
		}

		err = s.client.Client().Put(wp, key, bins)
		if err != nil {
			var asErr as.Error
			if errors.As(err, &asErr) &&
				(asErr.Matches(astypes.GENERATION_ERROR) || asErr.Matches(astypes.KEY_EXISTS_ERROR)) {
				continue // retry
			}
			return nil, fmt.Errorf("failed to write seen counter: %w", err)
		}
		return &IncrementResult{NewCount: score, ThresholdReached: shouldFire}, nil
	}
	return nil, fmt.Errorf("seen counter CAS exhausted for %s after %d attempts", txid, seenCASMaxAttempts)
}

// BatchAddPeer applies peer/weight to many txids (subtree-fetcher hot path).
func (s *aerospikeSeenCounter) BatchAddPeer(txids []string, peerID string, weight int) (map[string]*IncrementResult, error) {
	results := make(map[string]*IncrementResult, len(txids))
	if weight <= 0 || peerID == "" || len(txids) == 0 {
		return results, nil
	}

	var mu sync.Mutex
	err := forEachChunkConcurrent(txids, s.client.batchChunkConcurrency,
		func(chunk []string) error {
			local := make(map[string]*IncrementResult, len(chunk))
			chunkErr := s.batchAddPeerChunk(chunk, peerID, weight, local)
			mu.Lock()
			for k, v := range local {
				results[k] = v
			}
			mu.Unlock()
			return chunkErr
		})
	return results, err
}

func (s *aerospikeSeenCounter) batchAddPeerChunk(txids []string, peerID string, weight int, results map[string]*IncrementResult) error {
	mapPolicy := as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, as.MapWriteFlagsCreateOnly|as.MapWriteFlagsNoFail)
	batchRecs := make([]as.BatchRecordIfc, len(txids))
	for i, txid := range txids {
		key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
		if err != nil {
			return fmt.Errorf("failed to create key for %s: %w", txid, err)
		}
		batchRecs[i] = as.NewBatchWrite(
			nil, key,
			as.MapPutOp(mapPolicy, seenPeersBin, peerID, weight),
			as.GetBinOp(seenPeersBin),
			as.GetBinOp(seenThresholdFired),
		)
	}

	bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
	if err := s.client.Client().BatchOperate(bp, batchRecs); err != nil {
		return fmt.Errorf("batch add peer seen counters: %w", err)
	}

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

		score := sumPeerWeights(peersMapFromBin(extractPeersBin(rec.Record.Bins[seenPeersBin])))
		fired := false
		if firedVal, ok := rec.Record.Bins[seenThresholdFired].(int); ok && firedVal == 1 {
			fired = true
		}

		if score >= s.threshold && !fired {
			// Phase 2: generation-CAS fire (F-045).
			res, err := s.AddPeer(txids[i], peerID, weight)
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("firing threshold for %s: %w", txids[i], err)
				}
				continue
			}
			results[txids[i]] = res
			continue
		}

		results[txids[i]] = &IncrementResult{NewCount: score, ThresholdReached: false}
	}
	return firstErr
}

// BatchDelete removes seen counters at mine time (unchanged contract).
func (s *aerospikeSeenCounter) BatchDelete(txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	return forEachChunkConcurrent(txids, s.client.batchChunkConcurrency,
		func(chunk []string) error {
			batchRecs := make([]as.BatchRecordIfc, 0, len(chunk))
			for _, txid := range chunk {
				key, err := as.NewKey(s.client.Namespace(), s.setName, txid)
				if err != nil {
					return fmt.Errorf("failed to create key for %s: %w", txid, err)
				}
				batchRecs = append(batchRecs, as.NewBatchDelete(nil, key))
			}
			bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
			if err := s.client.Client().BatchOperate(bp, batchRecs); err != nil {
				return fmt.Errorf("batch delete seen counters: %w", err)
			}
			var firstErr error
			for i, br := range batchRecs {
				rec := br.BatchRec()
				if rec.Err != nil && !rec.Err.Matches(astypes.KEY_NOT_FOUND_ERROR) && firstErr == nil {
					firstErr = fmt.Errorf("delete seen counter for %s: %w", chunk[i], rec.Err)
				}
			}
			return firstErr
		})
}

func extractPeersBin(v interface{}) interface{} {
	switch t := v.(type) {
	case []interface{}:
		for i := len(t) - 1; i >= 0; i-- {
			switch t[i].(type) {
			case map[interface{}]interface{}, map[string]interface{}:
				return t[i]
			}
		}
		return nil
	default:
		return v
	}
}

func peersMapFromBin(v interface{}) map[string]int {
	out := make(map[string]int)
	switch m := v.(type) {
	case map[interface{}]interface{}:
		for k, val := range m {
			ks, ok := k.(string)
			if !ok {
				continue
			}
			out[ks] = asInt(val)
		}
	case map[string]interface{}:
		for k, val := range m {
			out[k] = asInt(val)
		}
	case map[string]int:
		for k, val := range m {
			out[k] = val
		}
	}
	return out
}

func peersToASMap(peers map[string]int) map[interface{}]interface{} {
	m := make(map[interface{}]interface{}, len(peers))
	for k, v := range peers {
		m[k] = v
	}
	return m
}

func sumPeerWeights(peers map[string]int) int {
	score := 0
	for _, w := range peers {
		score += w
	}
	return score
}

func asInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		if n > int64(^uint(0)>>1) || n < 0 {
			return 0
		}
		return int(n)
	case uint64:
		// Peer weights and block heights are small; reject absurd values.
		if n > uint64(^uint(0)>>1) {
			return 0
		}
		return int(n)
	default:
		return 0
	}
}
