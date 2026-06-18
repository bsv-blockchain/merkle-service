package store

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
)

// expectedIndicesBin holds the CDT list of subtree indices that produced a
// STUMP for a given (block, callbackURL). Stored as an unordered list with
// add-unique semantics so a re-driven subtree work item never double-counts.
const expectedIndicesBin = "indices"

// aerospikeExpectedStump is the Aerospike-backed ExpectedStumpStore. One record
// per (block, callbackURL); the index set lives in expectedIndicesBin.
type aerospikeExpectedStump struct {
	client      *AerospikeClient
	setName     string
	ttlSec      int
	maxRetries  int
	retryBaseMs int
	logger      *slog.Logger
}

var _ ExpectedStumpStore = (*aerospikeExpectedStump)(nil)

// NewExpectedStumpStore constructs an Aerospike-backed ExpectedStumpStore.
func NewExpectedStumpStore(client *AerospikeClient, setName string, ttlSec, maxRetries, retryBaseMs int, logger *slog.Logger) ExpectedStumpStore {
	return &aerospikeExpectedStump{
		client:      client,
		setName:     setName,
		ttlSec:      ttlSec,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
		logger:      logger,
	}
}

// expectedStumpKey is the per-(block, URL) record key. The set of subtrees that
// match a URL is a pure function of (block, URL), so live processing and a
// /reprocess to the same URL share — and agree on — the same record; no
// override-URL scoping (unlike the subtree counter) is needed.
func expectedStumpKey(blockHash, callbackURL string) string {
	return blockHash + "|" + callbackURL
}

// AddSubtreeIndex appends subtreeIndex to each URL's index set in one batched
// round trip. Idempotent: add-unique (with no-fail) means a re-driven subtree
// re-adding its own index is a no-op. Concurrent adds of DIFFERENT indices to
// the same (block, URL) list are safe — Aerospike applies each list append
// atomically server-side, so there is no lost-update race and no CAS is needed.
func (s *aerospikeExpectedStump) AddSubtreeIndex(blockHash string, subtreeIndex int, callbackURLs []string) error {
	if len(callbackURLs) == 0 {
		return nil
	}

	listPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
	wpol := as.NewBatchWritePolicy()
	wpol.RecordExistsAction = as.UPDATE // create the record/bin on first index
	wpol.Expiration = uint32(s.ttlSec)  //nolint:gosec // ttlSec is config-validated and fits uint32

	recs := make([]as.BatchRecordIfc, 0, len(callbackURLs))
	for _, url := range callbackURLs {
		key, err := as.NewKey(s.client.Namespace(), s.setName, expectedStumpKey(blockHash, url))
		if err != nil {
			return fmt.Errorf("expected-stump key for %s: %w", url, err)
		}
		recs = append(recs, as.NewBatchWrite(wpol, key,
			as.ListAppendWithPolicyOp(listPolicy, expectedIndicesBin, subtreeIndex),
		))
	}

	bp := s.client.BatchPolicy(s.maxRetries, s.retryBaseMs)
	if err := s.client.Client().BatchOperate(bp, recs); err != nil {
		return fmt.Errorf("record expected stump indices for block %s: %w", blockHash, err)
	}
	var firstErr error
	for i, r := range recs {
		if e := r.BatchRec().Err; e != nil && firstErr == nil {
			firstErr = fmt.Errorf("record expected stump index for %s: %w", callbackURLs[i], e)
		}
	}
	return firstErr
}

// GetSubtreeIndices returns the ascending set of subtree indices recorded for
// (block, URL). A missing record means no STUMP was produced for that URL —
// returned as an empty slice, not an error.
func (s *aerospikeExpectedStump) GetSubtreeIndices(blockHash, callbackURL string) ([]int, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, expectedStumpKey(blockHash, callbackURL))
	if err != nil {
		return nil, fmt.Errorf("expected-stump key: %w", err)
	}
	rec, err := s.client.Client().Get(s.client.ReadPolicy(), key, expectedIndicesBin)
	if err != nil {
		var asErr as.Error
		if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
			return nil, nil
		}
		return nil, fmt.Errorf("read expected stump indices: %w", err)
	}
	if rec == nil {
		return nil, nil
	}
	raw, ok := rec.Bins[expectedIndicesBin].([]interface{})
	if !ok {
		return nil, nil
	}
	out := make([]int, 0, len(raw))
	for _, v := range raw {
		if n, ok := v.(int); ok {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out, nil
}
