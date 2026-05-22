package store

import (
	"errors"
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

const subtreeCounterBin = "remaining"

// ErrCounterNotFound is returned by Decrement when the per-block subtree
// counter record is absent. The usual cause is TTL expiry: a large block whose
// processing outlives the counter's TTL loses the record mid-flight. A subtree
// worker cannot recreate the counter (Decrement is UPDATE_ONLY), so retrying a
// vanished counter is futile — callers must treat this as terminal for the
// work item and rely on a fresh block reprocess to rebuild the counter.
// Retrying it forever was the cause of the unbounded subtree-work republish
// loop.
var ErrCounterNotFound = errors.New("subtree counter not found")

// aerospikeSubtreeCounter is the Aerospike-backed SubtreeCounterStore
// implementation. Used to coordinate BLOCK_PROCESSED emission: the block
// processor initializes a counter with the subtree count, and each subtree
// worker decrements it. When the counter reaches zero, the last worker emits
// BLOCK_PROCESSED.
type aerospikeSubtreeCounter struct {
	client      *AerospikeClient
	setName     string
	ttlSec      int
	maxRetries  int
	retryBaseMs int
	logger      *slog.Logger
}

var _ SubtreeCounterStore = (*aerospikeSubtreeCounter)(nil)

func NewSubtreeCounterStore(client *AerospikeClient, setName string, ttlSec, maxRetries, retryBaseMs int, logger *slog.Logger) SubtreeCounterStore {
	return &aerospikeSubtreeCounter{
		client:      client,
		setName:     setName,
		ttlSec:      ttlSec,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
		logger:      logger,
	}
}

// Init creates a counter record for the given blockHash with the initial count.
func (s *aerospikeSubtreeCounter) Init(blockHash string, count int) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockHash)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE
	wp.Expiration = uint32(s.ttlSec) //nolint:gosec // ttlSec is config-validated and fits uint32

	err = s.client.Client().Put(wp, key, as.BinMap{subtreeCounterBin: count})
	if err != nil {
		return fmt.Errorf("failed to init subtree counter: %w", err)
	}
	return nil
}

// Decrement atomically decrements the counter for the given blockHash and returns the new value.
func (s *aerospikeSubtreeCounter) Decrement(blockHash string) (remaining int, err error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockHash)
	if err != nil {
		return 0, fmt.Errorf("failed to create key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE_ONLY
	// Re-stamp the TTL on every decrement so the counter expires only after
	// ttlSec of *inactivity*, not ttlSec after Init. Init's TTL would otherwise
	// be a hard deadline on the whole block: a block with tens of thousands of
	// subtrees can take far longer to drain than ttlSec, so the counter would
	// expire mid-flight and every remaining worker would fail with
	// KEY_NOT_FOUND. Re-stamping keeps the counter alive as long as subtrees
	// keep being processed.
	wp.Expiration = uint32(s.ttlSec) //nolint:gosec // ttlSec is config-validated and fits uint32

	record, err := s.client.Client().Operate(wp, key, as.AddOp(as.NewBin(subtreeCounterBin, -1)), as.GetOp())
	if err != nil {
		var asErr as.Error
		if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
			return 0, ErrCounterNotFound
		}
		return 0, fmt.Errorf("failed to decrement subtree counter: %w", err)
	}

	val, ok := record.Bins[subtreeCounterBin].(int)
	if !ok {
		return 0, fmt.Errorf("unexpected type for counter bin: %T", record.Bins[subtreeCounterBin])
	}

	return val, nil
}
