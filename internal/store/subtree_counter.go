package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

const (
	subtreeCounterBin = "remaining"
	// blockDataBin holds the JSON-encoded BlockProcessedData stamped at Init
	// and read back when the counter drains to zero so BLOCK_PROCESSED can
	// carry the merkle root / subtree list / coinbase BUMP. Stored as a single
	// string bin (an Aerospike CE core feature — no Enterprise dependency).
	blockDataBin = "blockdata"
)

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
// When data is non-nil it is JSON-encoded and stamped onto the record so the
// final Decrement can surface it on BLOCK_PROCESSED without a second fetch.
func (s *aerospikeSubtreeCounter) Init(blockHash string, count int, data *BlockProcessedData) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockHash)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE
	wp.Expiration = uint32(s.ttlSec) //nolint:gosec // ttlSec is config-validated and fits uint32

	bins := as.BinMap{subtreeCounterBin: count}
	if data != nil {
		encoded, mErr := json.Marshal(data)
		if mErr != nil {
			// Non-fatal: a counter without block data still drives
			// BLOCK_PROCESSED; the consumer just falls back to a datahub.
			s.logger.Warn("failed to encode block-processed data for counter", "blockHash", blockHash, "error", mErr)
		} else {
			bins[blockDataBin] = string(encoded)
		}
	}

	err = s.client.Client().Put(wp, key, bins)
	if err != nil {
		return fmt.Errorf("failed to init subtree counter: %w", err)
	}
	return nil
}

// Decrement atomically decrements the counter for the given blockHash and
// returns the new value. The Operate call already fetches every bin via GetOp,
// so when the counter has drained (remaining <= 0) the stashed
// BlockProcessedData is decoded from the same record and returned — no extra
// round trip. data is nil while remaining > 0 (the common per-subtree path) to
// avoid needless JSON work, and nil at zero if no data was stamped at Init.
func (s *aerospikeSubtreeCounter) Decrement(blockHash string) (remaining int, data *BlockProcessedData, err error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockHash)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create key: %w", err)
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
			return 0, nil, ErrCounterNotFound
		}
		return 0, nil, fmt.Errorf("failed to decrement subtree counter: %w", err)
	}

	val, ok := record.Bins[subtreeCounterBin].(int)
	if !ok {
		return 0, nil, fmt.Errorf("unexpected type for counter bin: %T", record.Bins[subtreeCounterBin])
	}

	// Only decode the stashed block data on the final decrement — the per-subtree
	// hot path (remaining > 0) skips the JSON work entirely.
	if val <= 0 {
		if raw, ok := record.Bins[blockDataBin].(string); ok && raw != "" {
			var d BlockProcessedData
			if uErr := json.Unmarshal([]byte(raw), &d); uErr != nil {
				s.logger.Warn("failed to decode block-processed data from counter", "blockHash", blockHash, "error", uErr)
			} else {
				data = &d
			}
		}
	}

	return val, data, nil
}
