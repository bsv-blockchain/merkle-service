package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/bsv-blockchain/merkle-service/internal/logfields"
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
			s.logger.Warn("failed to encode block-processed data for counter", logfields.BlockHash(blockHash), "error", mErr)
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
// returns the new value. The Operate reads back the counter via GetBinOp (the
// add op itself returns no value), so the common per-subtree path
// (remaining > 0) is a single round trip that ships only the small counter bin
// — NOT the stashed BlockProcessedData, which the previous GetOp (read-all-bins)
// re-transferred on every one of a block's N decrements (O(N) wasted block-data
// bytes per block; tens of MB for large blocks). Only on the final decrement
// (remaining <= 0) do we make one targeted read of the block-data bin and decode
// it. data is nil while remaining > 0, and nil at zero if no data was stamped at
// Init or it could not be read back.
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

	// Decrement then read back ONLY the counter bin: GetBinOp(subtreeCounterBin)
	// returns the post-add value without dragging the block-data bin over the
	// wire the way GetOp (read-all-bins) did on every decrement.
	record, err := s.client.Client().Operate(
		wp, key,
		as.AddOp(as.NewBin(subtreeCounterBin, -1)),
		as.GetBinOp(subtreeCounterBin),
	)
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

	// Only the final decrement needs the stashed block data — read it with one
	// targeted single-bin Get rather than shipping it on every decrement. Block
	// data is immutable once Init stamped it and the record's TTL was just
	// re-stamped above, so this follow-up read is safe; a miss (record gone, or
	// no data stamped) is tolerated and surfaces as data == nil.
	if val <= 0 {
		dataRec, dErr := s.client.Client().Get(s.client.ReadPolicy(), key, blockDataBin)
		switch {
		case dErr != nil:
			var asErr as.Error
			if !errors.As(dErr, &asErr) || !asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
				s.logger.Warn("failed to read block-processed data from counter", logfields.BlockHash(blockHash), "error", dErr)
			}
		case dataRec != nil:
			if raw, ok := dataRec.Bins[blockDataBin].(string); ok && raw != "" {
				var d BlockProcessedData
				if uErr := json.Unmarshal([]byte(raw), &d); uErr != nil {
					s.logger.Warn("failed to decode block-processed data from counter", logfields.BlockHash(blockHash), "error", uErr)
				} else {
					data = &d
				}
			}
		}
	}

	return val, data, nil
}
