package store

import (
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v7"
)

const subtreeCounterBin = "remaining"

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

func NewSubtreeCounterStore(client *AerospikeClient, setName string, ttlSec int, maxRetries int, retryBaseMs int, logger *slog.Logger) SubtreeCounterStore {
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
	wp.Expiration = uint32(s.ttlSec)

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

	record, err := s.client.Client().Operate(wp, key, as.AddOp(as.NewBin(subtreeCounterBin, -1)), as.GetOp())
	if err != nil {
		return 0, fmt.Errorf("failed to decrement subtree counter: %w", err)
	}

	val, ok := record.Bins[subtreeCounterBin].(int)
	if !ok {
		return 0, fmt.Errorf("unexpected type for counter bin: %T", record.Bins[subtreeCounterBin])
	}

	return val, nil
}
